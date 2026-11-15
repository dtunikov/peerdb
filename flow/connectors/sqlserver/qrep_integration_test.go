//go:build tilt

package connsqlserver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func pullPartition(
	t *testing.T, c *SqlServerConnector, config *protos.QRepConfig, partition *protos.QRepPartition,
) ([]types.QField, [][]types.QValue) {
	t.Helper()
	stream := model.NewQRecordStream(1024)
	errCh := make(chan error, 1)
	go func() {
		_, _, err := c.PullQRepRecords(t.Context(), shared.CatalogPool{}, nil, config, protos.DBType_CLICKHOUSE, partition, stream)
		stream.Close(err)
		errCh <- err
	}()
	var records [][]types.QValue
	for record := range stream.Records {
		records = append(records, record)
	}
	require.NoError(t, <-errCh)
	schema, err := stream.Schema()
	require.NoError(t, err)
	return schema.Fields, records
}

func TestQRepPartitionedPull(t *testing.T) {
	c := newTestConnector(t)
	ctx := t.Context()
	schema := createTestSchema(t, c)
	table := quoteIdentifier(schema) + ".items"

	_, err := c.db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id BIGINT PRIMARY KEY, name NVARCHAR(50), price DECIMAL(10,2), x XML)", table))
	require.NoError(t, err)
	_, err = c.db.ExecContext(ctx, fmt.Sprintf(`
		WITH n AS (SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_columns a, sys.all_columns b)
		INSERT INTO %s (id, name, price) SELECT i * 3, CONCAT(N'item ', i), i * 1.25 FROM n`, table))
	require.NoError(t, err)

	tableID := schema + ".items"
	tableSchemas, err := c.GetTableSchema(ctx, nil, 0, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableID, Exclude: []string{"x"}}})
	require.NoError(t, err)
	keys, err := c.GetDefaultPartitionKeyForTables(ctx, &protos.GetDefaultPartitionKeyForTablesInput{
		TableMappings:      []*protos.TableMapping{{SourceTableIdentifier: tableID}},
		TableSchemaMapping: tableSchemas,
	})
	require.NoError(t, err)
	require.Equal(t, "id", keys.TableDefaultPartitionKeyMapping[tableID])

	config := &protos.QRepConfig{
		FlowJobName:         "test_qrep",
		WatermarkTable:      tableID,
		WatermarkColumn:     "id",
		NumRowsPerPartition: 100,
		Exclude:             []string{"x"},
	}
	partitions, err := c.GetQRepPartitions(ctx, config, nil)
	require.NoError(t, err)
	require.Greater(t, len(partitions), 5)

	seen := make(map[int64]bool)
	for _, partition := range partitions {
		fields, records := pullPartition(t, c, config, partition)
		require.Len(t, fields, 3)
		require.Equal(t, types.QValueKindNumeric, fields[2].Type)
		require.Equal(t, int16(10), fields[2].Precision)
		for _, record := range records {
			id := record[0].(types.QValueInt64).Val
			require.False(t, seen[id], "duplicate id %d", id)
			seen[id] = true
			require.Equal(t, fmt.Sprintf("item %d", id/3), record[1].(types.QValueString).Val)
		}
	}
	require.Len(t, seen, 1000)

	// resume after the first partition
	resumed, err := c.GetQRepPartitions(ctx, config, partitions[0])
	require.NoError(t, err)
	require.NotEmpty(t, resumed)
	require.Greater(t, resumed[0].Range.GetIntRange().Start, partitions[0].Range.GetIntRange().End)

	// custom query over a full table partition
	fields, records := pullPartition(t, c, &protos.QRepConfig{
		WatermarkTable: tableID,
		Query:          fmt.Sprintf("SELECT id, price FROM %s WHERE id <= 30 ORDER BY id", table),
	}, &protos.QRepPartition{PartitionId: "full", FullTablePartition: true})
	require.Len(t, fields, 2)
	require.Len(t, records, 10)
}

func TestQRepNonIntegerWatermark(t *testing.T) {
	c := newTestConnector(t)
	ctx := t.Context()
	schema := createTestSchema(t, c)
	_, err := c.db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s.t (id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(), v INT)", quoteIdentifier(schema)))
	require.NoError(t, err)
	_, err = c.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s.t (v) VALUES (1), (2)", quoteIdentifier(schema)))
	require.NoError(t, err)

	config := &protos.QRepConfig{
		WatermarkTable:      schema + ".t",
		WatermarkColumn:     "id",
		NumRowsPerPartition: 1,
	}
	partitions, err := c.GetQRepPartitions(ctx, config, nil)
	require.NoError(t, err)
	require.Len(t, partitions, 1)
	require.True(t, partitions[0].FullTablePartition)

	_, records := pullPartition(t, c, config, partitions[0])
	require.Len(t, records, 2)
	require.IsType(t, types.QValueUUID{}, records[0][0])
}

func TestQRepEmptyTable(t *testing.T) {
	c := newTestConnector(t)
	ctx := t.Context()
	schema := createTestSchema(t, c)
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s.t (id INT PRIMARY KEY)", quoteIdentifier(schema)))
	require.NoError(t, err)

	partitions, err := c.GetQRepPartitions(ctx, &protos.QRepConfig{
		WatermarkTable:      schema + ".t",
		WatermarkColumn:     "id",
		NumRowsPerPartition: 100,
	}, nil)
	require.NoError(t, err)
	require.Len(t, partitions, 1)
	require.True(t, partitions[0].FullTablePartition)
}
