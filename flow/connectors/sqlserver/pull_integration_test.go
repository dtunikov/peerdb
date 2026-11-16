//go:build tilt

package connsqlserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type cdcFixture struct {
	c        *SqlServerConnector
	schema   string
	flowName string
	tables   []string
}

func setupCDCFixture(t *testing.T, ddl map[string]string) *cdcFixture {
	t.Helper()
	c := newTestConnector(t)
	f := &cdcFixture{
		c:        c,
		schema:   createTestSchema(t, c),
		flowName: "sqlserver_pull_" + strings.ToLower(common.RandomString(8)),
	}
	tableMapping := make(map[string]string)
	for table, columns := range ddl {
		_, err := c.db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE %s.%s (%s)", quoteIdentifier(f.schema), table, columns))
		require.NoError(t, err)
		enableTableCDC(t, c, f.schema, table)
		f.tables = append(f.tables, table)
		tableMapping[f.schema+"."+table] = table
	}
	_, err := c.SetupReplication(t.Context(), shared.CatalogPool{}, &protos.SetupReplicationInput{
		FlowJobName:      f.flowName,
		TableNameMapping: tableMapping,
	})
	require.NoError(t, err)
	return f
}

func (f *cdcFixture) exec(t *testing.T, query string) {
	t.Helper()
	_, err := f.c.db.ExecContext(t.Context(), strings.ReplaceAll(query, "{schema}", quoteIdentifier(f.schema)))
	require.NoError(t, err)
}

type pullResult struct {
	records    []model.Record[model.RecordItems]
	checkpoint model.CdcCheckpoint
}

func (f *cdcFixture) pull(t *testing.T, ctx context.Context, lastOffset model.CdcCheckpoint, maxBatchSize uint32) (pullResult, error) {
	t.Helper()
	tableNameMapping := make(map[string]model.SourceTableMapping)
	var mappings []*protos.TableMapping
	for _, table := range f.tables {
		tableNameMapping[f.schema+"."+table] = model.SourceTableMapping{Name: table}
		mappings = append(mappings, &protos.TableMapping{SourceTableIdentifier: f.schema + "." + table, DestinationTableIdentifier: table})
	}
	sourceSchemas, err := f.c.GetTableSchema(ctx, nil, 0, protos.TypeSystem_Q, mappings)
	require.NoError(t, err)
	schemas := make(map[string]*protos.TableSchema)
	for _, tm := range mappings {
		schemas[tm.DestinationTableIdentifier] = sourceSchemas[tm.SourceTableIdentifier]
	}

	stream := model.NewCDCStream[model.RecordItems](1 << 16)
	errCh := make(chan error, 1)
	go func() {
		errCh <- f.c.PullRecords(ctx, shared.CatalogPool{}, nil, &model.PullRecordsRequest[model.RecordItems]{
			RecordStream:           stream,
			FlowJobName:            f.flowName,
			TableNameMapping:       tableNameMapping,
			TableNameSchemaMapping: schemas,
			LastOffset:             lastOffset,
			MaxBatchSize:           maxBatchSize,
			IdleTimeout:            2 * time.Second,
		})
	}()
	var res pullResult
	for record := range stream.GetRecords() {
		res.records = append(res.records, record)
	}
	err = <-errCh
	res.checkpoint = stream.GetLastCheckpoint()
	return res, err
}

func (f *cdcFixture) lastOffset(t *testing.T) model.CdcCheckpoint {
	t.Helper()
	offset, err := f.c.GetLastOffset(t.Context(), f.flowName)
	require.NoError(t, err)
	return offset
}

func itemValue(t *testing.T, items model.RecordItems, col string) any {
	t.Helper()
	qv := items.GetColumnValue(col)
	require.NotNil(t, qv, col)
	return qv.Value()
}

func TestPullRecordsOperations(t *testing.T) {
	f := setupCDCFixture(t, map[string]string{
		"users":  "id INT PRIMARY KEY, name NVARCHAR(50), balance DECIMAL(10,2), bio NVARCHAR(MAX)",
		"orders": "id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2)",
	})
	f.exec(t, "INSERT INTO {schema}.users VALUES (1, N'alice', 100, N'long bio'), (2, N'bob', 50, NULL)")
	f.exec(t, "UPDATE {schema}.users SET name = N'alicia' WHERE id = 1")
	f.exec(t, "DELETE FROM {schema}.users WHERE id = 2")
	f.exec(t, "UPDATE {schema}.users SET id = 10 WHERE id = 1")
	f.exec(t, `BEGIN TRANSACTION;
		INSERT INTO {schema}.orders VALUES (1, 10, 25);
		UPDATE {schema}.users SET balance = balance - 25 WHERE id = 10;
		COMMIT`)

	res, err := f.pull(t, t.Context(), f.lastOffset(t), 1000)
	require.NoError(t, err)
	require.NotEmpty(t, res.checkpoint.Text)

	var users, orders []model.Record[model.RecordItems]
	for _, r := range res.records {
		switch r.GetDestinationTableName() {
		case "users":
			users = append(users, r)
		case "orders":
			orders = append(orders, r)
		}
		require.Positive(t, r.GetCommitTime().UnixNano())
	}
	require.Len(t, orders, 1)
	require.IsType(t, &model.InsertRecord[model.RecordItems]{}, orders[0])

	kinds := make([]string, 0, len(users))
	for _, r := range users {
		kinds = append(kinds, r.Kind())
	}
	// the primary key update arrives as a delete and an insert
	require.Equal(t, []string{"insert", "insert", "update", "delete", "delete", "insert", "update"}, kinds)

	update := users[2].(*model.UpdateRecord[model.RecordItems])
	require.Equal(t, "alicia", itemValue(t, update.NewItems, "name"))
	require.Equal(t, "alice", itemValue(t, update.OldItems, "name"))
	require.Equal(t, "long bio", itemValue(t, update.NewItems, "bio"))

	deleted := users[3].(*model.DeleteRecord[model.RecordItems])
	require.Equal(t, int32(2), itemValue(t, deleted.Items, "id"))
	require.Equal(t, "bob", itemValue(t, deleted.Items, "name"))

	last := users[6].(*model.UpdateRecord[model.RecordItems])
	require.Equal(t, int32(10), itemValue(t, last.NewItems, "id"))
	require.Equal(t, "75", itemValue(t, last.NewItems, "balance").(fmt.Stringer).String())

	// resuming from the batch checkpoint yields nothing new
	require.NoError(t, f.c.SetLastOffset(t.Context(), f.flowName, res.checkpoint))
	f.exec(t, "INSERT INTO {schema}.orders VALUES (2, 10, 1)")
	res, err = f.pull(t, t.Context(), f.lastOffset(t), 1000)
	require.NoError(t, err)
	require.Len(t, res.records, 1)
	require.Equal(t, int32(2), itemValue(t, res.records[0].(*model.InsertRecord[model.RecordItems]).Items, "id"))
}

func TestPullRecordsBacklogKeepsTransactionsWhole(t *testing.T) {
	f := setupCDCFixture(t, map[string]string{
		"events": "id INT IDENTITY PRIMARY KEY, tx INT NOT NULL",
	})
	const transactions, rowsPerTx = 60, 7
	for tx := range transactions {
		f.exec(t, fmt.Sprintf(`WITH n AS (SELECT TOP (%d) 1 AS x FROM sys.all_columns)
			INSERT INTO {schema}.events (tx) SELECT %d FROM n`, rowsPerTx, tx))
	}

	offset := f.lastOffset(t)
	seenTx := make(map[int32]int)
	batches := 0
	for len(seenTx) < transactions || seenTx[transactions-1] < rowsPerTx {
		res, err := f.pull(t, t.Context(), offset, 50)
		require.NoError(t, err)
		batches++
		require.Less(t, batches, 50)

		batchTx := make(map[int32]int)
		for _, r := range res.records {
			tx := itemValue(t, r.(*model.InsertRecord[model.RecordItems]).Items, "tx").(int32)
			batchTx[tx]++
		}
		for tx, count := range batchTx {
			require.Equal(t, rowsPerTx, count, "transaction %d split across batches", tx)
			seenTx[tx] += count
		}
		offset = res.checkpoint
	}
	require.Greater(t, batches, 1)
	require.Len(t, seenTx, transactions)
}

func TestPullRecordsMergesTablesInCommitOrder(t *testing.T) {
	f := setupCDCFixture(t, map[string]string{
		"a": "id INT IDENTITY PRIMARY KEY, tx INT NOT NULL",
		"b": "id INT IDENTITY PRIMARY KEY, tx INT NOT NULL",
	})
	const transactions, rowsPerTx = 30, 3
	var expected []string
	for tx := range transactions {
		f.exec(t, fmt.Sprintf(`BEGIN TRANSACTION;
			INSERT INTO {schema}.a (tx) VALUES (%[1]d);
			INSERT INTO {schema}.b (tx) VALUES (%[1]d);
			INSERT INTO {schema}.a (tx) VALUES (%[1]d);
			COMMIT`, tx))
		expected = append(expected, fmt.Sprintf("%d:a", tx), fmt.Sprintf("%d:b", tx), fmt.Sprintf("%d:a", tx))
	}

	offset := f.lastOffset(t)
	var got []string
	batches := 0
	for len(got) < len(expected) {
		res, err := f.pull(t, t.Context(), offset, 10)
		require.NoError(t, err)
		batches++
		require.Less(t, batches, 50)
		require.Zero(t, len(res.records)%rowsPerTx, "transaction split across batches")
		for _, r := range res.records {
			tx := itemValue(t, r.(*model.InsertRecord[model.RecordItems]).Items, "tx").(int32)
			got = append(got, fmt.Sprintf("%d:%s", tx, r.GetDestinationTableName()))
		}
		offset = res.checkpoint
	}
	require.Greater(t, batches, 1)
	require.Equal(t, expected, got)
}

func TestPullRecordsRetentionExceeded(t *testing.T) {
	f := setupCDCFixture(t, map[string]string{"t": "id INT PRIMARY KEY"})
	offset := f.lastOffset(t)
	f.exec(t, "INSERT INTO {schema}.t VALUES (1)")

	ctx := t.Context()
	table, err := common.ParseTableIdentifier(f.schema + ".t")
	require.NoError(t, err)
	ci, err := f.c.getCaptureInstance(ctx, table)
	require.NoError(t, err)

	// wait for the insert to be captured, then clean everything up to it
	var maxLSN lsn
	require.Eventually(t, func() bool {
		var count int
		require.NoError(t, f.c.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM cdc."+quoteIdentifier(ci.name+"_CT")).Scan(&count))
		maxLSN, err = f.c.getMaxLSN(ctx)
		require.NoError(t, err)
		return count > 0
	}, time.Minute, time.Second)
	_, err = f.c.db.ExecContext(ctx, "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = @p1, @low_water_mark = @p2, @threshold = 5000",
		ci.name, maxLSN[:])
	require.NoError(t, err)

	_, err = f.pull(t, ctx, offset, 1000)
	_, ok := errors.AsType[*exceptions.SqlServerCDCDataCleanedUpError](err)
	require.True(t, ok, "%v", err)
}

func TestPullRecordsIdleAdvancesCatalogCheckpoint(t *testing.T) {
	f := setupCDCFixture(t, map[string]string{"mirrored": "id INT PRIMARY KEY"})
	other := "other_" + strings.ToLower(common.RandomString(6))
	f.exec(t, fmt.Sprintf("CREATE TABLE {schema}.%s (id INT PRIMARY KEY)", other))
	enableTableCDC(t, f.c, f.schema, other)

	start := f.lastOffset(t)
	f.exec(t, fmt.Sprintf("INSERT INTO {schema}.%s VALUES (1)", other))

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	res, err := f.pull(t, ctx, start, 1000)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, res.records)

	advanced, err := parseLSN(f.lastOffset(t).Text)
	require.NoError(t, err)
	startLSN, err := parseLSN(start.Text)
	require.NoError(t, err)
	require.Positive(t, advanced.compare(startLSN))
}

func TestPullRecordsTypes(t *testing.T) {
	f := setupCDCFixture(t, map[string]string{
		"types": "id INT PRIMARY KEY, u UNIQUEIDENTIFIER, ts DATETIME2, tiny TINYINT",
	})
	f.exec(t, "INSERT INTO {schema}.types VALUES (1, '6F9619FF-8B86-D011-B42D-00C04FC964FF', '2024-02-29 13:14:15.1234567', 200)")

	res, err := f.pull(t, t.Context(), f.lastOffset(t), 1000)
	require.NoError(t, err)
	require.Len(t, res.records, 1)
	items := res.records[0].(*model.InsertRecord[model.RecordItems]).Items
	require.Equal(t, types.QValueUUID{}.Kind(), items.GetColumnValue("u").Kind())
	require.Equal(t, "6f9619ff-8b86-d011-b42d-00c04fc964ff", fmt.Sprint(itemValue(t, items, "u")))
	require.Equal(t, uint8(200), itemValue(t, items, "tiny"))
	require.Equal(t, time.Date(2024, 2, 29, 13, 14, 15, 123456700, time.UTC), itemValue(t, items, "ts"))
}
