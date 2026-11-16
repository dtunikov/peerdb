package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connsqlserver "github.com/PeerDB-io/peerdb/flow/connectors/sqlserver"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type SqlServerSource struct {
	conn   *connsqlserver.SqlServerConnector
	config *protos.SqlServerConfig
	// separate connection for seeding test data
	db *sql.DB
}

func (s *SqlServerSource) GeneratePeer(t *testing.T) *protos.Peer {
	t.Helper()
	peer := &protos.Peer{
		Name:   "sqlserver",
		Type:   protos.DBType_SQLSERVER,
		Config: &protos.Peer_SqlserverConfig{SqlserverConfig: s.config},
	}
	CreatePeer(t, peer)
	return peer
}

func (s *SqlServerSource) dropSchema(ctx context.Context, schema string) error {
	// dropping a tracked table also drops its capture instance
	if _, err := s.db.ExecContext(ctx, `
		DECLARE @sql NVARCHAR(MAX) = N'';
		SELECT @sql += N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';'
		FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = @p1;
		EXEC sp_executesql @sql;
		IF SCHEMA_ID(@p1) IS NOT NULL EXEC (N'DROP SCHEMA ' + QUOTENAME(@p1));`, schema); err != nil {
		return fmt.Errorf("failed to drop schema %s: %w", schema, err)
	}
	return nil
}

func (s *SqlServerSource) Teardown(t *testing.T, ctx context.Context, suffix string) {
	t.Helper()
	if err := s.dropSchema(ctx, "e2e_test_"+suffix); err != nil {
		t.Log(err)
	}
	_ = s.db.Close()
	_ = s.conn.Close()
}

func (s *SqlServerSource) Connector() connectors.Connector {
	return s.conn
}

func (s *SqlServerSource) SqlServerConnector() *connsqlserver.SqlServerConnector {
	return s.conn
}

func (s *SqlServerSource) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := s.db.ExecContext(ctx, sql, args...)
	return err
}

// EnableCDC tracks a table, PeerDB itself never enables CDC
func (s *SqlServerSource) EnableCDC(ctx context.Context, schema string, table string) error {
	return s.Exec(ctx,
		"EXEC sys.sp_cdc_enable_table @source_schema = @p1, @source_name = @p2, @role_name = NULL", schema, table)
}

func (s *SqlServerSource) GetRows(ctx context.Context, suffix, table, cols string) (*model.QRecordBatch, error) {
	tableName := fmt.Sprintf("e2e_test_%s.%s", suffix, table)
	config := &protos.QRepConfig{
		FlowJobName:    "testflow",
		WatermarkTable: tableName,
		Query:          fmt.Sprintf("SELECT %s FROM %s ORDER BY id", cols, tableName),
		Version:        shared.InternalVersion_Latest,
	}
	partition := &protos.QRepPartition{
		PartitionId:        "testpart",
		FullTablePartition: true,
	}

	stream := model.NewQRecordStream(1024)
	go func() {
		_, _, err := s.conn.PullQRepRecords(ctx, shared.CatalogPool{}, nil, config, protos.DBType_CLICKHOUSE, partition, stream)
		stream.Close(err)
	}()

	schema, err := stream.Schema()
	if err != nil {
		return nil, err
	}
	batch := &model.QRecordBatch{Schema: schema}
	for record := range stream.Records {
		batch.Records = append(batch.Records, record)
	}
	if err := stream.Err(); err != nil {
		return nil, err
	}
	return batch, nil
}

func SetupSqlServer(t *testing.T, suffix string) (*SqlServerSource, error) {
	t.Helper()
	config := internal.GetSqlServerConfigFromEnv()
	connector, err := connsqlserver.NewSqlServerConnector(t.Context(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup sqlserver connector: %w", err)
	}
	mssqlConnector, err := mssql.NewConnector(connsqlserver.ConnectionString(config))
	if err != nil {
		connector.Close()
		return nil, err
	}
	source := &SqlServerSource{conn: connector, config: config, db: sql.OpenDB(mssqlConnector)}

	schema := "e2e_test_" + suffix
	if err := source.dropSchema(t.Context(), schema); err != nil {
		source.Teardown(t, t.Context(), suffix)
		return nil, err
	}
	if err := source.Exec(t.Context(), "EXEC (N'CREATE SCHEMA ' + QUOTENAME(@p1))", schema); err != nil {
		source.Teardown(t, t.Context(), suffix)
		return nil, fmt.Errorf("failed to create schema %s: %w", schema, err)
	}
	return source, nil
}
