//go:build tilt

package sqlserver_clickhouse

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2e"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

type SqlServerClickHouseSuite struct {
	e2e.GenericSuite
}

func TestSqlServerClickHouseSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupSqlServerClickHouseSuite)
}

func SetupSqlServerClickHouseSuite(t *testing.T) SqlServerClickHouseSuite {
	t.Helper()
	return SqlServerClickHouseSuite{e2e.SetupClickHouseSuite(t, false, func(t *testing.T) (*e2e.SqlServerSource, string, error) {
		t.Helper()
		suffix := "mssqlch_" + strings.ToLower(common.RandomString(8))
		source, err := e2e.SetupSqlServer(t, suffix)
		return source, suffix, err
	})(t)}
}

func (s SqlServerClickHouseSuite) source() *e2e.SqlServerSource {
	return s.Source().(*e2e.SqlServerSource)
}

// createTrackedTable creates a table and enables CDC on it
func (s SqlServerClickHouseSuite) createTrackedTable(table string, columns string) string {
	t := s.T()
	qualified := e2e.AttachSchema(s, table)
	require.NoError(t, s.Source().Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s (%s)", qualified, columns)))
	require.NoError(t, s.source().EnableCDC(t.Context(), e2e.Schema(s), table))
	return qualified
}

func (s SqlServerClickHouseSuite) mirrorConfig(name string, tables ...string) *protos.FlowConnectionConfigs {
	mappings := make([]string, 0, len(tables)*2)
	for _, table := range tables {
		mappings = append(mappings, table, table+"_dst")
	}
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:   e2e.AddSuffix(s, name),
		TableMappings: e2e.TableMappings(s, mappings...),
		Destination:   s.Peer().Name,
	}
	flowConnConfig := connectionGen.GenerateFlowConnectionConfigs(s)
	flowConnConfig.DoInitialSnapshot = true
	return flowConnConfig
}

func (s SqlServerClickHouseSuite) run(flowConnConfig *protos.FlowConnectionConfigs) e2e.WorkflowRun {
	t := s.T()
	tc := e2e.NewTemporalClient(t)
	env := e2e.ExecutePeerflow(t, tc, flowConnConfig)
	e2e.SetupCDCFlowStatusQuery(t, env, flowConnConfig)
	return env
}

func (s SqlServerClickHouseSuite) Test_Peer_Validate() {
	t := s.T()
	require.NoError(t, s.source().SqlServerConnector().ValidateCheck(t.Context()))
}

func (s SqlServerClickHouseSuite) Test_Initial_Snapshot_Only() {
	t := s.T()
	ctx := t.Context()
	table := "snapshot_only"
	qualified := e2e.AttachSchema(s, table)
	// snapshot-only mirrors do not need CDC on the table
	require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, val NVARCHAR(50), amount DECIMAL(10,2), u UNIQUEIDENTIFIER DEFAULT NEWID())", qualified)))
	require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf(`
		WITH n AS (SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_columns)
		INSERT INTO %s (id, val, amount) SELECT i, CONCAT(N'val ', i), i * 0.5 FROM n`, qualified)))

	flowConnConfig := s.mirrorConfig(table, table)
	flowConnConfig.InitialSnapshotOnly = true
	flowConnConfig.SnapshotNumRowsPerPartition = 100
	env := s.run(flowConnConfig)

	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial snapshot", table, table+"_dst", "id,val,amount,u")
	e2e.EnvWaitForFinished(t, env, 3*time.Minute)
}

func (s SqlServerClickHouseSuite) Test_CDC_Simple() {
	t := s.T()
	ctx := t.Context()
	table := "cdc_simple"
	qualified := s.createTrackedTable(table, "id INT PRIMARY KEY, ky NVARCHAR(50) NOT NULL, val NVARCHAR(MAX)")
	for i := range 5 {
		require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s VALUES (%d, N'init_key_%d', N'init_value_%d')", qualified, i+1, i, i)))
	}

	flowConnConfig := s.mirrorConfig(table, table)
	env := s.run(flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial snapshot", table, table+"_dst", "id,ky,val")

	for i := range 5 {
		e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s VALUES (%d, N'cdc_key_%d', N'cdc_value_%d')", qualified, i+6, i, i)))
	}
	e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf("UPDATE %s SET val = N'updated' WHERE id = 2", qualified)))
	e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf("UPDATE %s SET ky = N'only ky' WHERE id = 7", qualified)))
	e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 3", qualified)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "cdc inserts, updates and a delete", table, table+"_dst", "id,ky,val")

	env.Cancel(ctx)
	e2e.RequireEnvCanceled(t, env)
}

func (s SqlServerClickHouseSuite) Test_CDC_Primary_Key_Update() {
	t := s.T()
	ctx := t.Context()
	table := "cdc_pk_update"
	qualified := s.createTrackedTable(table, "id INT PRIMARY KEY, val NVARCHAR(50)")
	require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, N'one'), (2, N'two')", qualified)))

	flowConnConfig := s.mirrorConfig(table, table)
	env := s.run(flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial snapshot", table, table+"_dst", "id,val")

	e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf("UPDATE %s SET id = 10 WHERE id = 1", qualified)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "primary key update", table, table+"_dst", "id,val")

	env.Cancel(ctx)
	e2e.RequireEnvCanceled(t, env)
}

func (s SqlServerClickHouseSuite) Test_CDC_Multiple_Tables_One_Transaction() {
	t := s.T()
	ctx := t.Context()
	users := s.createTrackedTable("users", "id INT PRIMARY KEY, balance DECIMAL(10,2) NOT NULL")
	orders := s.createTrackedTable("orders", "id INT PRIMARY KEY, user_id INT NOT NULL, amount DECIMAL(10,2) NOT NULL")
	require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", users)))

	flowConnConfig := s.mirrorConfig("multi_table_tx", "users", "orders")
	env := s.run(flowConnConfig)
	e2e.EnvWaitForEqualTablesWithNames(env, s, "initial snapshot", "users", "users_dst", "id,balance")

	for i := range 5 {
		e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf(`BEGIN TRANSACTION;
			INSERT INTO %s VALUES (%d, 1, 10);
			UPDATE %s SET balance = balance - 10 WHERE id = 1;
			COMMIT`, orders, i+1, users)))
	}
	e2e.EnvWaitForEqualTablesWithNames(env, s, "orders", "orders", "orders_dst", "id,user_id,amount")
	e2e.EnvWaitForEqualTablesWithNames(env, s, "users", "users", "users_dst", "id,balance")

	env.Cancel(ctx)
	e2e.RequireEnvCanceled(t, env)
}

func (s SqlServerClickHouseSuite) Test_CDC_Pause_Resume() {
	t := s.T()
	ctx := t.Context()
	table := "cdc_pause_resume"
	qualified := s.createTrackedTable(table, "id INT PRIMARY KEY, val NVARCHAR(50)")

	flowConnConfig := s.mirrorConfig(table, table)
	env := s.run(flowConnConfig)
	e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, N'before pause')", qualified)))
	e2e.EnvWaitForEqualTablesWithNames(env, s, "first insert", table, table+"_dst", "id,val")

	e2e.SignalWorkflow(ctx, env, model.FlowSignal, model.PauseSignal)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "pausing", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_PAUSED
	})
	e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (2, N'while paused')", qualified)))
	e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf("UPDATE %s SET val = N'updated while paused' WHERE id = 1", qualified)))

	e2e.SignalWorkflow(ctx, env, model.FlowSignal, model.NoopSignal)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "resuming", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING
	})
	e2e.EnvWaitForEqualTablesWithNames(env, s, "changes made while paused", table, table+"_dst", "id,val")

	env.Cancel(ctx)
	e2e.RequireEnvCanceled(t, env)
}

func (s SqlServerClickHouseSuite) Test_CDC_Backlog_Over_Multiple_Batches() {
	t := s.T()
	ctx := t.Context()
	table := "cdc_backlog"
	qualified := s.createTrackedTable(table, "id INT IDENTITY PRIMARY KEY, tx INT NOT NULL")

	flowConnConfig := s.mirrorConfig(table, table)
	flowConnConfig.DoInitialSnapshot = false
	flowConnConfig.MaxBatchSize = 20
	env := s.run(flowConnConfig)
	e2e.EnvWaitFor(t, env, 3*time.Minute, "running", func() bool {
		return env.GetFlowStatus(t) == protos.FlowStatus_STATUS_RUNNING
	})

	for tx := range 30 {
		e2e.EnvNoError(t, env, s.Source().Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s (tx) VALUES (%[2]d), (%[2]d), (%[2]d), (%[2]d), (%[2]d)", qualified, tx)))
	}
	e2e.EnvWaitForEqualTablesWithNames(env, s, "backlog drained", table, table+"_dst", "id,tx")

	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err)
	var batches int64
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM peerdb_stats.cdc_batches WHERE flow_name = $1 AND rows_in_batch > 0",
		flowConnConfig.FlowJobName).Scan(&batches))
	require.Greater(t, batches, int64(1))

	env.Cancel(ctx)
	e2e.RequireEnvCanceled(t, env)
}

func (s SqlServerClickHouseSuite) Test_Validate_Untracked_Table() {
	t := s.T()
	ctx := t.Context()
	table := "untracked"
	require.NoError(t, s.Source().Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", e2e.AttachSchema(s, table))))

	flowConnConfig := s.mirrorConfig(table, table)
	err := s.source().SqlServerConnector().ValidateMirrorSource(ctx, &protos.FlowConnectionConfigsCore{
		TableMappings: flowConnConfig.TableMappings,
	})
	require.ErrorContains(t, err, "is not tracked by CDC")
}
