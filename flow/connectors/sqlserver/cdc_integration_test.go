//go:build tilt

package connsqlserver

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func enableTableCDC(t *testing.T, c *SqlServerConnector, schema string, table string, columns ...string) {
	t.Helper()
	var columnList any
	if len(columns) > 0 {
		columnList = strings.Join(columns, ",")
	}
	_, err := c.db.ExecContext(t.Context(), `EXEC sys.sp_cdc_enable_table
		@source_schema = @p1, @source_name = @p2, @role_name = NULL, @captured_column_list = @p3`,
		schema, table, columnList)
	require.NoError(t, err)
}

func TestLSNNextMatchesServer(t *testing.T) {
	c := newTestConnector(t)
	l, err := parseLSN("0000002a0000ffffffff")
	require.NoError(t, err)
	var b []byte
	require.NoError(t, c.db.QueryRowContext(t.Context(), "SELECT sys.fn_cdc_increment_lsn(@p1)", l[:]).Scan(&b))
	require.Equal(t, hex.EncodeToString(b), l.next().String())
}

func TestValidateMirrorSource(t *testing.T) {
	c := newTestConnector(t)
	ctx := t.Context()
	schema := createTestSchema(t, c)
	for _, table := range []string{"tracked", "partial", "untracked"} {
		_, err := c.db.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s.%s (id INT PRIMARY KEY, a NVARCHAR(10), b INT)", quoteIdentifier(schema), table))
		require.NoError(t, err)
	}
	enableTableCDC(t, c, schema, "tracked")
	enableTableCDC(t, c, schema, "partial", "id", "a")

	config := func(table string, exclude ...string) *protos.FlowConnectionConfigsCore {
		return &protos.FlowConnectionConfigsCore{TableMappings: []*protos.TableMapping{{
			SourceTableIdentifier:      schema + "." + table,
			DestinationTableIdentifier: table,
			Exclude:                    exclude,
		}}}
	}

	require.NoError(t, c.ValidateMirrorSource(ctx, config("tracked")))

	err := c.ValidateMirrorSource(ctx, config("missing"))
	_, ok := errors.AsType[*common.SourceTablesMissingError](err)
	require.True(t, ok, "%v", err)

	require.ErrorContains(t, c.ValidateMirrorSource(ctx, config("untracked")), "is not tracked by CDC")
	snapshotOnly := config("untracked")
	snapshotOnly.DoInitialSnapshot = true
	snapshotOnly.InitialSnapshotOnly = true
	require.NoError(t, c.ValidateMirrorSource(ctx, snapshotOnly))

	require.ErrorContains(t, c.ValidateMirrorSource(ctx, config("partial")), "does not capture columns [b]")
	require.NoError(t, c.ValidateMirrorSource(ctx, config("partial", "b")))
}

func TestSetupReplicationStoresStartLSN(t *testing.T) {
	c := newTestConnector(t)
	ctx := t.Context()
	schema := createTestSchema(t, c)
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s.t (id INT PRIMARY KEY)", quoteIdentifier(schema)))
	require.NoError(t, err)
	enableTableCDC(t, c, schema, "t")

	flowName := "sqlserver_setup_" + strings.ToLower(common.RandomString(8))
	_, err = c.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{schema + ".t": "t"},
	})
	require.NoError(t, err)

	offset, err := c.GetLastOffset(ctx, flowName)
	require.NoError(t, err)
	start, err := parseLSN(offset.Text)
	require.NoError(t, err)

	table, err := common.ParseTableIdentifier(schema + ".t")
	require.NoError(t, err)
	ci, err := c.getCaptureInstance(ctx, table)
	require.NoError(t, err)
	require.GreaterOrEqual(t, start.compare(ci.startLSN), 0)

	_, err = c.SetupReplication(ctx, shared.CatalogPool{}, &protos.SetupReplicationInput{
		FlowJobName:      flowName,
		TableNameMapping: map[string]string{schema + ".missing": "t"},
	})
	require.ErrorContains(t, err, "not tracked by CDC")
}
