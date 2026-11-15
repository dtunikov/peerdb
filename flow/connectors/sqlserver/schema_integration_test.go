//go:build tilt

package connsqlserver

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// createTestSchema creates a schema dropped with all its tables on cleanup
func createTestSchema(t *testing.T, c *SqlServerConnector) string {
	t.Helper()
	schema := "test_" + strings.ToLower(common.RandomString(8))
	_, err := c.db.ExecContext(t.Context(), "CREATE SCHEMA "+quoteIdentifier(schema))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx := t.Context()
		rows, err := c.db.QueryContext(ctx, "SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID(@p1)", schema)
		if err == nil {
			tables, _ := collectRows(rows, func(rows *sql.Rows) (string, error) {
				var name string
				err := rows.Scan(&name)
				return name, err
			})
			for _, table := range tables {
				_, _ = c.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s.%s", quoteIdentifier(schema), quoteIdentifier(table)))
			}
		}
		_, _ = c.db.ExecContext(ctx, "DROP SCHEMA "+quoteIdentifier(schema))
	})
	return schema
}

const typesTableDDL = `CREATE TABLE %s.types (
	id INT NOT NULL,
	tenant TINYINT NOT NULL,
	c_bit BIT,
	c_smallint SMALLINT,
	c_bigint BIGINT,
	c_decimal DECIMAL(20,5),
	c_money MONEY,
	c_real REAL,
	c_float FLOAT,
	c_char CHAR(3),
	c_varchar VARCHAR(20),
	c_nvarchar_max NVARCHAR(MAX),
	c_date DATE,
	c_datetime DATETIME,
	c_datetime2 DATETIME2(7),
	c_datetimeoffset DATETIMEOFFSET,
	c_time TIME,
	c_varbinary VARBINARY(10),
	c_uuid UNIQUEIDENTIFIER,
	c_computed AS (id * 2),
	PRIMARY KEY (tenant, id)
)`

func TestGetTableSchemaAndValues(t *testing.T) {
	c := newTestConnector(t)
	ctx := t.Context()
	schema := createTestSchema(t, c)

	_, err := c.db.ExecContext(ctx, fmt.Sprintf(typesTableDDL, quoteIdentifier(schema)))
	require.NoError(t, err)
	_, err = c.db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s.types VALUES (
		1, 7, 1, -5, 9223372036854775807, 123456789.54321, 12.3456, 1.5, 2.25, 'abc', 'hello', N'héllo wörld',
		'2024-02-29', '2024-02-29 13:14:15.123', '2024-02-29 13:14:15.1234567', '2024-02-29 13:14:15 +02:00',
		'13:14:15.1234567', 0xDEAD, '6F9619FF-8B86-D011-B42D-00C04FC964FF')`, quoteIdentifier(schema)))
	require.NoError(t, err)

	tableID := schema + ".types"
	schemas, err := c.GetTableSchema(ctx, nil, 0, protos.TypeSystem_Q, []*protos.TableMapping{{
		SourceTableIdentifier: tableID,
		Exclude:               []string{"c_bit"},
	}})
	require.NoError(t, err)
	tableSchema := schemas[tableID]
	require.Equal(t, []string{"tenant", "id"}, tableSchema.PrimaryKeyColumns)

	expectedKinds := []types.QValueKind{
		types.QValueKindInt32, types.QValueKindUInt8, types.QValueKindInt16, types.QValueKindInt64,
		types.QValueKindNumeric, types.QValueKindNumeric, types.QValueKindFloat32, types.QValueKindFloat64,
		types.QValueKindString, types.QValueKindString, types.QValueKindString, types.QValueKindDate,
		types.QValueKindTimestamp, types.QValueKindTimestamp, types.QValueKindTimestampTZ, types.QValueKindTime,
		types.QValueKindBytes, types.QValueKindUUID,
	}
	require.Len(t, tableSchema.Columns, len(expectedKinds))
	for i, kind := range expectedKinds {
		require.Equal(t, string(kind), tableSchema.Columns[i].Type, tableSchema.Columns[i].Name)
	}
	require.False(t, tableSchema.Columns[0].Nullable)
	require.True(t, tableSchema.Columns[2].Nullable)

	table, err := common.ParseTableIdentifier(tableID)
	require.NoError(t, err)
	columns, _, err := c.getTableColumns(ctx, table, nil)
	require.NoError(t, err)
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, quoteIdentifier(col.name))
	}
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s", strings.Join(names, ", "), quoteTable(table)))
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	require.NoError(t, rows.Scan(ptrs...))
	require.NoError(t, rows.Err())

	got := make(map[string]types.QValue, len(columns))
	for i, col := range columns {
		qv, err := qvalueFromScanned(col, values[i])
		require.NoError(t, err, col.name)
		got[col.name] = qv
	}
	require.Equal(t, types.QValueInt32{Val: 1}, got["id"])
	require.Equal(t, types.QValueUInt8{Val: 7}, got["tenant"])
	require.Equal(t, types.QValueBoolean{Val: true}, got["c_bit"])
	require.Equal(t, types.QValueInt64{Val: 9223372036854775807}, got["c_bigint"])
	require.Equal(t, types.QValueNumeric{Val: decimal.RequireFromString("123456789.54321"), Precision: 20, Scale: 5}, got["c_decimal"])
	require.Equal(t, types.QValueNumeric{Val: decimal.RequireFromString("12.3456"), Precision: 19, Scale: 4}, got["c_money"])
	require.Equal(t, types.QValueFloat32{Val: 1.5}, got["c_real"])
	require.Equal(t, types.QValueString{Val: "abc"}, got["c_char"])
	require.Equal(t, types.QValueString{Val: "héllo wörld"}, got["c_nvarchar_max"])
	require.Equal(t, types.QValueDate{Val: time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)}, got["c_date"])
	require.Equal(t, types.QValueTimestamp{Val: time.Date(2024, 2, 29, 13, 14, 15, 123000000, time.UTC)}, got["c_datetime"])
	require.Equal(t, types.QValueTimestamp{Val: time.Date(2024, 2, 29, 13, 14, 15, 123456700, time.UTC)}, got["c_datetime2"])
	require.Equal(t, types.QValueTimestampTZ{Val: time.Date(2024, 2, 29, 11, 14, 15, 0, time.UTC)}, got["c_datetimeoffset"])
	require.Equal(t, types.QValueTime{Val: 13*time.Hour + 14*time.Minute + 15*time.Second + 123456700}, got["c_time"])
	require.Equal(t, types.QValueBytes{Val: []byte{0xDE, 0xAD}}, got["c_varbinary"])
	require.Equal(t, types.QValueUUID{Val: uuid.MustParse("6F9619FF-8B86-D011-B42D-00C04FC964FF")}, got["c_uuid"])
}

func TestGetTableSchemaUnsupportedType(t *testing.T) {
	c := newTestConnector(t)
	ctx := t.Context()
	schema := createTestSchema(t, c)
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s.t (id INT PRIMARY KEY, x XML)", quoteIdentifier(schema)))
	require.NoError(t, err)

	_, err = c.GetTableSchema(ctx, nil, 0, protos.TypeSystem_Q, []*protos.TableMapping{{SourceTableIdentifier: schema + ".t"}})
	require.ErrorContains(t, err, "unsupported SQL Server type: xml")

	_, err = c.GetTableSchema(ctx, nil, 0, protos.TypeSystem_Q, []*protos.TableMapping{{SourceTableIdentifier: schema + ".missing"}})
	require.ErrorContains(t, err, "not found")
}

func TestSchemaListing(t *testing.T) {
	c := newTestConnector(t)
	ctx := t.Context()
	schema := createTestSchema(t, c)
	_, err := c.db.ExecContext(ctx, fmt.Sprintf(typesTableDDL, quoteIdentifier(schema)))
	require.NoError(t, err)
	_, err = c.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s.no_pk (v INT)", quoteIdentifier(schema)))
	require.NoError(t, err)

	schemas, err := c.GetSchemas(ctx)
	require.NoError(t, err)
	require.Contains(t, schemas.Schemas, schema)
	require.NotContains(t, schemas.Schemas, "sys")
	require.NotContains(t, schemas.Schemas, "cdc")

	all, err := c.GetAllTables(ctx)
	require.NoError(t, err)
	require.Contains(t, all.Tables, schema+".types")

	tables, err := c.GetTablesInSchema(ctx, schema, false)
	require.NoError(t, err)
	require.Len(t, tables.Tables, 2)
	require.Equal(t, "no_pk", tables.Tables[0].TableName)
	require.False(t, tables.Tables[0].HasPrimaryKeyOrReplicaIdentity)
	require.True(t, tables.Tables[1].HasPrimaryKeyOrReplicaIdentity)
	require.True(t, tables.Tables[1].CanMirror)

	// not tracked by CDC
	tables, err = c.GetTablesInSchema(ctx, schema, true)
	require.NoError(t, err)
	require.False(t, tables.Tables[1].CanMirror)

	cols, err := c.GetColumns(ctx, 0, schema, "types")
	require.NoError(t, err)
	require.Len(t, cols.Columns, 19)
	require.True(t, cols.Columns[0].IsKey)
	require.Equal(t, "int", cols.Columns[0].Type)
	require.Equal(t, string(types.QValueKindInt32), cols.Columns[0].Qkind)
	require.False(t, cols.Columns[2].IsKey)
}
