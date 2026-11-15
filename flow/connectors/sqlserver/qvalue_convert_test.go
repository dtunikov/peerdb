package connsqlserver

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func TestQkindForType(t *testing.T) {
	cases := map[string]types.QValueKind{
		"bit":              types.QValueKindBoolean,
		"tinyint":          types.QValueKindUInt8,
		"smallint":         types.QValueKindInt16,
		"int":              types.QValueKindInt32,
		"bigint":           types.QValueKindInt64,
		"decimal":          types.QValueKindNumeric,
		"numeric":          types.QValueKindNumeric,
		"money":            types.QValueKindNumeric,
		"smallmoney":       types.QValueKindNumeric,
		"real":             types.QValueKindFloat32,
		"float":            types.QValueKindFloat64,
		"char":             types.QValueKindString,
		"varchar":          types.QValueKindString,
		"nchar":            types.QValueKindString,
		"nvarchar":         types.QValueKindString,
		"text":             types.QValueKindString,
		"ntext":            types.QValueKindString,
		"sysname":          types.QValueKindString,
		"date":             types.QValueKindDate,
		"datetime":         types.QValueKindTimestamp,
		"datetime2":        types.QValueKindTimestamp,
		"smalldatetime":    types.QValueKindTimestamp,
		"datetimeoffset":   types.QValueKindTimestampTZ,
		"time":             types.QValueKindTime,
		"binary":           types.QValueKindBytes,
		"varbinary":        types.QValueKindBytes,
		"image":            types.QValueKindBytes,
		"timestamp":        types.QValueKindBytes,
		"uniqueidentifier": types.QValueKindUUID,
		"NVARCHAR":         types.QValueKindString,
	}
	for typeName, expected := range cases {
		kind, err := qkindForType(typeName)
		require.NoError(t, err, typeName)
		require.Equal(t, expected, kind, typeName)
	}

	for _, typeName := range []string{"xml", "sql_variant", "hierarchyid", "geometry", "geography", "custom_clr"} {
		_, err := qkindForType(typeName)
		require.ErrorContains(t, err, "unsupported SQL Server type: "+typeName)
	}
}

func TestNewColumnMoneyPrecision(t *testing.T) {
	col, err := newColumn("m", "money", 19, 4, true)
	require.NoError(t, err)
	require.Equal(t, int16(19), col.precision)
	require.Equal(t, int16(4), col.scale)

	col, err = newColumn("m", "smallmoney", 10, 4, true)
	require.NoError(t, err)
	require.Equal(t, int16(10), col.precision)

	_, err = newColumn("x", "xml", 0, 0, true)
	require.ErrorContains(t, err, "column x")
}

func TestUUIDFromSqlServerBytes(t *testing.T) {
	// 6F9619FF-8B86-D011-B42D-00C04FC964FF as stored by SQL Server
	wire := []byte{0xFF, 0x19, 0x96, 0x6F, 0x86, 0x8B, 0x11, 0xD0, 0xB4, 0x2D, 0x00, 0xC0, 0x4F, 0xC9, 0x64, 0xFF}
	u, err := uuidFromSqlServerBytes(wire)
	require.NoError(t, err)
	require.Equal(t, uuid.MustParse("6F9619FF-8B86-D011-B42D-00C04FC964FF"), u)

	_, err = uuidFromSqlServerBytes([]byte{1, 2, 3})
	require.Error(t, err)
}

func TestQValueFromScanned(t *testing.T) {
	mustColumn := func(typeName string, precision, scale int16) column {
		col, err := newColumn("c", typeName, precision, scale, true)
		require.NoError(t, err)
		return col
	}
	offsetZone := time.FixedZone("+02:00", 2*60*60)
	cases := []struct {
		col      column
		val      any
		expected types.QValue
	}{
		{mustColumn("bit", 1, 0), true, types.QValueBoolean{Val: true}},
		{mustColumn("tinyint", 3, 0), int64(255), types.QValueUInt8{Val: 255}},
		{mustColumn("smallint", 5, 0), int64(-32768), types.QValueInt16{Val: -32768}},
		{mustColumn("int", 10, 0), int64(2147483647), types.QValueInt32{Val: 2147483647}},
		{mustColumn("bigint", 19, 0), int64(-9223372036854775808), types.QValueInt64{Val: -9223372036854775808}},
		{mustColumn("real", 24, 0), float64(1.5), types.QValueFloat32{Val: 1.5}},
		{mustColumn("float", 53, 0), float64(2.25), types.QValueFloat64{Val: 2.25}},
		{
			mustColumn("decimal", 20, 5), []byte("123456789.54321"),
			types.QValueNumeric{Val: decimal.RequireFromString("123456789.54321"), Precision: 20, Scale: 5},
		},
		{
			mustColumn("money", 19, 4), []byte("-922337203685477.5808"),
			types.QValueNumeric{Val: decimal.RequireFromString("-922337203685477.5808"), Precision: 19, Scale: 4},
		},
		{mustColumn("nvarchar", 0, 0), "héllo", types.QValueString{Val: "héllo"}},
		{mustColumn("varchar", 0, 0), []byte("abc"), types.QValueString{Val: "abc"}},
		{
			mustColumn("date", 10, 0), time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			types.QValueDate{Val: time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)},
		},
		{
			mustColumn("datetime2", 27, 7), time.Date(2024, 2, 29, 13, 14, 15, 123456700, time.UTC),
			types.QValueTimestamp{Val: time.Date(2024, 2, 29, 13, 14, 15, 123456700, time.UTC)},
		},
		{
			mustColumn("datetimeoffset", 34, 7), time.Date(2024, 2, 29, 13, 14, 15, 0, offsetZone),
			types.QValueTimestampTZ{Val: time.Date(2024, 2, 29, 11, 14, 15, 0, time.UTC)},
		},
		{
			mustColumn("time", 16, 7), time.Date(1, 1, 1, 13, 14, 15, 1234500, time.UTC),
			types.QValueTime{Val: 13*time.Hour + 14*time.Minute + 15*time.Second + 1234500},
		},
		{mustColumn("varbinary", 0, 0), []byte{0xDE, 0xAD}, types.QValueBytes{Val: []byte{0xDE, 0xAD}}},
		{
			mustColumn("uniqueidentifier", 0, 0),
			[]byte{0xFF, 0x19, 0x96, 0x6F, 0x86, 0x8B, 0x11, 0xD0, 0xB4, 0x2D, 0x00, 0xC0, 0x4F, 0xC9, 0x64, 0xFF},
			types.QValueUUID{Val: uuid.MustParse("6F9619FF-8B86-D011-B42D-00C04FC964FF")},
		},
		{mustColumn("int", 10, 0), nil, types.QValueNull(types.QValueKindInt32)},
	}
	for _, tc := range cases {
		qv, err := qvalueFromScanned(tc.col, tc.val)
		require.NoError(t, err, tc.col.typeName)
		require.Equal(t, tc.expected, qv, tc.col.typeName)
	}

	_, err := qvalueFromScanned(mustColumn("int", 10, 0), "not an int")
	require.ErrorContains(t, err, "unexpected string")
}
