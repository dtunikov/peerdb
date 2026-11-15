package connsqlserver

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// column describes a SQL Server column as read from sys.columns
type column struct {
	name      string
	typeName  string
	kind      types.QValueKind
	precision int16
	scale     int16
	nullable  bool
}

func (c column) typeModifier() int32 {
	if c.kind == types.QValueKindNumeric {
		return datatypes.MakeNumericTypmod(int32(c.precision), int32(c.scale))
	}
	return -1
}

// qkindForType maps a SQL Server system type name to a QValueKind
func qkindForType(typeName string) (types.QValueKind, error) {
	switch strings.ToLower(typeName) {
	case "bit":
		return types.QValueKindBoolean, nil
	case "tinyint":
		return types.QValueKindUInt8, nil
	case "smallint":
		return types.QValueKindInt16, nil
	case "int":
		return types.QValueKindInt32, nil
	case "bigint":
		return types.QValueKindInt64, nil
	case "decimal", "numeric", "money", "smallmoney":
		return types.QValueKindNumeric, nil
	case "real":
		return types.QValueKindFloat32, nil
	case "float":
		return types.QValueKindFloat64, nil
	case "char", "varchar", "nchar", "nvarchar", "text", "ntext", "sysname":
		return types.QValueKindString, nil
	case "date":
		return types.QValueKindDate, nil
	case "datetime", "datetime2", "smalldatetime":
		return types.QValueKindTimestamp, nil
	case "datetimeoffset":
		return types.QValueKindTimestampTZ, nil
	case "time":
		return types.QValueKindTime, nil
	case "binary", "varbinary", "image", "timestamp", "rowversion":
		return types.QValueKindBytes, nil
	case "uniqueidentifier":
		return types.QValueKindUUID, nil
	default:
		return types.QValueKindInvalid, fmt.Errorf("unsupported SQL Server type: %s", typeName)
	}
}

// newColumn also pins money precision and scale, which sys.columns reports per storage size
func newColumn(name string, typeName string, precision int16, scale int16, nullable bool) (column, error) {
	kind, err := qkindForType(typeName)
	if err != nil {
		return column{}, fmt.Errorf("column %s: %w", name, err)
	}
	switch strings.ToLower(typeName) {
	case "money":
		precision, scale = 19, 4
	case "smallmoney":
		precision, scale = 10, 4
	}
	return column{
		name:      name,
		typeName:  typeName,
		kind:      kind,
		precision: precision,
		scale:     scale,
		nullable:  nullable,
	}, nil
}

// uuidFromSqlServerBytes converts the mixed-endian uniqueidentifier wire format:
// the first three groups are little-endian, the rest are big-endian.
func uuidFromSqlServerBytes(b []byte) (uuid.UUID, error) {
	if len(b) != 16 {
		return uuid.UUID{}, fmt.Errorf("invalid uniqueidentifier length %d", len(b))
	}
	var u uuid.UUID
	copy(u[:], b)
	u[0], u[1], u[2], u[3] = u[3], u[2], u[1], u[0]
	u[4], u[5] = u[5], u[4]
	u[6], u[7] = u[7], u[6]
	return u, nil
}

func timeOfDay(t time.Time) time.Duration {
	return time.Duration(t.Hour())*time.Hour +
		time.Duration(t.Minute())*time.Minute +
		time.Duration(t.Second())*time.Second +
		time.Duration(t.Nanosecond())
}

// qvalueFromScanned converts a value scanned by go-mssqldb into a QValue of the column's kind.
func qvalueFromScanned(col column, val any) (types.QValue, error) {
	if val == nil {
		return types.QValueNull(col.kind), nil
	}

	switch col.kind {
	case types.QValueKindBoolean:
		if v, ok := val.(bool); ok {
			return types.QValueBoolean{Val: v}, nil
		}
	case types.QValueKindUInt8:
		if v, ok := val.(int64); ok {
			return types.QValueUInt8{Val: uint8(v)}, nil
		}
	case types.QValueKindInt16:
		if v, ok := val.(int64); ok {
			return types.QValueInt16{Val: int16(v)}, nil
		}
	case types.QValueKindInt32:
		if v, ok := val.(int64); ok {
			return types.QValueInt32{Val: int32(v)}, nil
		}
	case types.QValueKindInt64:
		if v, ok := val.(int64); ok {
			return types.QValueInt64{Val: v}, nil
		}
	case types.QValueKindFloat32:
		if v, ok := val.(float64); ok {
			return types.QValueFloat32{Val: float32(v)}, nil
		}
	case types.QValueKindFloat64:
		if v, ok := val.(float64); ok {
			return types.QValueFloat64{Val: v}, nil
		}
	case types.QValueKindNumeric:
		var s string
		switch v := val.(type) {
		case []byte:
			s = string(v)
		case string:
			s = v
		default:
			return nil, fmt.Errorf("unexpected %T for numeric column %s", val, col.name)
		}
		d, err := decimal.NewFromString(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse numeric column %s: %w", col.name, err)
		}
		return types.QValueNumeric{Val: d, Precision: col.precision, Scale: col.scale}, nil
	case types.QValueKindString:
		switch v := val.(type) {
		case string:
			return types.QValueString{Val: v}, nil
		case []byte:
			return types.QValueString{Val: string(v)}, nil
		}
	case types.QValueKindDate:
		if v, ok := val.(time.Time); ok {
			return types.QValueDate{Val: time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, time.UTC)}, nil
		}
	case types.QValueKindTimestamp:
		if v, ok := val.(time.Time); ok {
			// datetime types have no zone; keep the wall clock as UTC
			return types.QValueTimestamp{Val: time.Date(
				v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), v.Nanosecond(), time.UTC)}, nil
		}
	case types.QValueKindTimestampTZ:
		if v, ok := val.(time.Time); ok {
			return types.QValueTimestampTZ{Val: v.UTC()}, nil
		}
	case types.QValueKindTime:
		if v, ok := val.(time.Time); ok {
			return types.QValueTime{Val: timeOfDay(v)}, nil
		}
	case types.QValueKindBytes:
		if v, ok := val.([]byte); ok {
			return types.QValueBytes{Val: v}, nil
		}
	case types.QValueKindUUID:
		if v, ok := val.([]byte); ok {
			u, err := uuidFromSqlServerBytes(v)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", col.name, err)
			}
			return types.QValueUUID{Val: u}, nil
		}
	}
	return nil, fmt.Errorf("unexpected %T for %s column %s (%s)", val, col.kind, col.name, col.typeName)
}
