package connsqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// schema_id 16384+ are fixed database role schemas
const userSchemaFilter = `s.schema_id < 16384 AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'cdc', 'guest')`

func quoteIdentifier(ident string) string {
	return "[" + strings.ReplaceAll(ident, "]", "]]") + "]"
}

func quoteTable(table *common.QualifiedTable) string {
	return quoteIdentifier(table.Namespace) + "." + quoteIdentifier(table.Table)
}

func collectRows[T any](rows *sql.Rows, scan func(*sql.Rows) (T, error)) ([]T, error) {
	defer rows.Close()
	var res []T
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, rows.Err()
}

type rawColumn struct {
	name       string
	typeName   string
	pkOrdinal  int
	precision  int16
	scale      int16
	nullable   bool
	isComputed bool
}

// getRawColumns returns columns in ordinal order. CDC captures computed columns as NULL, so they are flagged.
func (c *SqlServerConnector) getRawColumns(ctx context.Context, schema string, table string) ([]rawColumn, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT
			c.name,
			CASE WHEN t.is_user_defined = 1 AND t.is_assembly_type = 0 THEN TYPE_NAME(c.system_type_id) ELSE t.name END,
			CAST(c.precision AS SMALLINT),
			CAST(c.scale AS SMALLINT),
			c.is_nullable,
			ISNULL(CAST(ic.key_ordinal AS INT), 0),
			c.is_computed
		FROM sys.columns c
		JOIN sys.types t ON t.user_type_id = c.user_type_id
		LEFT JOIN sys.indexes i ON i.object_id = c.object_id AND i.is_primary_key = 1
		LEFT JOIN sys.index_columns ic
			ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.column_id = c.column_id
		WHERE c.object_id = OBJECT_ID(QUOTENAME(@p1) + '.' + QUOTENAME(@p2), 'U')
		ORDER BY c.column_id`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns of %s.%s: %w", schema, table, err)
	}
	return collectRows(rows, func(rows *sql.Rows) (rawColumn, error) {
		var col rawColumn
		err := rows.Scan(&col.name, &col.typeName, &col.precision, &col.scale, &col.nullable, &col.pkOrdinal, &col.isComputed)
		return col, err
	})
}

// getTableColumns returns the replicated columns of a table and its primary key in key order
func (c *SqlServerConnector) getTableColumns(
	ctx context.Context, table *common.QualifiedTable, exclude []string,
) ([]column, []string, error) {
	rawColumns, err := c.getRawColumns(ctx, table.Namespace, table.Table)
	if err != nil {
		return nil, nil, err
	}
	if len(rawColumns) == 0 {
		return nil, nil, fmt.Errorf("table %s not found", table)
	}

	columns := make([]column, 0, len(rawColumns))
	pkByOrdinal := make(map[int]string)
	for _, raw := range rawColumns {
		if raw.pkOrdinal > 0 {
			pkByOrdinal[raw.pkOrdinal] = raw.name
		}
		if raw.isComputed || slices.Contains(exclude, raw.name) {
			continue
		}
		col, err := newColumn(raw.name, raw.typeName, raw.precision, raw.scale, raw.nullable)
		if err != nil {
			return nil, nil, fmt.Errorf("table %s: %w", table, err)
		}
		columns = append(columns, col)
	}

	primaryKey := make([]string, len(pkByOrdinal))
	for ordinal, name := range pkByOrdinal {
		primaryKey[ordinal-1] = name
	}
	return columns, primaryKey, nil
}

func (c *SqlServerConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	res := make(map[string]*protos.TableSchema, len(tableMappings))
	for _, tm := range tableMappings {
		table, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
		if err != nil {
			return nil, err
		}
		columns, primaryKey, err := c.getTableColumns(ctx, table, tm.Exclude)
		if err != nil {
			return nil, err
		}

		fields := make([]*protos.FieldDescription, 0, len(columns))
		for _, col := range columns {
			fields = append(fields, &protos.FieldDescription{
				Name:         col.name,
				Type:         string(col.kind),
				TypeModifier: col.typeModifier(),
				Nullable:     col.nullable,
			})
		}
		res[tm.SourceTableIdentifier] = &protos.TableSchema{
			TableIdentifier:   tm.SourceTableIdentifier,
			PrimaryKeyColumns: primaryKey,
			System:            system,
			NullableEnabled:   nullableEnabled,
			Columns:           fields,
		}
	}
	return res, nil
}

func (c *SqlServerConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT s.name + '.' + t.name
		FROM sys.tables t
		JOIN sys.schemas s ON s.schema_id = t.schema_id
		WHERE t.is_ms_shipped = 0 AND `+userSchemaFilter+`
		ORDER BY s.name, t.name`)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}
	tables, err := collectRows(rows, func(rows *sql.Rows) (string, error) {
		var table string
		err := rows.Scan(&table)
		return table, err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read tables: %w", err)
	}
	return &protos.AllTablesResponse{Tables: tables}, nil
}

func (c *SqlServerConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT s.name FROM sys.schemas s WHERE `+userSchemaFilter+` ORDER BY s.name`)
	if err != nil {
		return nil, fmt.Errorf("failed to get schemas: %w", err)
	}
	schemas, err := collectRows(rows, func(rows *sql.Rows) (string, error) {
		var schema string
		err := rows.Scan(&schema)
		return schema, err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read schemas: %w", err)
	}
	return &protos.PeerSchemasResponse{Schemas: schemas}, nil
}

func (c *SqlServerConnector) GetTablesInSchema(
	ctx context.Context, schema string, cdcEnabled bool,
) (*protos.SchemaTablesResponse, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT
			t.name,
			t.is_tracked_by_cdc,
			CAST(CASE WHEN EXISTS (
				SELECT 1 FROM sys.indexes i WHERE i.object_id = t.object_id AND i.is_primary_key = 1
			) THEN 1 ELSE 0 END AS BIT)
		FROM sys.tables t
		JOIN sys.schemas s ON s.schema_id = t.schema_id
		WHERE s.name = @p1 AND t.is_ms_shipped = 0
		ORDER BY t.name`, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables in schema %s: %w", schema, err)
	}
	tables, err := collectRows(rows, func(rows *sql.Rows) (*protos.TableResponse, error) {
		var name string
		var trackedByCDC, hasPrimaryKey bool
		if err := rows.Scan(&name, &trackedByCDC, &hasPrimaryKey); err != nil {
			return nil, err
		}
		return &protos.TableResponse{
			TableName:                      name,
			CanMirror:                      !cdcEnabled || trackedByCDC,
			HasPrimaryKeyOrReplicaIdentity: hasPrimaryKey,
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read tables in schema %s: %w", schema, err)
	}
	return &protos.SchemaTablesResponse{Tables: tables}, nil
}

func (c *SqlServerConnector) GetColumns(
	ctx context.Context, version uint32, schema string, table string,
) (*protos.TableColumnsResponse, error) {
	rawColumns, err := c.getRawColumns(ctx, schema, table)
	if err != nil {
		return nil, err
	}
	columns := make([]*protos.ColumnsItem, 0, len(rawColumns))
	for _, raw := range rawColumns {
		if raw.isComputed {
			continue
		}
		// unsupported types are listed with an empty qkind
		var qkind string
		if kind, err := qkindForType(raw.typeName); err == nil {
			qkind = string(kind)
		}
		columns = append(columns, &protos.ColumnsItem{
			Name:     raw.name,
			Type:     raw.typeName,
			IsKey:    raw.pkOrdinal > 0,
			Qkind:    qkind,
			Nullable: raw.nullable,
		})
	}
	return &protos.TableColumnsResponse{Columns: columns}, nil
}
