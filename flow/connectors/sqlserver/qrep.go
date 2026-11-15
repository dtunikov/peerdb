package connsqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func isIntegerKind(kind types.QValueKind) bool {
	switch kind {
	case types.QValueKindUInt8, types.QValueKindInt16, types.QValueKindInt32, types.QValueKindInt64:
		return true
	default:
		return false
	}
}

func (c *SqlServerConnector) GetDefaultPartitionKeyForTables(
	ctx context.Context,
	input *protos.GetDefaultPartitionKeyForTablesInput,
) (*protos.GetDefaultPartitionKeyForTablesOutput, error) {
	output := &protos.GetDefaultPartitionKeyForTablesOutput{
		TableDefaultPartitionKeyMapping: make(map[string]string, len(input.TableMappings)),
	}
	for _, tm := range input.TableMappings {
		schema, ok := input.TableSchemaMapping[tm.SourceTableIdentifier]
		if !ok || len(schema.PrimaryKeyColumns) != 1 {
			continue
		}
		pkColumn := schema.PrimaryKeyColumns[0]
		idx := slices.IndexFunc(schema.Columns, func(col *protos.FieldDescription) bool { return col.Name == pkColumn })
		if idx >= 0 && isIntegerKind(types.QValueKind(schema.Columns[idx].Type)) {
			output.TableDefaultPartitionKeyMapping[tm.SourceTableIdentifier] = pkColumn
		}
	}
	return output, nil
}

func (c *SqlServerConnector) GetQRepPartitions(
	ctx context.Context,
	config *protos.QRepConfig,
	last *protos.QRepPartition,
) ([]*protos.QRepPartition, error) {
	if config.WatermarkColumn == "" || config.NumPartitionsOverride == 1 {
		return utils.FullTablePartition(), nil
	}
	if config.NumPartitionsOverride == 0 && config.NumRowsPerPartition == 0 {
		return nil, errors.New("num rows per partition must be greater than 0")
	}

	table, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse watermark table %s: %w", config.WatermarkTable, err)
	}
	columns, _, err := c.getTableColumns(ctx, table, config.Exclude)
	if err != nil {
		return nil, err
	}
	idx := slices.IndexFunc(columns, func(col column) bool { return col.name == config.WatermarkColumn })
	if idx < 0 {
		return nil, fmt.Errorf("watermark column %s not found in %s", config.WatermarkColumn, table)
	}
	if !isIntegerKind(columns[idx].kind) {
		c.logger.Info("watermark column is not an integer, using a full table partition",
			slog.String("table", table.String()), slog.String("column", config.WatermarkColumn))
		return utils.FullTablePartition(), nil
	}

	quotedColumn := quoteIdentifier(config.WatermarkColumn)
	query := "SELECT MIN(CAST(" + quotedColumn + " AS BIGINT)), MAX(CAST(" + quotedColumn + " AS BIGINT)), COUNT_BIG(*) FROM " +
		quoteTable(table)
	if last != nil && last.Range != nil {
		intRange, ok := last.Range.Range.(*protos.PartitionRange_IntRange)
		if !ok {
			return nil, fmt.Errorf("unexpected range type %T when resuming QRep", last.Range.Range)
		}
		query += fmt.Sprintf(" WHERE %s > %d", quotedColumn, intRange.IntRange.End)
	}

	var minVal, maxVal sql.NullInt64
	var totalRows int64
	if err := c.db.QueryRowContext(ctx, query).Scan(&minVal, &maxVal, &totalRows); err != nil {
		return nil, fmt.Errorf("failed to query watermark range of %s: %w", table, err)
	}
	if !minVal.Valid {
		if last != nil {
			return nil, nil
		}
		return utils.FullTablePartition(), nil
	}

	numPartitions := int64(config.NumPartitionsOverride)
	if numPartitions == 0 {
		numPartitions = shared.AdjustNumPartitions(totalRows, int64(config.NumRowsPerPartition)).AdjustedNumPartitions
	}
	c.logger.Info("partitioning table",
		slog.String("table", table.String()),
		slog.Int64("rows", totalRows),
		slog.Int64("partitions", numPartitions))

	partitionHelper := utils.NewPartitionHelper(c.logger)
	if err := partitionHelper.AddPartitionsWithRange(minVal.Int64, maxVal.Int64, numPartitions); err != nil {
		return nil, fmt.Errorf("failed to add partitions: %w", err)
	}
	if config.AddNullPartition {
		partitionHelper.AddNullPartition()
	}
	return partitionHelper.GetPartitions(), nil
}

func (c *SqlServerConnector) PullQRepRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	config *protos.QRepConfig,
	dstType protos.DBType,
	partition *protos.QRepPartition,
	stream *model.QRecordStream,
) (int64, int64, error) {
	query, err := c.qrepQuery(ctx, config, partition)
	if err != nil {
		return 0, 0, err
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query %s: %w", config.WatermarkTable, err)
	}
	defer rows.Close()

	columns, err := columnsFromResult(rows)
	if err != nil {
		return 0, 0, err
	}
	fields := make([]types.QField, 0, len(columns))
	for _, col := range columns {
		fields = append(fields, col.qField())
	}
	stream.SetSchema(types.NewQRecordSchema(fields))

	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	var totalRecords int64
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return 0, 0, fmt.Errorf("failed to scan row: %w", err)
		}
		record := make([]types.QValue, len(columns))
		for i, col := range columns {
			qv, err := qvalueFromScanned(col, values[i])
			if err != nil {
				return 0, 0, err
			}
			record[i] = qv
		}
		if err := stream.Send(ctx, record); err != nil {
			return 0, 0, fmt.Errorf("failed to send record to stream: %w", err)
		}
		totalRecords++
		if totalRecords%50000 == 0 {
			c.logger.Info("pulling records", slog.Int64("records", totalRecords))
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("failed to read rows of %s: %w", config.WatermarkTable, err)
	}

	c.logger.Info("pulled records", slog.Int64("records", totalRecords))
	return totalRecords, 0, nil
}

// qrepQuery returns the partition query, selecting the table's replicated columns unless a custom query is set
func (c *SqlServerConnector) qrepQuery(
	ctx context.Context, config *protos.QRepConfig, partition *protos.QRepPartition,
) (string, error) {
	table, err := common.ParseTableIdentifier(config.WatermarkTable)
	if err != nil {
		return "", fmt.Errorf("failed to parse watermark table %s: %w", config.WatermarkTable, err)
	}

	var filter, start, end string
	if !partition.FullTablePartition {
		quotedColumn := quoteIdentifier(config.WatermarkColumn)
		switch r := partition.Range.Range.(type) {
		case *protos.PartitionRange_IntRange:
			start = strconv.FormatInt(r.IntRange.Start, 10)
			end = strconv.FormatInt(r.IntRange.End, 10)
			filter = fmt.Sprintf(" WHERE %s BETWEEN %s AND %s", quotedColumn, start, end)
		case *protos.PartitionRange_NullRange:
			if config.Query != "" {
				return "", errors.New("can't construct a null range partition for custom queries")
			}
			filter = fmt.Sprintf(" WHERE %s IS NULL", quotedColumn)
		default:
			return "", fmt.Errorf("unsupported partition range type %T for %s", r, table)
		}
	}

	if config.Query != "" {
		if partition.FullTablePartition {
			return config.Query, nil
		}
		return utils.ExecuteTemplate(config.Query, map[string]string{"start": start, "end": end})
	}

	columns, _, err := c.getTableColumns(ctx, table, config.Exclude)
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("no columns selected for %s (check Exclude configuration)", table)
	}
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, quoteIdentifier(col.name))
	}
	return fmt.Sprintf("SELECT %s FROM %s%s", strings.Join(names, ", "), quoteTable(table), filter), nil
}

// columnsFromResult types result columns from the TDS metadata, so custom queries work too
func columnsFromResult(rows *sql.Rows) ([]column, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get result column types: %w", err)
	}
	columns := make([]column, 0, len(columnTypes))
	for _, ct := range columnTypes {
		var precision, scale int16
		if p, s, ok := ct.DecimalSize(); ok {
			precision, scale = int16(p), int16(s)
		}
		nullable, ok := ct.Nullable()
		col, err := newColumn(ct.Name(), ct.DatabaseTypeName(), precision, scale, nullable || !ok)
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, nil
}
