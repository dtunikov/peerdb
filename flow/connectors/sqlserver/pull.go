package connsqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	cdcPollInterval        = time.Second
	noRecordsTimeout       = time.Hour
	idleCheckpointInterval = time.Minute

	initialWindowCommits = 16
	maxWindowCommits     = 1 << 20
)

// __$operation values in 'all update old' mode
const (
	opDelete       = 1
	opInsert       = 2
	opUpdateBefore = 3
	opUpdateAfter  = 4
)

type cdcTable struct {
	sourceTable     *common.QualifiedTable
	sourceName      string
	destinationName string
	captureInstance string
	changesQuery    string
	columns         []column
}

func (c *SqlServerConnector) prepareCDCTables(ctx context.Context, req *model.PullRecordsRequest[model.RecordItems]) ([]*cdcTable, error) {
	sources := make([]string, 0, len(req.TableNameMapping))
	for source := range req.TableNameMapping {
		sources = append(sources, source)
	}
	slices.Sort(sources)

	tables := make([]*cdcTable, 0, len(sources))
	for _, source := range sources {
		table, err := common.ParseTableIdentifier(source)
		if err != nil {
			return nil, err
		}
		ci, err := c.getCaptureInstance(ctx, table)
		if err != nil {
			return nil, err
		}
		if ci == nil {
			return nil, fmt.Errorf("table %s is not tracked by CDC", table)
		}
		destination := req.TableNameMapping[source].Name
		schema, ok := req.TableNameSchemaMapping[destination]
		if !ok {
			return nil, fmt.Errorf("schema of %s not found", destination)
		}

		columns := make([]column, 0, len(schema.Columns))
		selectList := make([]string, 0, len(schema.Columns))
		for _, field := range schema.Columns {
			col := column{name: field.Name, typeName: field.Type, kind: types.QValueKind(field.Type), nullable: field.Nullable}
			if col.kind == types.QValueKindNumeric {
				col.precision, col.scale = common.ParseNumericTypmod(field.TypeModifier)
			}
			columns = append(columns, col)
			selectList = append(selectList, "c."+quoteIdentifier(field.Name))
		}

		tables = append(tables, &cdcTable{
			sourceTable:     table,
			sourceName:      source,
			destinationName: destination,
			captureInstance: ci.name,
			columns:         columns,
			changesQuery: fmt.Sprintf(`
				SELECT c.__$start_lsn, c.__$seqval, c.__$operation, m.tran_end_time, %s
				FROM cdc.%s(@p1, @p2, N'all update old') c
				LEFT JOIN cdc.lsn_time_mapping m ON m.start_lsn = c.__$start_lsn
				ORDER BY c.__$start_lsn, c.__$seqval, c.__$operation`,
				strings.Join(selectList, ", "), quoteIdentifier("fn_cdc_get_all_changes_"+ci.name)),
		})
	}
	return tables, nil
}

// windowEnd returns the k-th commit after last, or maxLSN when fewer commits are pending
func (c *SqlServerConnector) windowEnd(ctx context.Context, last lsn, maxLSN lsn, k uint32) (lsn, error) {
	var b []byte
	if err := c.db.QueryRowContext(ctx, `
		SELECT MAX(start_lsn) FROM (
			SELECT TOP (@p1) start_lsn FROM cdc.lsn_time_mapping
			WHERE start_lsn > @p2 AND start_lsn <= @p3
			ORDER BY start_lsn
		) w`, k, last[:], maxLSN[:]).Scan(&b); err != nil {
		return lsn{}, fmt.Errorf("failed to compute window end: %w", err)
	}
	if b == nil {
		return maxLSN, nil
	}
	return lsnFromBytes(b)
}

func (c *SqlServerConnector) checkRetention(ctx context.Context, table *cdcTable, from lsn) error {
	var b []byte
	if err := c.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn(@p1)", table.captureInstance).Scan(&b); err != nil {
		return fmt.Errorf("failed to get min LSN of %s: %w", table.captureInstance, err)
	}
	minLSN, err := lsnFromBytes(b)
	if err != nil {
		return fmt.Errorf("capture instance %s: %w", table.captureInstance, err)
	}
	if minLSN.compare(from) > 0 {
		return exceptions.NewSqlServerCDCDataCleanedUpError(table.captureInstance, minLSN.String(), from.String())
	}
	return nil
}

// serverUTCOffset is needed because lsn_time_mapping stores server local time
func (c *SqlServerConnector) serverUTCOffset(ctx context.Context) (time.Duration, error) {
	var minutes int
	if err := c.db.QueryRowContext(ctx, "SELECT DATEPART(TZOFFSET, SYSDATETIMEOFFSET())").Scan(&minutes); err != nil {
		return 0, fmt.Errorf("failed to get server time zone offset: %w", err)
	}
	return time.Duration(minutes) * time.Minute, nil
}

func adjustWindowCommits(k uint32, windowRows uint32, maxBatchSize uint32) uint32 {
	if windowRows > maxBatchSize {
		return max(k/2, 1)
	}
	if windowRows < maxBatchSize/4 {
		return min(k*2, maxWindowCommits)
	}
	return k
}

type pullState struct {
	req                             *model.PullRecordsRequest[model.RecordItems]
	otelManager                     *otel_metrics.OtelManager
	utcOffset                       time.Duration
	recordCount                     uint32
	sourceSchemaAsDestinationColumn bool
}

func (s *pullState) addRecord(ctx context.Context, record model.Record[model.RecordItems]) error {
	if err := s.req.RecordStream.AddRecord(ctx, record); err != nil {
		return err
	}
	s.recordCount++
	return nil
}

func (c *SqlServerConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()

	last, err := parseLSN(req.LastOffset.Text)
	if err != nil {
		return fmt.Errorf("invalid checkpoint: %w", err)
	}
	tables, err := c.prepareCDCTables(ctx, req)
	if err != nil {
		return err
	}
	utcOffset, err := c.serverUTCOffset(ctx)
	if err != nil {
		return err
	}
	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, req.Env)
	if err != nil {
		return err
	}

	state := &pullState{
		req:                             req,
		otelManager:                     otelManager,
		utcOffset:                       utcOffset,
		sourceSchemaAsDestinationColumn: sourceSchemaAsDestinationColumn,
	}
	c.logger.Info("pulling records", slog.String("lastLSN", last.String()), slog.Int("tables", len(tables)))

	k := c.windowCommits.Load()
	if k == 0 {
		k = initialWindowCommits
	}
	defer func() { c.windowCommits.Store(k) }()
	noRecordsDeadline := time.Now().Add(noRecordsTimeout)
	var idleDeadline time.Time
	var lastIdleCheckpoint time.Time
	for state.recordCount < req.MaxBatchSize &&
		(state.recordCount == 0 || time.Now().Before(idleDeadline)) &&
		(state.recordCount > 0 || time.Now().Before(noRecordsDeadline)) {
		maxLSN, err := c.getMaxLSN(ctx)
		if err != nil {
			return err
		}
		if maxLSN.compare(last) <= 0 {
			wait := cdcPollInterval
			if state.recordCount > 0 {
				wait = min(wait, time.Until(idleDeadline))
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		to, err := c.windowEnd(ctx, last, maxLSN, k)
		if err != nil {
			return err
		}
		from := last.next()
		recordsBefore := state.recordCount
		var windowRows uint32
		for _, table := range tables {
			if err := c.checkRetention(ctx, table, from); err != nil {
				return err
			}
			rows, err := c.pullTableWindow(ctx, state, table, from, to)
			if err != nil {
				return err
			}
			windowRows += rows
		}

		last = to
		req.RecordStream.UpdateLatestCheckpointText(last.String())
		if recordsBefore == 0 && state.recordCount > 0 {
			idleDeadline = time.Now().Add(req.IdleTimeout)
		}
		// nothing to sync yet, so the catalog can move forward and stay ahead of CDC cleanup
		if state.recordCount == 0 && time.Since(lastIdleCheckpoint) >= idleCheckpointInterval {
			if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{Text: last.String()}); err != nil {
				return fmt.Errorf("failed to store idle checkpoint: %w", err)
			}
			lastIdleCheckpoint = time.Now()
		}
		k = adjustWindowCommits(k, windowRows, req.MaxBatchSize)
	}

	c.logger.Info("pulled records", slog.Uint64("records", uint64(state.recordCount)), slog.String("lastLSN", last.String()))
	return nil
}

// pullTableWindow emits the table's changes in (from-1, to] and returns how many change rows it read
func (c *SqlServerConnector) pullTableWindow(
	ctx context.Context, state *pullState, table *cdcTable, from lsn, to lsn,
) (uint32, error) {
	rows, err := c.db.QueryContext(ctx, table.changesQuery, from[:], to[:])
	if err != nil {
		return 0, fmt.Errorf("failed to read changes of %s: %w", table.sourceName, err)
	}
	defer rows.Close()

	values := make([]any, len(table.columns))
	var startLSN, seqval []byte
	var operation int
	var commitTime sql.NullTime
	ptrs := make([]any, 0, len(table.columns)+4)
	ptrs = append(ptrs, &startLSN, &seqval, &operation, &commitTime)
	for i := range values {
		ptrs = append(ptrs, &values[i])
	}

	var count uint32
	var before model.RecordItems
	var beforeSeqval []byte
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return count, fmt.Errorf("failed to scan change of %s: %w", table.sourceName, err)
		}
		count++

		items := model.NewRecordItems(len(table.columns) + 1)
		for i, col := range table.columns {
			qv, err := qvalueFromScanned(col, values[i])
			if err != nil {
				return count, fmt.Errorf("table %s: %w", table.sourceName, err)
			}
			items.AddColumn(col.name, qv)
		}
		if state.sourceSchemaAsDestinationColumn {
			items.AddColumn("_peerdb_source_schema", types.QValueString{Val: table.sourceTable.Namespace})
		}

		var baseRecord model.BaseRecord
		if commitTime.Valid {
			baseRecord.CommitTimeNano = commitTime.Time.Add(-state.utcOffset).UnixNano()
		}

		var record model.Record[model.RecordItems]
		switch operation {
		case opInsert:
			record = &model.InsertRecord[model.RecordItems]{
				BaseRecord:           baseRecord,
				Items:                items,
				SourceTableName:      table.sourceName,
				DestinationTableName: table.destinationName,
			}
		case opDelete:
			record = &model.DeleteRecord[model.RecordItems]{
				BaseRecord:           baseRecord,
				Items:                items,
				SourceTableName:      table.sourceName,
				DestinationTableName: table.destinationName,
			}
		case opUpdateBefore:
			before = items
			beforeSeqval = slices.Clone(seqval)
			continue
		case opUpdateAfter:
			oldItems := model.NewRecordItems(0)
			if before.ColToVal != nil && slices.Equal(beforeSeqval, seqval) {
				oldItems = before
			}
			before, beforeSeqval = model.RecordItems{}, nil
			record = &model.UpdateRecord[model.RecordItems]{
				BaseRecord:           baseRecord,
				OldItems:             oldItems,
				NewItems:             items,
				SourceTableName:      table.sourceName,
				DestinationTableName: table.destinationName,
			}
		default:
			return count, fmt.Errorf("unexpected CDC operation %d for %s", operation, table.sourceName)
		}
		if err := state.addRecord(ctx, record); err != nil {
			return count, err
		}
		if state.otelManager != nil && commitTime.Valid {
			state.otelManager.Metrics.SourceLagGauge.Record(ctx, time.Since(commitTime.Time.Add(-state.utcOffset)).Milliseconds())
		}
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("failed to read changes of %s: %w", table.sourceName, err)
	}
	return count, nil
}
