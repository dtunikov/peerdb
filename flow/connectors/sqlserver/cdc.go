package connsqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	captureStartPollInterval = time.Second
	captureStartTimeout      = 2 * time.Minute
)

type captureInstance struct {
	name     string
	roleName sql.NullString
	startLSN lsn
}

// getCaptureInstance returns the newest capture instance of a table, or nil if the table is not tracked
func (c *SqlServerConnector) getCaptureInstance(ctx context.Context, table *common.QualifiedTable) (*captureInstance, error) {
	var ci captureInstance
	var start []byte
	err := c.db.QueryRowContext(ctx, `
		SELECT TOP 1 capture_instance, role_name, start_lsn
		FROM cdc.change_tables
		WHERE source_object_id = OBJECT_ID(QUOTENAME(@p1) + '.' + QUOTENAME(@p2))
		ORDER BY create_date DESC`, table.Namespace, table.Table).Scan(&ci.name, &ci.roleName, &start)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to get capture instance of %s: %w", table, err)
	}
	if ci.startLSN, err = lsnFromBytes(start); err != nil {
		return nil, fmt.Errorf("capture instance %s: %w", ci.name, err)
	}
	return &ci, nil
}

// getMaxLSN returns the zero LSN until the capture job records its first transaction
func (c *SqlServerConnector) getMaxLSN(ctx context.Context) (lsn, error) {
	var b []byte
	if err := c.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&b); err != nil {
		return lsn{}, fmt.Errorf("failed to get max LSN: %w", err)
	}
	if b == nil {
		return lsn{}, nil
	}
	return lsnFromBytes(b)
}

func (c *SqlServerConnector) EnsurePullability(
	ctx context.Context, req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	return nil, nil
}

func (c *SqlServerConnector) ExportTxSnapshot(
	context.Context, string, []*protos.TableMapping, map[string]string,
) (*protos.ExportTxSnapshotOutput, any, error) {
	return nil, nil, nil
}

func (c *SqlServerConnector) FinishExport(any) error {
	return nil
}

func (c *SqlServerConnector) SetupReplConn(context.Context, map[string]string) error {
	return nil
}

func (c *SqlServerConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	return nil
}

// SetupReplication stores the starting LSN. It waits until the capture job has scanned past the start of
// every mirrored capture instance, so the retention check never mistakes a fresh capture instance for cleanup.
func (c *SqlServerConnector) SetupReplication(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	req *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	var requiredLSN lsn
	for source := range req.TableNameMapping {
		table, err := common.ParseTableIdentifier(source)
		if err != nil {
			return model.SetupReplicationResult{}, err
		}
		ci, err := c.getCaptureInstance(ctx, table)
		if err != nil {
			return model.SetupReplicationResult{}, err
		}
		if ci == nil {
			return model.SetupReplicationResult{}, fmt.Errorf("table %s is not tracked by CDC", table)
		}
		if ci.startLSN.compare(requiredLSN) > 0 {
			requiredLSN = ci.startLSN
		}
	}

	startLSN, err := c.waitForMaxLSN(ctx, requiredLSN)
	if err != nil {
		return model.SetupReplicationResult{}, err
	}
	if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{Text: startLSN.String()}); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("failed to store starting LSN: %w", err)
	}
	c.logger.Info("[sqlserver] stored starting LSN", slog.String("lsn", startLSN.String()))
	return model.SetupReplicationResult{}, nil
}

func (c *SqlServerConnector) waitForMaxLSN(ctx context.Context, required lsn) (lsn, error) {
	deadline := time.Now().Add(captureStartTimeout)
	for {
		maxLSN, err := c.getMaxLSN(ctx)
		if err != nil {
			return lsn{}, err
		}
		if maxLSN != (lsn{}) && maxLSN.compare(required) >= 0 {
			return maxLSN, nil
		}
		if time.Now().After(deadline) {
			return lsn{}, fmt.Errorf("CDC capture job has not reached LSN %s after %s (max LSN %s), check that SQL Server Agent is running",
				required, captureStartTimeout, maxLSN)
		}
		select {
		case <-ctx.Done():
			return lsn{}, ctx.Err()
		case <-time.After(captureStartPollInterval):
		}
	}
}

func (c *SqlServerConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	flowName := ctx.Value(shared.FlowNameKey).(string)
	return c.SetLastOffset(ctx, flowName, lastOffset)
}

const (
	// how long to wait before re-checking max LSN when the capture job has nothing past the last LSN;
	// windows are pulled back to back while changes are pending
	cdcPollInterval        = time.Second
	noRecordsTimeout       = time.Hour
	idleCheckpointInterval = time.Minute

	initialWindowCommits = 16
	maxWindowCommits     = 1 << 20

	// how long the capture job may go without scanning the log before PullRecords fails
	captureStallTimeout       = 10 * time.Minute
	captureStallCheckInterval = time.Minute
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
			// start_lsn - commit lsn of the tx the row change belongs to
			// command_id - order of the change rows inside a transaction
			// seqval - log position of the operation, shared by an update's before and after images
			// operation - delete, insert, update before-image, update after-image
			// tran_end_time - tx commit timestamp
			// the before-image becomes UpdateRecord.OldItems; ClickHouse needs it to tombstone the old row when a primary key changes
			// reads the change table since fn_cdc_get_all_changes_* hides __$command_id, needed to follow the clustered
			// index without sorting the window; NOLOCK as in the function, so the capture job does not block the read
			changesQuery: fmt.Sprintf(`
				SELECT c.__$start_lsn, c.__$seqval, c.__$operation, m.tran_end_time, %s
				FROM cdc.%s c WITH (NOLOCK)
				LEFT JOIN cdc.lsn_time_mapping m ON m.start_lsn = c.__$start_lsn
				WHERE c.__$start_lsn >= @p1 AND c.__$start_lsn <= @p2
				ORDER BY c.__$start_lsn, c.__$command_id, c.__$seqval, c.__$operation`,
				strings.Join(selectList, ", "), quoteIdentifier(ci.name+"_CT")),
		})
	}
	return tables, nil
}

// windowEnd returns the k-th commit after last (or the last pending one when fewer are pending)
// and how many commits the window covers
func (c *SqlServerConnector) windowEnd(ctx context.Context, last lsn, maxLSN lsn, k uint32) (lsn, uint32, error) {
	var b []byte
	var commits int64
	if err := c.db.QueryRowContext(ctx, `
		SELECT MAX(start_lsn), COUNT(*) FROM (
			SELECT TOP (@p1) start_lsn FROM cdc.lsn_time_mapping
			WHERE start_lsn > @p2 AND start_lsn <= @p3
			ORDER BY start_lsn
		) w`, k, last[:], maxLSN[:]).Scan(&b, &commits); err != nil {
		return lsn{}, 0, fmt.Errorf("failed to compute window end: %w", err)
	}
	if b == nil {
		return maxLSN, 0, nil
	}
	end, err := lsnFromBytes(b)
	return end, uint32(commits), err
}

// checkCaptureJobScanning fails when the capture job stopped scanning the log. Max LSN alone cannot tell,
// it also stands still on an idle database. A running capture job records a scan session every polling
// interval, empty ones included, so a stale last scan means the job is not running.
func (c *SqlServerConnector) checkCaptureJobScanning(ctx context.Context) error {
	var secondsSinceScan sql.NullInt64
	var scanning sql.NullBool
	if err := c.db.QueryRowContext(ctx, `
		SELECT DATEDIFF(SECOND, MAX(end_time), GETDATE()),
			CAST(MAX(CASE WHEN scan_phase <> 'Done' THEN 1 ELSE 0 END) AS BIT)
		FROM sys.dm_cdc_log_scan_sessions
		WHERE session_id <> 0`).Scan(&secondsSinceScan, &scanning); err != nil {
		return fmt.Errorf("failed to read CDC log scan sessions: %w", err)
	}
	// a long scan of a large transaction is progress, not a stall
	if scanning.Bool {
		return nil
	}
	sinceScan := time.Duration(secondsSinceScan.Int64) * time.Second
	if secondsSinceScan.Valid && sinceScan < captureStallTimeout {
		return nil
	}
	// no sessions since the instance started means the job never ran
	if !secondsSinceScan.Valid {
		sinceScan = 0
	}
	return exceptions.NewSqlServerCaptureJobStalledError(sinceScan)
}

// checkRetention fails when cleanup removed changes at or after from in any table. Reading change tables
// returns no error for a removed range, and cleanup can run during the read, so it runs after the window.
// One query for all tables: cleanup moves cdc.change_tables.start_lsn, which fn_cdc_get_min_lsn returns.
func (c *SqlServerConnector) checkRetention(ctx context.Context, tables []*cdcTable, from lsn) error {
	rows, err := c.db.QueryContext(ctx, "SELECT capture_instance, start_lsn FROM cdc.change_tables")
	if err != nil {
		return fmt.Errorf("failed to get min LSNs of capture instances: %w", err)
	}
	type minLSNRow struct {
		captureInstance string
		minLSN          []byte
	}
	minLSNRows, err := collectRows(rows, func(rows *sql.Rows) (minLSNRow, error) {
		var r minLSNRow
		err := rows.Scan(&r.captureInstance, &r.minLSN)
		return r, err
	})
	if err != nil {
		return fmt.Errorf("failed to get min LSNs of capture instances: %w", err)
	}
	minLSNs := make(map[string][]byte, len(minLSNRows))
	for _, r := range minLSNRows {
		minLSNs[r.captureInstance] = r.minLSN
	}

	for _, table := range tables {
		b, ok := minLSNs[table.captureInstance]
		if !ok {
			return fmt.Errorf("capture instance %s of table %s no longer exists", table.captureInstance, table.sourceName)
		}
		minLSN, err := lsnFromBytes(b)
		if err != nil {
			return fmt.Errorf("capture instance %s: %w", table.captureInstance, err)
		}
		if minLSN.compare(from) > 0 {
			return exceptions.NewSqlServerCDCDataCleanedUpError(table.captureInstance, minLSN.String(), from.String())
		}
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

// adjustWindowCommits sizes the next window from how many records the last one produced.
// Commits vary a lot in size, so a fixed k either overshoots the batch on large transactions
// or wastes round trips on small ones. When the window alone exceeded the batch, shrink k in
// proportion so the next one lands near half a batch. When it filled less than a quarter, double k,
// but only if k was the limit: a window cut short by fewer pending commits says nothing about k,
// and growing there would let an idle mirror ratchet k up to the max before the next burst.
func adjustWindowCommits(k, windowCommits, windowRecords, maxBatchSize uint32) uint32 {
	if windowRecords > maxBatchSize {
		target := max(maxBatchSize/2, 1)
		return max(uint32(uint64(k)*uint64(target)/uint64(windowRecords)), 1)
	}
	if windowRecords < maxBatchSize/4 && windowCommits >= k {
		return min(k*2, maxWindowCommits)
	}
	return k
}

type pullState struct {
	req                             *model.PullRecordsRequest[model.RecordItems]
	otelManager                     *otel_metrics.OtelManager
	utcOffset                       time.Duration
	recordCount                     atomic.Uint32
	sourceSchemaAsDestinationColumn bool
}

func (s *pullState) addRecord(ctx context.Context, record model.Record[model.RecordItems]) error {
	if err := s.req.RecordStream.AddRecord(ctx, record); err != nil {
		return err
	}
	s.recordCount.Add(1)
	return nil
}

// PullRecords reads change tables in windows of k commits, starting after the checkpointed LSN.
//
// Each iteration:
//  1. waits for the capture job to advance max LSN past last
//  2. picks the window end: the k-th commit after last (from cdc.lsn_time_mapping), capped at max LSN
//  3. reads every table's changes in (last, to] in parallel
//  4. moves last to the window end and adjusts k for the next window
//
// Windows end on a commit boundary and every table is read up to the same LSN, so the checkpoint
// never splits a transaction and stays valid for all tables. Bounding by commits instead of reading
// up to max LSN keeps a single query from returning an unbounded backlog after a long pause.
// k survives across batches on the connector, so the next batch starts with a tuned size.
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
	parallelism := int(req.SqlServerCdcConfig.GetPullParallelism())
	if parallelism == 0 {
		parallelism = -1
	}

	state := &pullState{
		req:                             req,
		otelManager:                     otelManager,
		utcOffset:                       utcOffset,
		sourceSchemaAsDestinationColumn: sourceSchemaAsDestinationColumn,
	}
	k := c.windowCommits.Load()
	if k == 0 {
		k = initialWindowCommits
	}
	c.logger.Info("[sqlserver] pulling records", slog.String("lastLSN", last.String()), slog.Int("tables", len(tables)),
		slog.Uint64("windowCommits", uint64(k)))
	defer func() { c.windowCommits.Store(k) }()
	noRecordsDeadline := time.Now().Add(noRecordsTimeout)
	var idleDeadline time.Time
	var lastIdleCheckpoint time.Time
	var caughtUpSince, lastStallCheck time.Time
	stallCheckEnabled := true
	// The loop stops when:
	// - the batch is full OR
	// - IdleTimeout passes after the first record OR
	// - noRecordsTimeout passes without any records
	for state.recordCount.Load() < req.MaxBatchSize &&
		(state.recordCount.Load() == 0 || time.Now().Before(idleDeadline)) &&
		(state.recordCount.Load() > 0 || time.Now().Before(noRecordsDeadline)) {
		maxLSN, err := c.getMaxLSN(ctx)
		if err != nil {
			return err
		}
		// waits for the capture job to advance max LSN past last
		if maxLSN.compare(last) <= 0 {
			// caught up, so an idle mirror does not keep reporting the lag of its last change
			if otelManager != nil {
				otelManager.Metrics.SourceLagGauge.Record(ctx, 0)
			}
			if caughtUpSince.IsZero() {
				caughtUpSince = time.Now()
			}

			// max lsn also stands still on an idle database, so a stopped capture job (e.g. SQL Server Agent down)
			// would otherwise look like no changes
			// error out if capture job hasn't scanned anything for more than captureStallTimeout
			if stallCheckEnabled && time.Since(caughtUpSince) >= captureStallTimeout &&
				time.Since(lastStallCheck) >= captureStallCheckInterval {
				lastStallCheck = time.Now()
				if err := c.checkCaptureJobScanning(ctx); err != nil {
					if _, ok := errors.AsType[*exceptions.SqlServerCaptureJobStalledError](err); ok {
						return err
					}
					// best effort: the DMV needs VIEW DATABASE (PERFORMANCE) STATE
					c.logger.Warn("[sqlserver] unable to check CDC capture job progress", slog.Any("error", err))
					stallCheckEnabled = false
				}
			}
			wait := cdcPollInterval
			if state.recordCount.Load() > 0 {
				wait = min(wait, time.Until(idleDeadline))
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		caughtUpSince = time.Time{}

		to, windowCommits, err := c.windowEnd(ctx, last, maxLSN, k)
		if err != nil {
			return err
		}
		from := last.next()
		recordsBefore := state.recordCount.Load()

		// reads every table's changes in (from, to] in parallel
		var windowRecords atomic.Uint32
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(parallelism)
		for _, table := range tables {
			group.Go(func() error {
				rows, err := c.pullTableWindow(groupCtx, state, table, from, to)
				windowRecords.Add(rows)
				return err
			})
		}
		if err := group.Wait(); err != nil {
			return err
		}
		if err := c.checkRetention(ctx, tables, from); err != nil {
			return err
		}

		// moves last to the window end
		last = to
		req.RecordStream.UpdateLatestCheckpointText(last.String())
		if recordsBefore == 0 && state.recordCount.Load() > 0 {
			idleDeadline = time.Now().Add(req.IdleTimeout)
		}
		// nothing to sync yet, so the catalog can move forward and stay ahead of CDC cleanup
		if state.recordCount.Load() == 0 && time.Since(lastIdleCheckpoint) >= idleCheckpointInterval {
			if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{Text: last.String()}); err != nil {
				return fmt.Errorf("failed to store idle checkpoint: %w", err)
			}
			lastIdleCheckpoint = time.Now()
		}
		// adjusts k for the next window
		if newK := adjustWindowCommits(k, windowCommits, windowRecords.Load(), req.MaxBatchSize); newK != k {
			c.logger.Info("[sqlserver] adjusted CDC window size",
				slog.Uint64("oldWindowCommits", uint64(k)),
				slog.Uint64("newWindowCommits", uint64(newK)),
				slog.Uint64("lastWindowCommits", uint64(windowCommits)),
				slog.Uint64("lastWindowRecords", uint64(windowRecords.Load())),
				slog.Uint64("maxBatchSize", uint64(req.MaxBatchSize)))
			k = newK
		}
	}

	c.logger.Info("[sqlserver] pulled records", slog.Uint64("records", uint64(state.recordCount.Load())), slog.String("lastLSN", last.String()),
		slog.Uint64("windowCommits", uint64(k)))
	return nil
}

// pullTableWindow emits the table's changes in (from-1, to] and returns how many records it emitted.
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
		count++
		if state.otelManager != nil && commitTime.Valid {
			state.otelManager.Metrics.SourceLagGauge.Record(ctx, time.Since(commitTime.Time.Add(-state.utcOffset)).Milliseconds())
		}
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("failed to read changes of %s: %w", table.sourceName, err)
	}
	return count, nil
}
