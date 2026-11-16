package connsqlserver

import (
	"cmp"
	"container/heap"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
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
	c.logger.Info("stored starting LSN", slog.String("lsn", startLSN.String()))
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
	cdcPollInterval        = time.Second
	noRecordsTimeout       = time.Hour
	idleCheckpointInterval = time.Minute
	// decoded changes each table reader keeps ahead of the merge
	tableChangesBuffer = 256
)

// __$operation values of change table rows
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

		// reads the change table directly: ORDER BY is its clustered index, so rows stream without a sort,
		// which fn_cdc_get_all_changes_* cannot do since it does not expose __$command_id
		tables = append(tables, &cdcTable{
			sourceTable:     table,
			sourceName:      source,
			destinationName: destination,
			captureInstance: ci.name,
			columns:         columns,
			changesQuery: fmt.Sprintf(`
				SELECT c.__$start_lsn, c.__$command_id, c.__$seqval, c.__$operation, m.tran_end_time, %s
				FROM cdc.%s c
				LEFT JOIN cdc.lsn_time_mapping m ON m.start_lsn = c.__$start_lsn
				WHERE c.__$start_lsn >= @p1 AND c.__$start_lsn <= @p2
				ORDER BY c.__$start_lsn, c.__$command_id, c.__$seqval, c.__$operation
				OPTION (LOOP JOIN)`,
				strings.Join(selectList, ", "), quoteIdentifier(ci.name+"_CT")),
		})
	}
	return tables, nil
}

// checkRetention fails if CDC cleanup may have removed changes at or after from. Reading the change
// tables directly does not raise an error for a cleaned up range, so this runs after every read:
// cleanup only moves a capture instance's start forward, so passing afterwards covers the whole read.
func (c *SqlServerConnector) checkRetention(ctx context.Context, tables []*cdcTable, from lsn) error {
	rows, err := c.db.QueryContext(ctx, "SELECT capture_instance, start_lsn FROM cdc.change_tables")
	if err != nil {
		return fmt.Errorf("failed to get min LSNs of capture instances: %w", err)
	}
	defer rows.Close()
	minLSNs := make(map[string]lsn)
	for rows.Next() {
		var name string
		var b []byte
		if err := rows.Scan(&name, &b); err != nil {
			return fmt.Errorf("failed to scan min LSN of capture instance: %w", err)
		}
		minLSN, err := lsnFromBytes(b)
		if err != nil {
			return fmt.Errorf("capture instance %s: %w", name, err)
		}
		minLSNs[name] = minLSN
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get min LSNs of capture instances: %w", err)
	}

	for _, table := range tables {
		minLSN, ok := minLSNs[table.captureInstance]
		if !ok {
			return fmt.Errorf("capture instance %s of %s no longer exists", table.captureInstance, table.sourceName)
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

// changeKey orders changes like the change tables' clustered index. Commit LSN and command ID also
// order changes of different tables, since they are per transaction rather than per table.
type changeKey struct {
	startLSN  lsn
	seqval    lsn
	commandID int
	operation int
}

func (k changeKey) compare(other changeKey) int {
	return cmp.Or(
		k.startLSN.compare(other.startLSN),
		cmp.Compare(k.commandID, other.commandID),
		k.seqval.compare(other.seqval),
		cmp.Compare(k.operation, other.operation),
	)
}

type change struct {
	record     model.Record[model.RecordItems]
	commitTime time.Time
	key        changeKey
}

type tableChange struct {
	change
	table int
}

// changeHeap holds the next change of each table that is still being read
type changeHeap []tableChange

func (h *changeHeap) Len() int { return len(*h) }
func (h *changeHeap) Less(i, j int) bool {
	if c := (*h)[i].key.compare((*h)[j].key); c != 0 {
		return c < 0
	}
	return (*h)[i].table < (*h)[j].table
}
func (h *changeHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *changeHeap) Push(x any)    { *h = append(*h, x.(tableChange)) }
func (h *changeHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

type pullState struct {
	req                             *model.PullRecordsRequest[model.RecordItems]
	otelManager                     *otel_metrics.OtelManager
	idleDeadline                    time.Time
	utcOffset                       time.Duration
	recordCount                     uint32
	sourceSchemaAsDestinationColumn bool
}

func (s *pullState) addRecord(ctx context.Context, ch change) error {
	if err := s.req.RecordStream.AddRecord(ctx, ch.record); err != nil {
		return err
	}
	if s.recordCount == 0 {
		s.idleDeadline = time.Now().Add(s.req.IdleTimeout)
	}
	s.recordCount++
	if s.otelManager != nil && !ch.commitTime.IsZero() {
		s.otelManager.Metrics.SourceLagGauge.Record(ctx, time.Since(ch.commitTime).Milliseconds())
	}
	return nil
}

// batchDone reports whether the batch is full or waited IdleTimeout since its first record
func (s *pullState) batchDone() bool {
	return s.recordCount >= s.req.MaxBatchSize || (s.recordCount > 0 && !time.Now().Before(s.idleDeadline))
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

	noRecordsDeadline := time.Now().Add(noRecordsTimeout)
	var lastIdleCheckpoint time.Time
	for !state.batchDone() && (state.recordCount > 0 || time.Now().Before(noRecordsDeadline)) {
		maxLSN, err := c.getMaxLSN(ctx)
		if err != nil {
			return err
		}
		if maxLSN.compare(last) <= 0 {
			wait := cdcPollInterval
			if state.recordCount > 0 {
				wait = min(wait, time.Until(state.idleDeadline))
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		from := last.next()
		to, err := c.pullChanges(ctx, state, tables, from, maxLSN)
		if err != nil {
			return err
		}
		if err := c.checkRetention(ctx, tables, from); err != nil {
			return err
		}
		last = to
		req.RecordStream.UpdateLatestCheckpointText(last.String())
		// nothing to sync yet, so the catalog can move forward and stay ahead of CDC cleanup
		if state.recordCount == 0 && time.Since(lastIdleCheckpoint) >= idleCheckpointInterval {
			if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{Text: last.String()}); err != nil {
				return fmt.Errorf("failed to store idle checkpoint: %w", err)
			}
			lastIdleCheckpoint = time.Now()
		}
	}

	c.logger.Info("pulled records", slog.Uint64("records", uint64(state.recordCount)), slog.String("lastLSN", last.String()))
	return nil
}

// pullChanges merges the tables' changes in (from-1, to] by commit and emits them. Once the batch is done
// it stops at the next commit boundary: every table is then read past the last emitted commit, so that
// commit is a consistent checkpoint for all tables. It returns the checkpoint, to if all changes were read.
func (c *SqlServerConnector) pullChanges(
	ctx context.Context, state *pullState, tables []*cdcTable, from lsn, to lsn,
) (lsn, error) {
	readCtx, cancelRead := context.WithCancel(ctx)
	group, groupCtx := errgroup.WithContext(readCtx)
	defer func() {
		cancelRead()
		_ = group.Wait()
	}()

	streams := make([]chan change, len(tables))
	for i, table := range tables {
		streams[i] = make(chan change, tableChangesBuffer)
		group.Go(func() error {
			return c.readTableChanges(groupCtx, state, table, from, to, streams[i])
		})
	}

	heads := make(changeHeap, 0, len(tables))
	// a reader closes its stream only after reading all of it, and otherwise fails through groupCtx
	receive := func(table int) error {
		select {
		case ch, ok := <-streams[table]:
			if ok {
				heap.Push(&heads, tableChange{change: ch, table: table})
			}
			return nil
		case <-groupCtx.Done():
			if err := group.Wait(); err != nil {
				return err
			}
			return groupCtx.Err()
		}
	}
	for i := range streams {
		if err := receive(i); err != nil {
			return lsn{}, err
		}
	}

	var commit lsn
	for heads.Len() > 0 {
		head := heads[0]
		if commit != (lsn{}) && head.key.startLSN != commit && state.batchDone() {
			return commit, nil
		}
		commit = head.key.startLSN
		heap.Pop(&heads)
		if err := state.addRecord(ctx, head.change); err != nil {
			return lsn{}, err
		}
		if err := receive(head.table); err != nil {
			return lsn{}, err
		}
	}
	return to, nil
}

// readTableChanges decodes the table's changes in (from-1, to] into out, and closes out once all are read
func (c *SqlServerConnector) readTableChanges(
	ctx context.Context, state *pullState, table *cdcTable, from lsn, to lsn, out chan<- change,
) error {
	rows, err := c.db.QueryContext(ctx, table.changesQuery, from[:], to[:])
	if err != nil {
		return fmt.Errorf("failed to read changes of %s: %w", table.sourceName, err)
	}
	defer rows.Close()

	values := make([]any, len(table.columns))
	var startLSN, seqval []byte
	var commandID, operation int
	var commitTime sql.NullTime
	ptrs := make([]any, 0, len(table.columns)+5)
	ptrs = append(ptrs, &startLSN, &commandID, &seqval, &operation, &commitTime)
	for i := range values {
		ptrs = append(ptrs, &values[i])
	}

	var before model.RecordItems
	var beforeSeqval []byte
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("failed to scan change of %s: %w", table.sourceName, err)
		}

		items := model.NewRecordItems(len(table.columns) + 1)
		for i, col := range table.columns {
			qv, err := qvalueFromScanned(col, values[i])
			if err != nil {
				return fmt.Errorf("table %s: %w", table.sourceName, err)
			}
			items.AddColumn(col.name, qv)
		}
		if state.sourceSchemaAsDestinationColumn {
			items.AddColumn("_peerdb_source_schema", types.QValueString{Val: table.sourceTable.Namespace})
		}

		var baseRecord model.BaseRecord
		var committedAt time.Time
		if commitTime.Valid {
			committedAt = commitTime.Time.Add(-state.utcOffset)
			baseRecord.CommitTimeNano = committedAt.UnixNano()
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
			return fmt.Errorf("unexpected CDC operation %d for %s", operation, table.sourceName)
		}

		key := changeKey{commandID: commandID, operation: operation}
		if key.startLSN, err = lsnFromBytes(startLSN); err != nil {
			return fmt.Errorf("start LSN of %s: %w", table.sourceName, err)
		}
		if key.seqval, err = lsnFromBytes(seqval); err != nil {
			return fmt.Errorf("seqval of %s: %w", table.sourceName, err)
		}
		select {
		case out <- change{key: key, record: record, commitTime: committedAt}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read changes of %s: %w", table.sourceName, err)
	}
	close(out)
	return nil
}
