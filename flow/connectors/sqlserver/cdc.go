package connsqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
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

func (c *SqlServerConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()
	return errors.New("SQL Server CDC pull is not implemented yet")
}
