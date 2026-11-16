package connsqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// SQL Server 2016
const minMajorVersion = 13

func (c *SqlServerConnector) getMajorVersion(ctx context.Context) (int, error) {
	var major int
	if err := c.db.QueryRowContext(ctx,
		"SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT)",
	).Scan(&major); err != nil {
		return 0, fmt.Errorf("failed to get SQL Server major version: %w", err)
	}
	return major, nil
}

func (c *SqlServerConnector) ValidateCheck(ctx context.Context) error {
	major, err := c.getMajorVersion(ctx)
	if err != nil {
		return err
	}
	if major < minMajorVersion {
		return fmt.Errorf("SQL Server must be version 2016 (13.x) or above, current major version: %d", major)
	}
	return nil
}

func (c *SqlServerConnector) tableExists(ctx context.Context, table *common.QualifiedTable) (bool, error) {
	var exists bool
	if err := c.db.QueryRowContext(ctx,
		"SELECT CAST(CASE WHEN OBJECT_ID(QUOTENAME(@p1) + '.' + QUOTENAME(@p2), 'U') IS NULL THEN 0 ELSE 1 END AS BIT)",
		table.Namespace, table.Table,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check table %s: %w", table, err)
	}
	return exists, nil
}

func (c *SqlServerConnector) capturedColumns(ctx context.Context, ci *captureInstance) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT cc.column_name
		FROM cdc.captured_columns cc
		JOIN cdc.change_tables ct ON ct.object_id = cc.object_id
		WHERE ct.capture_instance = @p1`, ci.name)
	if err != nil {
		return nil, fmt.Errorf("failed to get captured columns of %s: %w", ci.name, err)
	}
	return collectRows(rows, func(rows *sql.Rows) (string, error) {
		var name string
		err := rows.Scan(&name)
		return name, err
	})
}

func (c *SqlServerConnector) canReadCaptureInstance(ctx context.Context, ci *captureInstance) (bool, error) {
	var canRead bool
	if err := c.db.QueryRowContext(ctx, `
		SELECT CAST(CASE
			WHEN HAS_PERMS_BY_NAME('cdc.' + QUOTENAME(@p1 + '_CT'), 'OBJECT', 'SELECT') = 1
				AND (@p2 IS NULL OR IS_ROLEMEMBER(@p2) = 1 OR IS_SRVROLEMEMBER('sysadmin') = 1)
			THEN 1 ELSE 0 END AS BIT)`, ci.name, ci.roleName,
	).Scan(&canRead); err != nil {
		return false, fmt.Errorf("failed to check permissions on capture instance %s: %w", ci.name, err)
	}
	return canRead, nil
}

// checkCaptureJob is best effort: reading msdb needs extra permissions
func (c *SqlServerConnector) checkCaptureJob(ctx context.Context) error {
	var count int
	if err := c.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = 'capture'",
	).Scan(&count); err != nil {
		c.logger.Warn("[sqlserver] unable to check the CDC capture job", slog.Any("error", err))
		return nil
	}
	if count == 0 {
		return errors.New("CDC capture job does not exist for this database")
	}
	return nil
}

func missingColumns(required []column, captured []string) []string {
	var missing []string
	for _, col := range required {
		if !slices.Contains(captured, col.name) {
			missing = append(missing, col.name)
		}
	}
	return missing
}

func (c *SqlServerConnector) ValidateMirrorSource(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
	tables := make([]*common.QualifiedTable, 0, len(cfg.TableMappings))
	var missingTables []common.QualifiedTable
	for _, tm := range cfg.TableMappings {
		table, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
		if err != nil {
			return fmt.Errorf("invalid source table identifier: %w", err)
		}
		exists, err := c.tableExists(ctx, table)
		if err != nil {
			return err
		}
		if !exists {
			missingTables = append(missingTables, *table)
		}
		tables = append(tables, table)
	}
	if len(missingTables) > 0 {
		return common.NewSourceTablesMissingError(missingTables)
	}

	tableColumns := make([][]column, len(tables))
	for i, tm := range cfg.TableMappings {
		columns, _, err := c.getTableColumns(ctx, tables[i], tm.Exclude)
		if err != nil {
			return err
		}
		tableColumns[i] = columns
	}

	if cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly {
		return nil
	}

	var cdcEnabled bool
	if err := c.db.QueryRowContext(ctx, "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()").Scan(&cdcEnabled); err != nil {
		return fmt.Errorf("failed to check if CDC is enabled: %w", err)
	}
	if !cdcEnabled {
		return errors.New("CDC is not enabled on the database, run sys.sp_cdc_enable_db")
	}
	if err := c.checkCaptureJob(ctx); err != nil {
		return err
	}

	for i, table := range tables {
		ci, err := c.getCaptureInstance(ctx, table)
		if err != nil {
			return err
		}
		if ci == nil {
			return fmt.Errorf("table %s is not tracked by CDC, run sys.sp_cdc_enable_table", table)
		}
		if canRead, err := c.canReadCaptureInstance(ctx, ci); err != nil {
			return err
		} else if !canRead {
			return fmt.Errorf("user cannot read changes of capture instance %s (needs SELECT on its change table and its gating role)",
				ci.name)
		}

		captured, err := c.capturedColumns(ctx, ci)
		if err != nil {
			return err
		}
		if missing := missingColumns(tableColumns[i], captured); len(missing) > 0 {
			return fmt.Errorf("capture instance %s of table %s does not capture columns %v, exclude them or recreate the capture instance",
				ci.name, table, missing)
		}
	}
	return nil
}
