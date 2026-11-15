package connsqlserver

import (
	"context"
	"fmt"
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
