package connsqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"

	mssql "github.com/microsoft/go-mssqldb"
	"go.temporal.io/sdk/log"

	metadataStore "github.com/PeerDB-io/peerdb/flow/connectors/external_metadata"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

// SQL Server error numbers for failed logins
const (
	errLoginFailed       = 18456
	errLoginUntrusted    = 18452
	errCannotOpenDBLogin = 4060
)

type SqlServerConnector struct {
	*metadataStore.PostgresMetadata
	logger log.Logger
	config *protos.SqlServerConfig
	db     *sql.DB
}

func connectionString(config *protos.SqlServerConfig) string {
	query := url.Values{}
	query.Set("database", config.Database)
	query.Set("app name", "peerdb")
	u := url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(config.User, config.Password),
		Host:     shared.JoinHostPort(config.Server, config.Port),
		RawQuery: query.Encode(),
	}
	return u.String()
}

func NewSqlServerConnector(ctx context.Context, config *protos.SqlServerConfig) (*SqlServerConnector, error) {
	logger := internal.LoggerFromCtx(ctx)

	connector, err := mssql.NewConnector(connectionString(config))
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL Server connection config: %w", err)
	}
	db := sql.OpenDB(connector)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to SQL Server: %w", classifyConnectError(err))
	}

	pgMetadata, err := metadataStore.NewPostgresMetadata(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &SqlServerConnector{
		PostgresMetadata: pgMetadata,
		logger:           logger,
		config:           config,
		db:               db,
	}, nil
}

// classifyConnectError marks login failures as AuthError so drop flow skips source cleanup.
func classifyConnectError(err error) error {
	if msErr, ok := errors.AsType[mssql.Error](err); ok {
		switch msErr.Number {
		case errLoginFailed, errLoginUntrusted, errCannotOpenDBLogin:
			return exceptions.NewAuthError(err)
		}
	}
	return err
}

func (c *SqlServerConnector) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	if err := c.db.Close(); err != nil {
		c.logger.Error("failed to close SQL Server connection", slog.Any("error", err))
		return fmt.Errorf("failed to close SQL Server connection: %w", err)
	}
	return nil
}

func (c *SqlServerConnector) ConnectionActive(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *SqlServerConnector) GetVersion(ctx context.Context) (string, error) {
	var version string
	if err := c.db.QueryRowContext(ctx, "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))").Scan(&version); err != nil {
		return "", fmt.Errorf("failed to get SQL Server version: %w", err)
	}
	return version, nil
}
