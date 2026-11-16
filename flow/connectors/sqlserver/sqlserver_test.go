package connsqlserver

import (
	"errors"
	"net/url"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
)

func TestConnectionStringEscapesCredentials(t *testing.T) {
	connStr := ConnectionString(&protos.SqlServerConfig{
		Server:   "db.example.com",
		Port:     1433,
		User:     "user@domain",
		Password: "p@ss;word=1&x",
		Database: "my db",
	})
	u, err := url.Parse(connStr)
	require.NoError(t, err)
	require.Equal(t, "sqlserver", u.Scheme)
	require.Equal(t, "db.example.com:1433", u.Host)
	require.Equal(t, "user@domain", u.User.Username())
	password, _ := u.User.Password()
	require.Equal(t, "p@ss;word=1&x", password)
	require.Equal(t, "my db", u.Query().Get("database"))

	_, err = mssql.NewConnector(connStr)
	require.NoError(t, err)
}

func TestClassifyConnectError(t *testing.T) {
	_, ok := errors.AsType[*exceptions.AuthError](classifyConnectError(mssql.Error{Number: errLoginFailed}))
	require.True(t, ok)

	_, ok = errors.AsType[*exceptions.AuthError](classifyConnectError(mssql.Error{Number: 1205}))
	require.False(t, ok)
}
