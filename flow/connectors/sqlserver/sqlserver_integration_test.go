//go:build tilt

package connsqlserver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/internal"
)

func newTestConnector(t *testing.T) *SqlServerConnector {
	t.Helper()
	connector, err := NewSqlServerConnector(t.Context(), internal.GetSqlServerConfigFromEnv())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, connector.Close())
	})
	return connector
}

func TestConnectAndValidate(t *testing.T) {
	connector := newTestConnector(t)
	require.NoError(t, connector.ConnectionActive(t.Context()))
	require.NoError(t, connector.ValidateCheck(t.Context()))

	version, err := connector.GetVersion(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, version)
}

func TestConnectWrongPassword(t *testing.T) {
	config := internal.GetSqlServerConfigFromEnv()
	config.Password = "wrong-password"
	_, err := NewSqlServerConnector(t.Context(), config)
	require.Error(t, err)
	require.ErrorContains(t, err, "auth error")
}
