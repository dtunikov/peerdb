package connsqlserver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLSNRoundTrip(t *testing.T) {
	l, err := parseLSN("0000002a000001f00003")
	require.NoError(t, err)
	require.Equal(t, lsn{0, 0, 0, 0x2a, 0, 0, 0x01, 0xf0, 0, 0x03}, l)
	require.Equal(t, "0000002a000001f00003", l.String())

	_, err = parseLSN("zz")
	require.Error(t, err)
	_, err = parseLSN("0102")
	require.ErrorContains(t, err, "invalid LSN length 2")
	_, err = lsnFromBytes(make([]byte, 11))
	require.Error(t, err)
}

func TestLSNCompareAndNext(t *testing.T) {
	a, err := parseLSN("0000002a000001f00003")
	require.NoError(t, err)
	b, err := parseLSN("0000002a000001f10000")
	require.NoError(t, err)
	require.Negative(t, a.compare(b))
	require.Positive(t, b.compare(a))
	require.Zero(t, a.compare(a))

	require.Equal(t, "0000002a000001f00004", a.next().String())
	carry, err := parseLSN("0000002a0000ffffffff")
	require.NoError(t, err)
	require.Equal(t, "0000002a000100000000", carry.next().String())
	require.Equal(t, "0000002a000001f00003", a.String(), "next must not mutate the receiver")
}

func TestMissingColumns(t *testing.T) {
	required := []column{{name: "id"}, {name: "a"}, {name: "b"}}
	require.Empty(t, missingColumns(required, []string{"b", "a", "id", "extra"}))
	require.Equal(t, []string{"a", "b"}, missingColumns(required, []string{"id"}))
}

func TestAdjustWindowCommits(t *testing.T) {
	// overshoot shrinks k so the next window targets half a batch
	require.Equal(t, uint32(64), adjustWindowCommits(256, 256, 2000, 1000))
	require.Equal(t, uint32(5), adjustWindowCommits(256, 256, 25000, 1000))
	require.Equal(t, uint32(1), adjustWindowCommits(1, 1, 2000, 1000))
	// small full window grows k
	require.Equal(t, uint32(512), adjustWindowCommits(256, 256, 100, 1000))
	require.Equal(t, uint32(maxWindowCommits), adjustWindowCommits(maxWindowCommits, maxWindowCommits, 0, 1000))
	// window cut short by pending commits keeps k
	require.Equal(t, uint32(256), adjustWindowCommits(256, 3, 100, 1000))
	require.Equal(t, uint32(256), adjustWindowCommits(256, 0, 0, 1000))
	// within band keeps k
	require.Equal(t, uint32(256), adjustWindowCommits(256, 256, 500, 1000))
}
