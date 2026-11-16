package connsqlserver

import (
	"container/heap"
	"fmt"
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

func TestChangeHeapOrder(t *testing.T) {
	key := func(commit byte, commandID int, seq byte, op int) changeKey {
		return changeKey{startLSN: lsn{9: commit}, commandID: commandID, seqval: lsn{9: seq}, operation: op}
	}
	h := changeHeap{}
	for _, c := range []tableChange{
		{change: change{key: key(2, 1, 5, opInsert)}, table: 0},
		{change: change{key: key(1, 2, 3, opInsert)}, table: 1},
		{change: change{key: key(1, 1, 4, opUpdateAfter)}, table: 0},
		{change: change{key: key(1, 1, 4, opUpdateBefore)}, table: 0},
		{change: change{key: key(2, 1, 5, opInsert)}, table: 1},
	} {
		heap.Push(&h, c)
	}
	var order []string
	for h.Len() > 0 {
		c := heap.Pop(&h).(tableChange)
		order = append(order, fmt.Sprintf("%d/%d/%d/%d", c.key.startLSN[9], c.key.commandID, c.key.operation, c.table))
	}
	// commit first, then command within the transaction, ties broken by table
	require.Equal(t, []string{"1/1/3/0", "1/1/4/0", "1/2/2/1", "2/1/2/0", "2/1/2/1"}, order)
}
