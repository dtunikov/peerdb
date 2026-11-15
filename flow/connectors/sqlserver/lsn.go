package connsqlserver

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// lsn is a SQL Server log sequence number, binary(10) compared as a big-endian integer
type lsn [10]byte

func lsnFromBytes(b []byte) (lsn, error) {
	var l lsn
	if len(b) != len(l) {
		return l, fmt.Errorf("invalid LSN length %d", len(b))
	}
	copy(l[:], b)
	return l, nil
}

func parseLSN(text string) (lsn, error) {
	b, err := hex.DecodeString(text)
	if err != nil {
		return lsn{}, fmt.Errorf("invalid LSN %q: %w", text, err)
	}
	return lsnFromBytes(b)
}

func (l lsn) String() string {
	return hex.EncodeToString(l[:])
}

func (l lsn) compare(other lsn) int {
	return bytes.Compare(l[:], other[:])
}

// next matches sys.fn_cdc_increment_lsn
func (l lsn) next() lsn {
	for i := len(l) - 1; i >= 0; i-- {
		l[i]++
		if l[i] != 0 {
			break
		}
	}
	return l
}
