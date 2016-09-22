package wal

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWAL(t *testing.T) {
	origMaxSegmentSize := maxSegmentSize
	defer func() {
		maxSegmentSize = origMaxSegmentSize
	}()
	maxSegmentSize = 5

	dir, err := ioutil.TempDir("", "waltest")
	if !assert.NoError(t, err) {
		return
	}
	defer os.RemoveAll(dir)

	wal, err := Open(dir, 0)
	if !assert.NoError(t, err) {
		return
	}
	defer wal.Close()

	r, err := wal.NewReader(nil)
	if !assert.NoError(t, err) {
		return
	}
	defer r.Close()

	testReadWrite := func(val string) bool {
		n, readErr := wal.Write([]byte(val))
		if !assert.NoError(t, readErr) {
			return false
		}
		if !assert.Equal(t, 1, n) {
			return false
		}

		b, readErr := r.Read()
		if !assert.NoError(t, readErr) {
			return false
		}
		if !assert.Equal(t, len(val), n) {
			return false
		}
		if !assert.Equal(t, val, string(b[:1])) {
			return false
		}

		return true
	}

	if !testReadWrite("1") {
		return
	}
	if !testReadWrite("2") {
		return
	}

	// Reopen WAL
	wal.Close()
	wal, err = Open(dir, 0)
	if !assert.NoError(t, err) {
		return
	}
	defer wal.Close()

	r2, err := wal.NewReader(r.Offset())
	if !assert.NoError(t, err) {
		return
	}

	if !testReadWrite("3") {
		return
	}

	r, err = wal.NewReader(nil)
	if !assert.NoError(t, err) {
		return
	}
	defer r.Close()

	for _, expected := range []string{"1", "2", "3"} {
		b, readErr := r.Read()
		if !assert.NoError(t, readErr) {
			return
		}
		if !assert.Equal(t, expected, string(b)) {
			return
		}
	}

	// Reader opened at prior offset should only get "3"
	b, readErr := r2.Read()
	if !assert.NoError(t, readErr) {
		return
	}
	if !assert.Equal(t, "3", string(b)) {
		return
	}

	// Truncate as of known offset, should not delete any files
	testTruncate(t, wal, r.Offset(), 1)

	// Truncate as of now, which should remove old log segment
	testTruncate(t, wal, newOffset(time.Now().UnixNano(), 0), 0)
}

func testTruncate(t *testing.T, wal *WAL, offset Offset, expectedSegments int) {
	err := wal.TruncateBefore(offset)
	if assert.NoError(t, err, "Should be able to truncate") {
		segments, err := ioutil.ReadDir(wal.dir)
		if assert.NoError(t, err, "Should be able to list segments") {
			assert.Equal(t, expectedSegments, len(segments))
		}
	}
}
