package wal

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/oxtoacart/bpool"
	"github.com/stretchr/testify/assert"
)

func TestFileNaming(t *testing.T) {
	seq := newFileSequence()
	filename := filepath.Join("folder", sequenceToFilename(seq))
	assert.Equal(t, seq, filenameToSequence(filename))
	filename = filename + compressedSuffix
	assert.Equal(t, seq, filenameToSequence(filename))
}

func TestOffsetAfter(t *testing.T) {
	assert.True(t, NewOffset(0, 1).After(nil))
	assert.False(t, Offset(nil).After(NewOffset(0, 1)))

	assert.True(t, NewOffset(1, 0).After(nil))
	assert.False(t, Offset(nil).After(NewOffset(1, 0)))

	assert.True(t, NewOffset(1, 50).After(NewOffset(1, 0)))
	assert.False(t, NewOffset(1, 0).After(NewOffset(1, 50)))

	assert.True(t, NewOffset(2, 0).After(NewOffset(1, 50)))
	assert.False(t, NewOffset(1, 50).After(NewOffset(2, 0)))

	assert.False(t, Offset(nil).After(Offset(nil)))
	assert.False(t, NewOffset(1, 50).After(NewOffset(1, 50)))
}

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
	// defer os.RemoveAll(dir)

	wal, err := Open(&Opts{Dir: dir, SyncInterval: 1 * time.Millisecond, MaxMemoryBacklog: 1})
	if !assert.NoError(t, err) {
		return
	}
	defer wal.Close()

	bufferPool := bpool.NewBytePool(1, 65536)
	r, err := wal.NewReader("test", nil, bufferPool.Get)
	if !assert.NoError(t, err) {
		return
	}
	defer r.Close()

	testReadWrite := func(val string) bool {
		wal.log.Debugf("testReadWrite: %v", val)
		readErr := wal.Write([]byte(val))
		if !assert.NoError(t, readErr) {
			return false
		}

		wal.log.Debug("Reading")
		b, readErr := r.Read()
		if !assert.NoError(t, readErr) {
			return false
		}
		if !assert.Equal(t, val, string(b[:1])) {
			return false
		}
		wal.log.Debug("Read")

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

	latest, lc, err := wal.Latest()
	if !assert.NoError(t, err) {
		return
	}
	if !assert.EqualValues(t, 9, lc.Position()) {
		return
	}
	if !assert.Equal(t, "2", string(latest)) {
		return
	}

	wal, err = Open(&Opts{Dir: dir, SyncInterval: 0})
	if !assert.NoError(t, err) {
		return
	}
	defer wal.Close()

	r2, err := wal.NewReader("test", r.Offset(), bufferPool.Get)
	if !assert.NoError(t, err) {
		return
	}
	defer r2.Close()

	// Problem is here
	if !testReadWrite("3") {
		return
	}

	// Compress item 1
	err = wal.CompressBefore(r2.Offset())
	if !assert.NoError(t, err) {
		return
	}

	assertWALContents := func(entries []string) {
		wal.log.Debugf("Asserting WAL contents: %v", entries)
		// Read the full WAL again
		r, err = wal.NewReader("test", nil, bufferPool.Get)
		if !assert.NoError(t, err) {
			return
		}
		defer r.Close()

		for _, expected := range entries {
			b, readErr := r.Read()
			if !assert.NoError(t, readErr) {
				return
			}
			if !assert.Equal(t, expected, string(b)) {
				return
			}
		}
	}

	assertWALContents([]string{"1", "2", "3"})

	// Corrupt the Snappy WAL file
	files, _ := ioutil.ReadDir(dir)
	for _, fi := range files {
		name := filepath.Join(dir, fi.Name())
		file, _ := os.OpenFile(name, os.O_RDWR, 0644)
		if strings.HasSuffix(name, compressedSuffix) {
			w := snappy.NewWriter(file)
			lenBuf := make([]byte, 4)
			encoding.PutUint32(lenBuf, 100)
			_, err := w.Write(lenBuf)
			if err != nil {
				panic(err)
			}
			w.Flush()
			file.Write([]byte("garbage"))
		}
		file.Close()
	}

	assertWALContents([]string{"3"})

	// Reader opened at prior offset should only get "3"
	wal.log.Debug(r2.Offset())
	b, readErr := r2.Read()
	if !assert.NoError(t, readErr) {
		return
	}
	if !assert.Equal(t, "3", string(b)) {
		return
	}

	err = wal.Write([]byte("data to force new WAL"))
	if !assert.NoError(t, err) {
		return
	}

	// Truncate as of known offset, should not delete any files
	truncateErr := wal.TruncateBefore(r2.Offset())
	testTruncate(t, wal, truncateErr, 2)

	// Truncate as of now, which should remove old log segment
	truncateErr = wal.TruncateBeforeTime(time.Now())
	testTruncate(t, wal, truncateErr, 1)

	// Truncate to size 1, which should remove remaining log segment
	truncateErr = wal.TruncateToSize(1)
	testTruncate(t, wal, truncateErr, 0)
}

func testTruncate(t *testing.T, wal *WAL, err error, expectedSegments int) {
	if assert.NoError(t, err, "Should be able to truncate") {
		segments, err := ioutil.ReadDir(wal.dir)
		if assert.NoError(t, err, "Should be able to list segments") {
			assert.Equal(t, expectedSegments, len(segments))
		}
	}
}
