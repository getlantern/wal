package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/getlantern/golog"
)

const (
	maxSegmentSize = 10485760
)

var (
	log = golog.LoggerFor("wal")

	encoding = binary.BigEndian
)

type filebased struct {
	dir          string
	file         *os.File
	fileSequence int64
	position     int64
	open         func(filename string, position int64) (*os.File, error)
}

func (fb *filebased) advance(offset Offset) (err error) {
	if offset != nil {
		fb.fileSequence = offset.FileSequence()
		fb.position = offset.Position()
	} else {
		fb.fileSequence = time.Now().UnixNano()
		fb.position = 0
	}
	if fb.file != nil {
		err = fb.file.Close()
		if err != nil {
			return err
		}
	}
	fb.file, err = fb.open(filepath.Join(fb.dir, fmt.Sprintf("%d", fb.fileSequence)), fb.position)
	return err
}

// WAL provides a simple write-ahead log backed by a single file on disk.
type WAL struct {
	filebased
	syncImmediate bool
	bufWriter     *bufio.Writer
	mx            sync.RWMutex
}

func Open(dir string, offset Offset, syncInterval time.Duration) (*WAL, error) {
	wal := &WAL{filebased: filebased{dir: dir, open: func(filename string, position int64) (*os.File, error) {
		return os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	}}}
	err := wal.advance(offset)
	if err != nil {
		return nil, err
	}
	wal.bufWriter = bufio.NewWriter(wal.file)
	if syncInterval <= 0 {
		wal.syncImmediate = true
	} else {
		go wal.sync(syncInterval)
	}
	return wal, nil
}

func (wal *WAL) Write(b []byte) (int, error) {
	wal.mx.Lock()
	defer wal.mx.Unlock()
	lenBuf := make([]byte, 4)
	encoding.PutUint32(lenBuf, uint32(len(b)))
	n, err := wal.bufWriter.Write(lenBuf)
	wal.position += int64(n)
	if err != nil {
		return 0, err
	}

	n, err = wal.bufWriter.Write(b)
	if err != nil {
		return 0, err
	}

	if wal.syncImmediate {
		wal.doSync()
	}

	wal.position += int64(n)
	if wal.position > maxSegmentSize {
		err = wal.advance(nil)
		if err != nil {
			return n, fmt.Errorf("Unable to advance to next file: %v", err)
		}
		wal.bufWriter = bufio.NewWriter(wal.file)
	}

	return n, nil
}

func (wal *WAL) Offset() Offset {
	wal.mx.RLock()
	o := newOffset(wal.fileSequence, wal.position)
	wal.mx.RUnlock()
	return o
}

func (wal *WAL) TruncateBefore(o Offset) error {
	files, err := ioutil.ReadDir(wal.dir)
	if err != nil {
		return fmt.Errorf("Unable to list log files to delete: %v", err)
	}

	cutoff := fmt.Sprintf("%d", o.FileSequence())
	for _, file := range files {
		if file.Name() >= cutoff {
			// Files are sorted by name, if we've gotten past the cutoff, don't bother
			// continuing
			break
		}
		rmErr := os.Remove(filepath.Join(wal.dir, file.Name()))
		if rmErr != nil {
			return rmErr
		}
	}

	return nil
}

func (wal *WAL) Close() error {
	flushErr := wal.bufWriter.Flush()
	closeErr := wal.file.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

func (wal *WAL) sync(syncInterval time.Duration) {
	for {
		time.Sleep(syncInterval)
		wal.mx.Lock()
		wal.doSync()
		wal.mx.Unlock()
	}
}

func (wal *WAL) doSync() {
	err := wal.bufWriter.Flush()
	if err != nil {
		log.Errorf("Unable to flush wal: %v", err)
	} else {
		err = wal.file.Sync()
		if err != nil {
			log.Errorf("Unable to sync wal: %v", err)
		}
	}
}

type Reader struct {
	filebased
	wal       *WAL
	bufReader *bufio.Reader
	mx        sync.Mutex
}

func (wal *WAL) NewReader(offset Offset) (*Reader, error) {
	r := &Reader{wal: wal, filebased: filebased{dir: wal.dir, open: func(filename string, position int64) (*os.File, error) {
		file, err := os.OpenFile(filename, os.O_RDONLY, 0600)
		if err == nil {
			_, err = file.Seek(position, 0)
		}
		return file, err
	}}}

	files, err := ioutil.ReadDir(wal.dir)
	if err != nil {
		return nil, fmt.Errorf("Unable to list existing log files: %v", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("No log files, can't open reader")
	}

	if offset == nil {
		fileSequence, parseErr := strconv.ParseInt(files[0].Name(), 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("Unable to parse log file name %v: %v", files[0].Name(), parseErr)
		}
		offset = newOffset(fileSequence, 0)
	}

	err = r.advance(offset)
	if err != nil {
		return nil, err
	}
	r.bufReader = bufio.NewReader(r.file)
	return r, nil
}

func (r *Reader) Read() ([]byte, error) {
	r.wal.mx.RLock()
	defer r.wal.mx.RUnlock()
	r.mx.Lock()
	defer r.mx.Unlock()

	// Read length
	lenBuf := make([]byte, 4)
	read := 0
	length := 0
	for {
		n, err := r.bufReader.Read(lenBuf[read:])
		if err == io.EOF && n == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		read += n
		r.position += int64(n)
		if read == 4 {
			length = int(encoding.Uint32(lenBuf))
			break
		}
	}

	if length == 0 {
		return nil, nil
	}

	// Read data
	b := make([]byte, length)
	read = 0
	for {
		n, err := r.bufReader.Read(b)
		if err == io.EOF && n == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		read += n
		r.position += int64(n)
		if read == length {
			break
		}
	}

	if r.position > maxSegmentSize {
		err := r.advance(nil)
		if err != nil {
			return nil, err
		}
		r.bufReader = bufio.NewReader(r.file)
	}

	return b, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}
