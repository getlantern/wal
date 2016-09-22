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
	sentinel = 0
)

var (
	log = golog.LoggerFor("wal")

	maxSegmentSize = int64(10485760)
	encoding       = binary.BigEndian
	sentinelBytes  = make([]byte, 4) // same as 0
)

type filebased struct {
	dir          string
	file         *os.File
	fileSequence int64
	position     int64
	fileFlags    int
}

func (fb *filebased) openFile() error {
	var err error
	if fb.file != nil {
		err = fb.file.Close()
		if err != nil {
			log.Errorf("Unable to close existing file %v: %v", fb.file.Name(), err)
		}
	}
	fb.file, err = os.OpenFile(filepath.Join(fb.dir, fmt.Sprintf("%d", fb.fileSequence)), fb.fileFlags, 0600)
	return err
}

// WAL provides a simple write-ahead log backed by a single file on disk.
type WAL struct {
	filebased
	syncImmediate bool
	bufWriter     *bufio.Writer
	mx            sync.Mutex
}

func Open(dir string, syncInterval time.Duration) (*WAL, error) {
	// Append sentinel values to all existing files just in case
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("Unable to list existing log files: %v", err)
	}
	for _, fileInfo := range files {
		file, err := os.OpenFile(filepath.Join(dir, fileInfo.Name()), os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, fmt.Errorf("Unable to append sentinel to existing file %v: %v", fileInfo.Name(), err)
		}
		defer file.Close()
		_, err = file.Write(sentinelBytes)
		if err != nil {
			return nil, fmt.Errorf("Unable to append sentinel to existing file %v: %v", fileInfo.Name(), err)
		}
	}

	wal := &WAL{filebased: filebased{dir: dir, fileFlags: os.O_CREATE | os.O_APPEND | os.O_WRONLY}}
	err = wal.advance()
	if err != nil {
		return nil, err
	}

	if syncInterval <= 0 {
		wal.syncImmediate = true
	} else {
		go wal.sync(syncInterval)
	}

	return wal, nil
}

func (wal *WAL) Write(bufs ...[]byte) (int, error) {
	wal.mx.Lock()
	defer wal.mx.Unlock()

	length := 0
	for _, b := range bufs {
		length += len(b)
	}
	if length == 0 {
		return 0, nil
	}

	lenBuf := make([]byte, 4)
	encoding.PutUint32(lenBuf, uint32(length))
	n, err := wal.bufWriter.Write(lenBuf)
	wal.position += int64(n)
	if err != nil {
		return 0, err
	}

	for _, b := range bufs {
		n, err = wal.bufWriter.Write(b)
		if err != nil {
			return 0, err
		}
	}

	if wal.syncImmediate {
		wal.doSync()
	}

	wal.position += int64(n)
	if wal.position >= maxSegmentSize {
		// Write sentinel length to mark end of file
		_, err = wal.bufWriter.Write(sentinelBytes)
		if err != nil {
			return 0, err
		}
		err = wal.bufWriter.Flush()
		if err != nil {
			return 0, err
		}
		err = wal.advance()
		if err != nil {
			return n, fmt.Errorf("Unable to advance to next file: %v", err)
		}
	}

	return n, nil
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

func (wal *WAL) advance() error {
	wal.fileSequence = time.Now().UnixNano()
	wal.position = 0
	err := wal.openFile()
	if err == nil {
		wal.bufWriter = bufio.NewWriter(wal.file)
	}
	return err
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
	bufReader *bufio.Reader
}

func (wal *WAL) NewReader(offset Offset) (*Reader, error) {
	r := &Reader{filebased: filebased{dir: wal.dir, fileFlags: os.O_RDONLY}}
	files, err := ioutil.ReadDir(wal.dir)
	if err != nil {
		return nil, fmt.Errorf("Unable to list existing log files: %v", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("No log files, can't open reader")
	}

	if offset != nil {
		r.fileSequence = offset.FileSequence()
		r.position = offset.Position()
		openErr := r.open()
		if openErr != nil {
			return nil, openErr
		}
	} else {
		err = r.advance()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Reader) Read() ([]byte, error) {
	// Read length
	lenBuf := make([]byte, 4)
	read := 0
	length := 0
	for {
		read = 0

		for {
			n, err := r.bufReader.Read(lenBuf[read:])
			if err == io.EOF && n == 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			read += n
			r.position += int64(n)
			if read == 4 {
				length = int(encoding.Uint32(lenBuf))
				break
			}
		}

		if length > sentinel {
			break
		}

		err := r.advance()
		if err != nil {
			return nil, err
		}
	}

	// Read data
	b := make([]byte, length)
	read = 0
	for {
		n, err := r.bufReader.Read(b[read:])
		if err == io.EOF && n == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		read += n
		r.position += int64(n)
		if read == length {
			break
		}
	}

	return b, nil
}

func (r *Reader) Offset() Offset {
	return newOffset(r.fileSequence, r.position)
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) open() error {
	err := r.openFile()
	if err != nil {
		return err
	}
	_, err = r.file.Seek(r.position, 0)
	if err != nil {
		return err
	}
	r.bufReader = bufio.NewReader(r.file)
	return nil
}

func (r *Reader) advance() error {
	for {
		files, err := ioutil.ReadDir(r.dir)
		if err != nil {
			return fmt.Errorf("Unable to list existing log files: %v", err)
		}

		cutoff := fmt.Sprintf("%d", r.fileSequence)
		for _, fileInfo := range files {
			if fileInfo.Name() > cutoff {
				// Files are sorted by name, if we've gotten past the cutoff, don't bother
				// continuing
				r.position = 0
				r.fileSequence, err = strconv.ParseInt(fileInfo.Name(), 10, 64)
				if err != nil {
					return fmt.Errorf("Unable to parse file sequence from filename %v: %v", fileInfo.Name(), err)
				}
				return r.open()
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}
