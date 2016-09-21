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

var (
	log = golog.LoggerFor("wal")

	maxSegmentSize = int64(10485760)
	encoding       = binary.BigEndian
)

type filebased struct {
	dir          string
	file         *os.File
	fileSequence int64
	position     int64
	open         func(filename string, position int64) (*os.File, error)
}

func (fb *filebased) openFile() error {
	var err error
	if fb.file != nil {
		err = fb.file.Close()
		if err != nil {
			log.Errorf("Unable to close existing file %v: %v", fb.file.Name(), err)
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
	mx            sync.Mutex
}

func Open(dir string, syncInterval time.Duration) (*WAL, error) {
	wal := &WAL{filebased: filebased{dir: dir, open: func(filename string, position int64) (*os.File, error) {
		return os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	}}}
	// Append to latest file if available
	files, err := ioutil.ReadDir(wal.dir)
	if err != nil {
		return nil, fmt.Errorf("Unable to list existing log files: %v", err)
	}

	if len(files) > 0 {
		// Files are sorted by name, so the last in the list is the latest
		latestFile := files[len(files)-1]
		if latestFile.Size() < maxSegmentSize {
			wal.position = latestFile.Size()
			wal.fileSequence, err = strconv.ParseInt(latestFile.Name(), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Unable to parse file sequence from filename %v: %v", latestFile.Name(), err)
			}
			err := wal.openFile()
			if err != nil {
				return nil, err
			}
			wal.bufWriter = bufio.NewWriter(wal.file)
		}
	}

	if wal.file == nil {
		err := wal.advance()
		if err != nil {
			return nil, err
		}
	}

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
	if wal.position >= maxSegmentSize {
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
	r := &Reader{filebased: filebased{dir: wal.dir, open: func(filename string, position int64) (*os.File, error) {
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

	err = r.advance()
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) Read() ([]byte, error) {
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

	if r.position >= maxSegmentSize {
		err := r.advance()
		if err != nil {
			return nil, err
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

func (r *Reader) advance() error {
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
	}

	return fmt.Errorf("No file found past position %d", r.position)
}
