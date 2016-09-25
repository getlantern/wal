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
	"github.com/golang/snappy"
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
	fb.file, err = os.OpenFile(fb.filename(), fb.fileFlags, 0600)
	return err
}

func (fb *filebased) filename() string {
	return filepath.Join(fb.dir, sequenceToFilename(fb.fileSequence))
}

// WAL provides a simple write-ahead log backed by a single file on disk. It is
// safe to write to a single WAL from multiple goroutines.
type WAL struct {
	filebased
	syncImmediate bool
	writer        *snappy.Writer
	mx            sync.Mutex
}

// Open opens a WAL in the given directory. It will be force synced to disk
// every syncInterval. If syncInterval is 0, it will force sync on every write
// to the WAL.
func Open(dir string, syncInterval time.Duration) (*WAL, error) {
	err := convertLegacyFormat(dir)
	if err != nil {
		return nil, err
	}

	err = appendSentinels(dir)
	if err != nil {
		return nil, err
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

func convertLegacyFormat(dir string) error {
	// Compress existing files with snappy if necessary
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("Unable to list existing log files: %v", err)
	}

	for _, fileInfo := range files {
		if fileInfo.Name()[0] != '0' {
			// Filename isn't left-padded, assuming old format
			log.Debugf("Converting legacy WAL file %v", fileInfo.Name())
			in, err := os.OpenFile(filepath.Join(dir, fileInfo.Name()), os.O_RDONLY, 0600)
			if err != nil {
				return fmt.Errorf("Unable to open legacy file for conversion: %v", err)
			}
			defer in.Close()
			bufIn := bufio.NewReader(in)
			out, err := os.OpenFile(filepath.Join(dir, sequenceToFilename(filenameToSequence(fileInfo.Name())/1000)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				return fmt.Errorf("Unable to open new file for legacy conversion: %v", err)
			}
			defer out.Close()
			snappyOut := snappy.NewBufferedWriter(out)
			_, err = io.Copy(snappyOut, bufIn)
			if err != nil {
				return fmt.Errorf("Unable to convert legacy file: %v", err)
			}
			err = snappyOut.Close()
			if err != nil {
				return fmt.Errorf("Unable to close snappy on conversion: %v", err)
			}
			err = out.Close()
			if err != nil {
				return fmt.Errorf("Unable to close converted file: %v", err)
			}
			err = os.Remove(in.Name())
			if err != nil {
				log.Errorf("Unable to remove old file '%v': %v", in.Name(), err)
			}
		}
	}
	return nil
}

func appendSentinels(dir string) error {
	// Append sentinel values to all existing files just in case
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("Unable to list existing log files: %v", err)
	}

	for _, fileInfo := range files {
		file, sentinelErr := os.OpenFile(filepath.Join(dir, fileInfo.Name()), os.O_APPEND|os.O_WRONLY, 0600)
		if sentinelErr != nil {
			return fmt.Errorf("Unable to append sentinel to existing file %v: %v", fileInfo.Name(), sentinelErr)
		}
		defer file.Close()
		writer := snappy.NewWriter(file)
		defer writer.Close()
		_, sentinelErr = writer.Write(sentinelBytes)
		if sentinelErr != nil {
			return fmt.Errorf("Unable to append sentinel to existing file %v: %v", fileInfo.Name(), sentinelErr)
		}
	}

	return nil
}

// Write atomically writes one or more buffers to the WAL.
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
	n, err := wal.writer.Write(lenBuf)
	wal.position += int64(n)
	if err != nil {
		return 0, err
	}

	for _, b := range bufs {
		n, err = wal.writer.Write(b)
		if err != nil {
			return 0, err
		}
		wal.position += int64(n)
	}

	if wal.syncImmediate {
		wal.doSync()
	}

	if wal.position >= maxSegmentSize {
		// Write sentinel length to mark end of file
		_, err = wal.writer.Write(sentinelBytes)
		if err != nil {
			return 0, err
		}
		err = wal.writer.Close()
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

// TruncateBefore removes all data prior to the given offset from disk.
func (wal *WAL) TruncateBefore(o Offset) error {
	files, err := ioutil.ReadDir(wal.dir)
	if err != nil {
		return fmt.Errorf("Unable to list log files to delete: %v", err)
	}

	cutoff := sequenceToFilename(o.FileSequence())
	for i, file := range files {
		if i == len(files)-1 || file.Name() >= cutoff {
			// Files are sorted by name, if we've gotten past the cutoff or
			// encountered the last (active) file, don't bother continuing.
			break
		}
		rmErr := os.Remove(filepath.Join(wal.dir, file.Name()))
		if rmErr != nil {
			return rmErr
		}
		log.Debugf("Removed WAL file %v", filepath.Join(wal.dir, file.Name()))
	}

	return nil
}

// TruncateBeforeTime truncates WAL data prior to the given timestamp.
func (wal *WAL) TruncateBeforeTime(ts time.Time) error {
	return wal.TruncateBefore(newOffset(tsToFileSequence(ts), 0))
}

// Close closes the wal, including flushing any unsaved writes.
func (wal *WAL) Close() error {
	snappyErr := wal.writer.Close()
	closeErr := wal.file.Close()
	if snappyErr != nil {
		return snappyErr
	}
	return closeErr
}

func (wal *WAL) advance() error {
	wal.fileSequence = newFileSequence()
	wal.position = 0
	err := wal.openFile()
	if err == nil {
		wal.writer = snappy.NewBufferedWriter(wal.file)
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
	err := wal.writer.Flush()
	if err != nil {
		log.Errorf("Unable to flush wal: %v", err)
		return
	}
	err = wal.file.Sync()
	if err != nil {
		log.Errorf("Unable to sync wal: %v", err)
	}
}

// Reader allows reading from a WAL. It is NOT safe to read from a single Reader
// from multiple goroutines.
type Reader struct {
	filebased
	reader *snappy.Reader
}

// NewReader constructs a new Reader for reading from this WAL starting at the
// given offset. The returned Reader is NOT safe for use from multiple
// goroutines.
func (wal *WAL) NewReader(offset Offset) (*Reader, error) {
	r := &Reader{filebased: filebased{dir: wal.dir, fileFlags: os.O_RDONLY}}
	if offset != nil {
		offsetString := sequenceToFilename(offset.FileSequence())
		if offsetString[0] != '0' {
			log.Debugf("Converting legacy offset")
			offset = newOffset(offset.FileSequence()/1000, offset.Position())
		}

		files, err := ioutil.ReadDir(wal.dir)
		if err != nil {
			return nil, fmt.Errorf("Unable to list existing log files: %v", err)
		}

		cutoff := sequenceToFilename(offset.FileSequence())
		for _, fileInfo := range files {
			if fileInfo.Name() >= cutoff {
				// Found exist or more recent WAL file
				r.fileSequence = filenameToSequence(fileInfo.Name())
				if r.fileSequence == offset.FileSequence() {
					// Exact match, start at right position
					r.position = offset.Position()
				} else {
					// Newer WAL file, start at beginning
					r.position = 0
				}
				openErr := r.open()
				if openErr != nil {
					return nil, fmt.Errorf("Unable to open existing log file at %v: %v", fileInfo.Name(), openErr)
				}
				break
			}
		}
	}

	if r.file == nil {
		// Didn't find WAL file, advance
		err := r.advance()
		if err != nil {
			return nil, fmt.Errorf("Unable to advance initially: %v", err)
		}
	}
	return r, nil
}

// Read reads the next chunk from the WAL, blocking until one is available.
func (r *Reader) Read() ([]byte, error) {
top:
	for {
		// Read length
		lenBuf := make([]byte, 4)
		read := 0
		length := 0
		for {
			read = 0

			for {
				n, err := r.reader.Read(lenBuf[read:])
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
			n, err := r.reader.Read(b[read:])
			if err == io.EOF && n == 0 {
				files, err := ioutil.ReadDir(r.dir)
				if err != nil {
					return nil, fmt.Errorf("Unable to list existing log files: %v", err)
				}
				cutoff := sequenceToFilename(r.fileSequence)
				for _, file := range files {
					if file.Name() > cutoff {
						log.Errorf("Out of data to read, and newer log files present, assuming WAL at %v corrupted. Advancing and continuing.", r.filename())
						err := r.advance()
						if err != nil {
							return nil, err
						}
						continue top
					}
				}
				// No newer log files, continue trying to read from this one
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
}

// Offset returns the furthest Offset read by this Reader. It is NOT safe to
// call this concurrently with Read().
func (r *Reader) Offset() Offset {
	return newOffset(r.fileSequence, r.position)
}

// Close closes the Reader.
func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) open() error {
	err := r.openFile()
	if err != nil {
		return err
	}
	bufReader := bufio.NewReader(r.file)
	r.reader = snappy.NewReader(bufReader)
	if r.position > 0 {
		// Read to the correct offset
		// Note - we cannot just seek on the file because the data is compressed and
		// the recorded position does not correspond to a file offset.
		_, seekErr := io.CopyN(ioutil.Discard, r.reader, r.position)
		if seekErr != nil {
			return seekErr
		}
	}
	return nil
}

func (r *Reader) advance() error {
	for {
		files, err := ioutil.ReadDir(r.dir)
		if err != nil {
			return fmt.Errorf("Unable to list existing log files: %v", err)
		}

		cutoff := sequenceToFilename(r.fileSequence)
		for _, fileInfo := range files {
			if fileInfo.Name() > cutoff {
				// Files are sorted by name, if we've gotten past the cutoff, don't bother
				// continuing
				r.position = 0
				r.fileSequence = filenameToSequence(fileInfo.Name())
				return r.open()
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func newFileSequence() int64 {
	return tsToFileSequence(time.Now())
}

func tsToFileSequence(ts time.Time) int64 {
	return ts.UnixNano() / 1000
}

func sequenceToFilename(seq int64) string {
	return fmt.Sprintf("%019d", seq)
}

func filenameToSequence(filename string) int64 {
	_, filePart := filepath.Split(filename)
	seq, err := strconv.ParseInt(filePart, 10, 64)
	if err != nil {
		log.Errorf("Unparseable filename '%v': %v", filename, err)
		return 0
	}
	return seq
}
