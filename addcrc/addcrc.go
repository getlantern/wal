package main

import (
	"bufio"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/getlantern/golog"
	"github.com/golang/snappy"
)

const (
	compressedSuffix = ".snappy"
)

var (
	log = golog.LoggerFor("addcrc")

	encoding = binary.BigEndian
)

func main() {
	inFile, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("Unable to open input file: %v", err)
	}
	defer inFile.Close()

	outFile, err := ioutil.TempFile("", "addcrc")
	if err != nil {
		log.Fatalf("Unable to open output file: %v", err)
	}
	defer outFile.Close()

	var flush func() error
	var in io.Reader
	var out io.Writer
	if strings.HasSuffix(inFile.Name(), compressedSuffix) {
		in = snappy.NewReader(inFile)
		sout := snappy.NewBufferedWriter(outFile)
		out = sout
		flush = sout.Flush
	} else {
		in = bufio.NewReaderSize(inFile, 2<<16)
		bout := bufio.NewWriterSize(outFile, 2<<16)
		out = bout
		flush = bout.Flush
	}

	h := newHash()
	headerBuf := make([]byte, 4)
	for {
		_, err := io.ReadFull(in, headerBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Unable to read: %v", err)
		}

		length := int(encoding.Uint32(headerBuf))
		if length == 0 {
			// At end, stop
			break
		}

		data := make([]byte, length)
		_, err = io.ReadFull(in, data)
		if err != nil && err != io.EOF {
			log.Error("Unable to read, assuming file corrupted")
			break
		}

		h.Reset()
		h.Write(data)

		_, err = out.Write(headerBuf)
		if err != nil {
			log.Fatalf("Unable to write length: %v", err)
		}
		encoding.PutUint32(headerBuf, h.Sum32())
		_, err = out.Write(headerBuf)
		if err != nil {
			log.Fatalf("Unable to write crc: %v", err)
		}
		_, err = out.Write(data)
		if err != nil {
			log.Fatalf("Unable to write data: %v", err)
		}
	}

	err = flush()
	if err != nil {
		log.Fatalf("Unable to flush: %v", err)
	}
	err = outFile.Close()
	if err != nil {
		log.Fatalf("Unable to close outfile: %v", err)
	}
	err = inFile.Close()
	if err != nil {
		log.Fatalf("Unable to close infile: %v", err)
	}
	err = os.Rename(inFile.Name(), inFile.Name()+"_nocrc")
	if err != nil {
		log.Fatalf("Unable to rename infile: %v", err)
	}
	err = os.Rename(outFile.Name(), inFile.Name())
	if err != nil {
		log.Fatalf("Unable to rename outfile: %v", err)
	}

	log.Debugf("Finished adding CRCs to %v", os.Args[1])
}

func newHash() hash.Hash32 {
	return crc32.New(crc32.MakeTable(crc32.Castagnoli))
}
