package main

import (
	"log"
	"os"
	"strconv"
)

type RioFile struct {
	//number of bytes read or written
	processedBytes int
	//maximum single read or write chunk size *
	MaxProcessingChunk int
	file               struct {
		fp *os.File
		//bytes written since last fsync.
		buffered int
		//fsync after 'autoSync' bytes written, if value is 0 means don't auto sync
		autoSync int
	}
}

func NewRioWithFile(f *os.File) *RioFile {
	return &RioFile{
		processedBytes:     0,
		MaxProcessingChunk: 1024,
		file: struct {
			fp       *os.File
			buffered int
			autoSync int
		}{
			fp:       f,
			buffered: 0,
			autoSync: AOF_AUTOSYNC_BYTES,
		},
	}
}

// Make sure 'len' bytes are written
func (r *RioFile) Write(buf []byte, len int) error {
	for len > 0 {
		bytesToWrite := len
		// limit the number of bytes written at a time
		if r.MaxProcessingChunk != 0 && r.MaxProcessingChunk < len {
			bytesToWrite = r.MaxProcessingChunk
		}
		err := fileWrite(r, buf, bytesToWrite)
		if err != nil {
			log.Printf("write file error: %v \n", err)
			return err
		}
		buf = buf[len:]
		len -= bytesToWrite
		r.processedBytes += bytesToWrite
	}
	return nil
}

// WriteBulkCount write multi bulk count in the format: "*<count>\r\n".
func (r *RioFile) WriteBulkCount(prefix string, cnt int) error {
	cbuf := []byte(prefix + strconv.Itoa(cnt) + "\r\n")
	return r.Write(cbuf, len(cbuf))
}

// WriteBulkString write binary-safe string in the format: "$<count>\r\n<payload>\r\n".
func (r *RioFile) WriteBulkString(s string) error {
	if err := r.WriteBulkCount("$", len(s)); err != nil {
		return err
	}
	sbuf := []byte(s + "\r\n")
	return r.Write(sbuf, len(sbuf))
}

// WriteBulkInt64 write an int64 value in format: "$<count>\r\n<payload>\r\n".
func (r *RioFile) WriteBulkInt64(n int64) error {
	s := strconv.FormatInt(n, 10)
	return r.WriteBulkString(s)
}

func (r *RioFile) Read(buf []byte, len int) error {
	for len > 0 {
		bytesToRead := len
		if r.MaxProcessingChunk > 0 && r.MaxProcessingChunk < len {
			bytesToRead = r.MaxProcessingChunk
		}
		if err := fileRead(r, buf, len); err != nil {
			return err
		}
		buf = buf[bytesToRead:]
		len -= bytesToRead
		r.processedBytes += bytesToRead
	}
	return nil
}

func fileWrite(r *RioFile, buf []byte, len int) error {
	_, err := r.file.fp.Write(buf[:len])
	if err != nil {
		log.Printf("write file error: %v \n", err)
		return err
	}
	r.file.buffered += len
	//sync data to disk if trigger auto sync
	if r.file.autoSync > 0 && r.file.buffered >= r.file.autoSync {
		if err = r.file.fp.Sync(); err != nil {
			log.Printf("sync disk error: %v \n", err)
			return err
		}
		r.file.buffered = 0
	}
	return nil
}

func fileRead(r *RioFile, buf []byte, len int) error {
	_, err := r.file.fp.Read(buf[:len])
	if err != nil {
		log.Printf("read file error: %v \n", err)
		return err
	}
	return nil
}
