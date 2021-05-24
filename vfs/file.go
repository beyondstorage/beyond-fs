package vfs

import (
	"bytes"
	"io"
	"sync"

	"github.com/Xuanwo/go-bufferpool"
	"github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/types"
)

var (
	fileBufPool = bufferpool.New(4 * 1024 * 1024)
)

type File struct {
	s types.Storager
	o *types.Object

	buf *bufferpool.Buffer
	off int64
	mu  sync.Mutex

	writeBytes int
	writeError error
}

func NewFile(s types.Storager, o *types.Object) *File {
	f := &File{
		s:   s,
		o:   o,
		buf: fileBufPool.Get(),
	}
	return f
}

func (f *File) Offset() (off int64) {
	return f.off
}

func (f *File) Seek(whence int, offset int64) (off int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch whence {
	case io.SeekStart:
		f.off = offset
	case io.SeekCurrent:
		f.off += offset
	case io.SeekEnd:
		// If content-length exist, we use content-length
		// If not, we use 0 instead.
		size, _ := f.o.GetContentLength()
		f.off = size - offset
	default:
		panic("invalid whence")
	}
	return f.off, nil
}

func (f *File) Read(offset int64, buf []byte) (n int, err error) {
	size := len(buf)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.buf.Reset()

	// TODO: We could reuse the buffer.
	bytesRead, err := f.s.Read(f.o.Path, f.buf,
		pairs.WithOffset(offset),
		pairs.WithSize(int64(size)))
	if err != nil {
		return int(bytesRead), err
	}

	n = copy(buf, f.buf.Bytes()[:bytesRead])
	f.off += int64(n)
	return
}

func (f *File) Write(offset int64, buf []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err = f.buf.Write(buf)
	if err != nil {
		return
	}
	f.off += int64(n)
	return
}

func (f *File) Flush() {
	f.mu.Lock()
	defer f.mu.Unlock()

	buf := f.buf.Bytes()
	n, err := f.s.Write(f.o.Path, bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		f.writeError = err
		panic(err)
	}
	f.writeBytes = int(n)
	return
}

func (f *File) Sync() {}

func (f *File) Close() (err error) {
	f.buf.Free()
	f.buf = nil
	f.s = nil
	f.o = nil
	return
}
