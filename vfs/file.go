package vfs

import (
	"fmt"
	"go.uber.org/zap"
	"sync"

	"github.com/Xuanwo/go-bufferpool"
	"github.com/beyondstorage/go-storage/v4/pairs"

	"github.com/beyondstorage/beyond-fs/meta"
)

var (
	fileBufPool = bufferpool.New(4 * 1024 * 1024)
)

type fileHandleMap struct {
	lock sync.Mutex
	m    map[uint64]*FileHandle
}

func newFileHandleMap() *fileHandleMap {
	return &fileHandleMap{
		m: make(map[uint64]*FileHandle),
	}
}

func (fhm *fileHandleMap) Get(id uint64) *FileHandle {
	fhm.lock.Lock()
	defer fhm.lock.Unlock()

	return fhm.m[id]
}

func (fhm *fileHandleMap) Set(id uint64, dh *FileHandle) {
	fhm.lock.Lock()
	defer fhm.lock.Unlock()

	fhm.m[id] = dh
}

func (fhm *fileHandleMap) Delete(id uint64) {
	fhm.lock.Lock()
	defer fhm.lock.Unlock()

	delete(fhm.m, id)
}

type FileHandle struct {
	ID uint64

	ino   *Inode
	fs    *FS
	meta  meta.Service
	cache *Cache

	mu     sync.Mutex
	size   uint64
	offset uint64

	// Read operations
	buf *bufferpool.Buffer

	// Write operations
	idx uint64
}

func (fh *FileHandle) GetInode() *Inode {
	return fh.ino
}

func (fh *FileHandle) Read(offset uint64, buf []byte) (n int, err error) {
	size := fh.size
	if size > uint64(len(buf)) {
		size = uint64(len(buf))
	}

	fh.mu.Lock()
	defer fh.mu.Unlock()

	fh.buf.Reset()

	fh.fs.logger.Info("read data",
		zap.String("path", fh.ino.Path),
		zap.Uint64("offset", offset),
		zap.Uint64("size", size))
	byteRead, err := fh.fs.s.Read(fh.ino.Path, fh.buf,
		pairs.WithOffset(int64(offset)),
		pairs.WithSize(int64(size)))
	if err != nil {
		fh.fs.logger.Error("read underlying", zap.Error(err))
		return
	}

	copy(buf, fh.buf.Bytes()[:byteRead])
	fh.offset += uint64(byteRead)
	return int(byteRead), nil
}

func (fh *FileHandle) PrepareForWrite() (err error) {
	err = fh.cache.startWrite(fh.ID, fh.ino.Path)
	if err != nil {
		return
	}
	return
}

func (fh *FileHandle) Write(offset uint64, buf []byte) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if offset != fh.offset {
		return 0, fmt.Errorf("random write is not allowd")
	}

	fh.fs.logger.Info("write data",
		zap.String("path", fh.ino.Path),
		zap.Uint64("offset", offset),
		zap.Int("size", len(buf)))
	byteWritten, err := fh.cache.write(fh.ID, fh.idx, buf)
	if err != nil {
		fh.fs.logger.Error("write buffer", zap.Error(err))
		return
	}

	fh.idx += 1
	fh.size += uint64(byteWritten)
	fh.offset += uint64(byteWritten)

	return int(byteWritten), nil
}

func (fh *FileHandle) CloseForWrite() (err error) {
	err = fh.cache.endWrite(fh.ID)
	if err != nil {
		return
	}
	return
}
