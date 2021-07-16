package vfs

import (
	"sync"

	"github.com/beyondstorage/beyond-fs/meta"
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

	ino  *Inode
	fs   *FS
	meta meta.Service
}
