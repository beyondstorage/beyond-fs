package vfs

import (
	"errors"
	"github.com/beyondstorage/beyond-fs/meta"
	"github.com/beyondstorage/go-storage/v4/types"
	"sync"
	"time"
)

type dirHandleMap struct {
	lock sync.Mutex
	m    map[uint64]*DirHandle
}

func newDirHandleMap() *dirHandleMap {
	return &dirHandleMap{
		m: make(map[uint64]*DirHandle),
	}
}

func (dhm *dirHandleMap) Get(id uint64) *DirHandle {
	dhm.lock.Lock()
	defer dhm.lock.Unlock()

	return dhm.m[id]
}

func (dhm *dirHandleMap) Set(id uint64, dh *DirHandle) {
	dhm.lock.Lock()
	defer dhm.lock.Unlock()

	dhm.m[id] = dh
}

func (dhm *dirHandleMap) Delete(id uint64) {
	dhm.lock.Lock()
	defer dhm.lock.Unlock()

	delete(dhm.m, id)
}

type DirHandle struct {
	ID uint64

	ino  *Inode
	fs   *FS
	it   *types.ObjectIterator
	meta meta.Service
}

func (dh *DirHandle) Next() (ino *Inode, err error) {
	o, err := dh.it.Next()
	if err != nil && errors.Is(err, types.IterateDone) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	ino = newInode(dh.ino.ID, o)
	err = dh.fs.SetInode(ino, time.Minute)
	if err != nil {
		return
	}
	return
}
