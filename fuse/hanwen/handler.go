package hanwen

import (
	"go.uber.org/atomic"
	"sync"

	"github.com/beyondstorage/go-fs/vfs"
)

type Handler struct {
	f *vfs.File
	d *vfs.Dir

	ino *Inode

	id uint64
}

type HandlerMap struct {
	m    map[uint64]*Handler
	l    sync.Mutex
	free *atomic.Uint64
}

func NewHandlerMap() *HandlerMap {
	return &HandlerMap{
		m:    make(map[uint64]*Handler),
		free: atomic.NewUint64(2),
	}
}

func (m *HandlerMap) NewFile(file *vfs.File, ino *Inode) (f *Handler) {
	f = &Handler{f: file, ino: ino}
	_ = m.Set(f)
	return f
}

func (m *HandlerMap) NewDir(dir *vfs.Dir, ino *Inode) (f *Handler) {
	f = &Handler{d: dir, ino: ino}
	_ = m.Set(f)
	return f
}

func (m *HandlerMap) Set(f *Handler) (fh uint64) {
	m.l.Lock()
	defer m.l.Unlock()

	f.id = m.free.Inc()
	m.m[f.id] = f
	return f.id
}
func (m *HandlerMap) Get(fh uint64) (f *Handler, ok bool) {
	m.l.Lock()
	defer m.l.Unlock()

	f, ok = m.m[fh]
	return
}
func (m *HandlerMap) Del(fh uint64) (f *Handler, deleted bool) {
	m.l.Lock()
	defer m.l.Unlock()

	f, deleted = m.m[fh]
	if deleted {
		delete(m.m, fh)
	}
	return
}
