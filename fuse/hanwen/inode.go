package hanwen

import (
	"path"
	"sync"

	"go.uber.org/atomic"

	"github.com/beyondstorage/go-fs/vfs"
)

type Inode struct {
	attr *vfs.Attr

	id uint64
}

func (i *Inode) Expire() {
	i.attr.Expire()
}

func (i *Inode) Name() (name string) {
	return path.Base(i.attr.Path())
}

func (i *Inode) Path() (path string) {
	return i.attr.Path()
}

func (i *Inode) FormatPath(name string) (path string) {
	if i.Path() == "" {
		return name
	}
	return i.Path() + "/" + name
}

type InodeMap struct {
	m    map[uint64]*Inode
	l    sync.Mutex
	free *atomic.Uint64
}

func NewInodeMap() *InodeMap {
	return &InodeMap{
		m:    make(map[uint64]*Inode),
		free: atomic.NewUint64(2),
	}
}

func (m *InodeMap) Init(attr *vfs.Attr) {
	i := &Inode{
		attr: attr,
		id:   1,
	}

	m.m[i.id] = i
}

func (m *InodeMap) New(attr *vfs.Attr) (i *Inode) {
	i = &Inode{attr: attr}
	_ = m.Set(i)
	return i
}
func (m *InodeMap) Set(i *Inode) (ino uint64) {
	m.l.Lock()
	defer m.l.Unlock()

	i.id = m.free.Inc()
	m.m[i.id] = i
	return i.id
}
func (m *InodeMap) Get(ino uint64) (i *Inode, ok bool) {
	m.l.Lock()
	defer m.l.Unlock()

	i, ok = m.m[ino]
	return
}

func (m *InodeMap) Del(ino uint64) (i *Inode, deleted bool) {
	m.l.Lock()
	defer m.l.Unlock()

	i, deleted = m.m[ino]
	if deleted {
		delete(m.m, ino)
	}
	return
}
