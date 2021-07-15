package vfs

import (
	"github.com/beyondstorage/go-storage/v4/types"
	"os"
	"path"
	"time"
)

//go:generate go run github.com/tinylib/msgp

type Inode struct {
	ID       uint64
	ParentID uint64

	Path       string
	Name       string
	Generation uint64
	Size       uint64
	Mode       uint32 // The same with os.FileMode
	Atime      time.Time
	Mtime      time.Time
	Ctime      time.Time
}

func (ino *Inode) IsDir() bool {
	return ino.Mode&uint32(os.ModeDir) != 0
}

func newInode(parent uint64, o *types.Object) *Inode {
	ino := &Inode{
		ID:       NextInodeID(),
		ParentID: parent,

		Path:       o.Path,
		Name:       path.Base(o.Path),
		Generation: 1,
		Mode:       formatMode(o.Mode),
	}

	if v, ok := o.GetContentLength(); ok {
		ino.Size = uint64(v)
	}
	// TODO: we will support other time later
	if v, ok := o.GetLastModified(); ok {
		ino.Atime = v
		ino.Mtime = v
		ino.Ctime = v
	}
	return ino
}

func formatMode(o types.ObjectMode) uint32 {
	var mode uint32
	if o.IsDir() {
		mode = uint32(os.ModeDir) | 0755
	} else {
		mode = 0644
	}
	return mode
}
