package vfs

import (
	"os"
	"time"
)

//go:generate go run github.com/tinylib/msgp

type Inode struct {
	ID         uint64
	Generation uint64
	Ino        uint64
	Size       uint64
	Mode       uint32 // The same with os.FileMode
	Atime      time.Time
	Mtime      time.Time
	Ctime      time.Time
}

func (ino *Inode) IsDir() bool {
	return ino.Mode&uint32(os.ModeDir) == 0
}
