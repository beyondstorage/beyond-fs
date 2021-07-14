package vfs

import (
	"time"
)

//go:generate go run github.com/tinylib/msgp

type Inode struct {
	ID         uint64
	Generation uint64
	Ino        uint64
	Size       uint64
	Mode       uint32
	Atime      time.Time
	Mtime      time.Time
	Ctime      time.Time
}

func (ino *Inode) IsDir() bool {
	panic("implement me")
}
