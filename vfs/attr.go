package vfs

import (
	"github.com/beyondstorage/go-storage/v4/types"
	"time"
)

type Attr struct {
	o *types.Object
}

func NewAttr(o *types.Object) *Attr {
	return &Attr{o: o}
}

func (a *Attr) Path() string {
	return a.o.Path
}

func (a *Attr) Mode() types.ObjectMode {
	return a.o.Mode
}

func (a *Attr) Size() int64 {
	v, ok := a.o.GetContentLength()
	if !ok {
		return 0
	}
	return v
}

func (a *Attr) Mtime() time.Time {
	t, ok := a.o.GetLastModified()
	if !ok {
		return time.Unix(0, 0)
	}
	return t
}

func (a *Attr) Expire() {
	a.o.Expire()
}
