package vfs

import (
	"errors"
	"github.com/beyondstorage/go-storage/v4/types"
)

type Dir struct {
	s  types.Storager
	o  *types.Object
	it *types.ObjectIterator
}

func (d *Dir) Next() (attr *Attr, hasNext bool, err error) {
	o, err := d.it.Next()
	if err != nil && errors.Is(err, types.IterateDone) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return NewAttr(o), true, nil
}
