package vfs

import (
	"bytes"
	"fmt"
	"time"

	_ "github.com/beyondstorage/go-service-fs/v3"
	_ "github.com/beyondstorage/go-service-s3/v2"
	"github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/beyondstorage/beyond-fs/meta"
)

var (
	nextInode  = atomic.NewUint64(0)
	nextHandle = atomic.NewUint64(0)
)

func NextInodeID() uint64 {
	return nextInode.Inc()
}

func NextHandle() uint64 {
	return nextHandle.Inc()
}

type FS struct {
	s    types.Storager
	meta meta.Service

	dhm    *dirHandleMap
	fhm    *fileHandleMap
	logger *zap.Logger
}

type Config struct {
	StoragePath string

	Logger *zap.Logger
}

func NewFS(cfg *Config) (fs *FS, err error) {
	store, err := services.NewStoragerFromString(cfg.StoragePath)
	if err != nil {
		return nil, err
	}

	metaSrv, err := meta.NewBadger()
	if err != nil {
		return nil, err
	}

	fs = &FS{
		s:    store,
		meta: metaSrv,

		dhm:    newDirHandleMap(),
		fhm:    newFileHandleMap(),
		logger: cfg.Logger,
	}

	o := types.NewObject(nil, true)
	o.ID = store.Metadata().WorkDir
	o.Path = ""
	o.Mode = types.ModeDir
	err = fs.SetInode(newInode(1, o))
	if err != nil {
		return nil, err
	}
	return fs, err
}

func (fs *FS) Create(parent uint64, name string) (ino *Inode, fh *FileHandle, err error) {
	// FIXME: we need to handle file exists.
	p, err := fs.GetInode(parent)
	if err != nil {
		return nil, nil, err
	}

	path := p.GetEntryPath(name)
	_, err = fs.s.Write(path, bytes.NewReader([]byte{}), 0)
	if err != nil {
		fs.logger.Error("write", zap.String("path", path), zap.Error(err))
		return nil, nil, err
	}

	o := fs.s.Create(path)
	o.Path = path
	o.Mode = types.ModeRead
	o.SetContentLength(0)
	o.SetLastModified(time.Now())

	ino = newInode(parent, o)
	err = fs.SetInode(ino)
	if err != nil {
		return
	}

	fh, err = fs.CreateFileHandle(ino)
	if err != nil {
		return
	}
	return
}

func (fs *FS) Delete(parent uint64, name string) (err error) {
	ino, err := fs.GetEntry(parent, name)
	if err != nil {
		return
	}
	err = fs.s.Delete(ino.Path)
	if err != nil {
		return
	}
	err = fs.DeleteInode(ino)
	if err != nil {
		return
	}
	err = fs.DeleteEntry(parent, name)
	if err != nil {
		return
	}
	return
}

func (fs *FS) DeleteDir(path string) (err error) {
	panic("implement me")
}

func (fs *FS) CreateFileHandle(ino *Inode) (fh *FileHandle, err error) {
	fh = &FileHandle{
		ID:   NextHandle(),
		ino:  ino,
		fs:   fs,
		meta: fs.meta,
	}
	fs.fhm.Set(fh.ID, fh)
	return fh, nil
}

func (fs *FS) GetFileHandle(fhid uint64) (fh *FileHandle, err error) {
	return fs.fhm.Get(fhid), nil
}

func (fs *FS) DeleteFileHandle(fhid uint64) (err error) {
	fs.fhm.Delete(fhid)
	return nil
}

func (fs *FS) CreateDirHandle(ino *Inode) (dh *DirHandle, err error) {
	it, err := fs.s.List(ino.Path, pairs.WithListMode(types.ListModeDir))
	if err != nil {
		return
	}

	dh = &DirHandle{
		ID:   NextHandle(),
		ino:  ino,
		fs:   fs,
		it:   it,
		meta: fs.meta,
	}
	fs.dhm.Set(dh.ID, dh)
	return dh, err
}

func (fs *FS) GetDirHandle(dhid uint64) (dh *DirHandle, err error) {
	return fs.dhm.Get(dhid), nil
}

func (fs *FS) DeleteDirHandle(dhid uint64) (err error) {
	fs.dhm.Delete(dhid)
	return nil
}

func (fs *FS) SetInode(ino *Inode) (err error) {
	bs, err := ino.MarshalMsg(nil)
	if err != nil {
		return fmt.Errorf("marshal inode: %w", err)
	}

	err = fs.meta.Set(meta.InodeKey(ino.ID), bs)
	if err != nil {
		return fmt.Errorf("set inode: %w", err)
	}
	if ino.ID == ino.ParentID {
		// Don't set entry key for root directory.
		return nil
	}
	err = fs.meta.Set(meta.EntryKey(ino.ParentID, ino.Name), bs)
	if err != nil {
		return fmt.Errorf("set entry: %w", err)
	}
	return nil
}

func (fs *FS) GetInode(id uint64) (ino *Inode, err error) {
	bs, err := fs.meta.Get(meta.InodeKey(id))
	if err != nil {
		return nil, fmt.Errorf("get inode: %w", err)
	}
	if bs == nil {
		return nil, nil
	}

	ino = &Inode{}

	_, err = ino.UnmarshalMsg(bs)
	if err != nil {
		return nil, fmt.Errorf("unmarshal inode: %w", err)
	}
	return
}

func (fs *FS) DeleteInode(ino *Inode) (err error) {
	err = fs.meta.Delete(meta.InodeKey(ino.ID))
	if err != nil {
		return fmt.Errorf("del inode: %w", err)
	}
	return
}

func (fs *FS) DeleteInodeByID(id uint64) (err error) {
	err = fs.meta.Delete(meta.InodeKey(id))
	if err != nil {
		return fmt.Errorf("del inode: %w", err)
	}
	return
}

func (fs *FS) GetEntry(parent uint64, name string) (ino *Inode, err error) {
	bs, err := fs.meta.Get(meta.EntryKey(parent, name))
	if err != nil {
		return nil, fmt.Errorf("get entry: %w", err)
	}
	if bs == nil {
		return nil, nil
	}

	ino = &Inode{}

	_, err = ino.UnmarshalMsg(bs)
	if err != nil {
		return nil, fmt.Errorf("unmarshal inode: %w", err)
	}
	return
}

func (fs *FS) DeleteEntry(parent uint64, name string) (err error) {
	err = fs.meta.Delete(meta.EntryKey(parent, name))
	if err != nil {
		return fmt.Errorf("del inode: %w", err)
	}
	return
}
