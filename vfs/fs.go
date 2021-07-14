package vfs

import (
	"fmt"

	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"
	"go.uber.org/zap"

	"github.com/beyondstorage/beyond-fs/meta"
)

type FS struct {
	s    types.Storager
	meta meta.Service

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
		s:      store,
		meta:   metaSrv,
		logger: cfg.Logger,
	}
	return fs, err
}

func (fs *FS) Delete(path string) (err error) {
	panic("implement me")
}

func (fs *FS) DeleteDir(path string) (err error) {
	panic("implement me")
}

func (fs *FS) ListInode(id uint64) (err error) {
	return
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
	return nil
}

func (fs *FS) GetInode(id uint64) (ino *Inode, err error) {
	bs, err := fs.meta.Get(meta.InodeKey(id))
	if err != nil {
		return nil, fmt.Errorf("get inode: %w", err)
	}
	if bs != nil {
		return nil, nil
	}

	ino = &Inode{}

	_, err = ino.UnmarshalMsg(bs)
	if err != nil {
		return nil, fmt.Errorf("unmarshal inode: %w", err)
	}
	return
}

func (fs *FS) DelInode(id uint64) (err error) {
	err = fs.meta.Delete(meta.InodeKey(id))
	if err != nil {
		return fmt.Errorf("del inode: %w", err)
	}
	return
}
