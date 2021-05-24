package vfs

import (
	"errors"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/types"
	"go.uber.org/zap"
)

type FS struct {
	s     types.Storager
	direr types.Direr

	logger *zap.Logger
}

type Config struct {
	Store types.Storager

	Logger *zap.Logger
}

func New(cfg *Config) *FS {
	fs := &FS{
		s:      cfg.Store,
		logger: cfg.Logger,
	}

	if fs.logger == nil {
		fs.logger, _ = zap.NewDevelopment()
	}

	direr, ok := fs.getDirer()
	if ok {
		fs.direr = direr
	}
	return fs
}

func (fs *FS) String() string {
	return fs.s.String()
}

func (fs *FS) Root() (attr *Attr) {
	o := fs.s.Create("")
	o.Mode |= types.ModeDir
	return NewAttr(o)
}

func (fs *FS) getDirer() (dir types.Direr, ok bool) {
	if fs.direr != nil {
		return fs.direr, true
	}

	direr, ok := fs.s.(types.Direr)
	if !ok {
		return nil, false
	}
	fs.direr = direr

	return fs.direr, true
}

func (fs *FS) Lookup(path string) (attr *Attr, err error) {
	fs.logger.Debug("fs lookup path", zap.String("path", path))

	o, err := fs.s.Stat(path)
	if err != nil {
		fs.logger.Info("storage stat",
			zap.String("path", path),
			zap.Error(err))
		return nil, err
	}
	return NewAttr(o), nil
}

func (fs *FS) Create(path string) (attr *Attr, file *File, err error) {
	_, err = fs.s.Write(path, nil, 0)
	if err != nil {
		fs.logger.Error("storage write file",
			zap.String("path", path),
			zap.Stringer("fs", fs),
			zap.Error(err))
		return
	}
	o, err := fs.s.Stat(path)
	if err != nil {
		fs.logger.Error("storage stat file",
			zap.String("path", path),
			zap.Stringer("fs", fs),
			zap.Error(err))
		return
	}
	return NewAttr(o), NewFile(fs.s, o), nil
}

func (fs *FS) Open(path string) (file *File, err error) {
	o, err := fs.s.Stat(path)
	if err != nil {
		fs.logger.Error("storage stat file",
			zap.String("path", path),
			zap.Stringer("fs", fs),
			zap.Error(err))
		return
	}

	return NewFile(fs.s, o), nil
}
func (fs *FS) Delete(path string) (err error) {
	return fs.s.Delete(path)
}

func (fs *FS) Mkdir(path string) (attr *Attr, err error) {
	if fs.direr == nil {
		fs.logger.Error("fs can't create dir",
			zap.String("path", path),
			zap.Stringer("fs", fs))
		return nil, errors.New("not implemented")
	}

	o, err := fs.direr.CreateDir(path)
	if err != nil {
		fs.logger.Error("storage create dir",
			zap.String("path", path),
			zap.Stringer("fs", fs),
			zap.Error(err))
		return nil, err
	}

	return NewAttr(o), nil
}
func (fs *FS) OpenDir(path string) (dir *Dir, err error) {
	fs.logger.Debug("fs open dir", zap.String("path", path))

	dir = &Dir{
		s: fs.s,
		o: fs.s.Create(path),
	}

	dir.it, err = fs.s.List(path, ps.WithListMode(types.ListModeDir))
	if err != nil {
		fs.logger.Error("storage list dir",
			zap.String("path", path),
			zap.Error(err))
		return nil, err
	}
	return
}

func (fs *FS) Rename(path string) {}
