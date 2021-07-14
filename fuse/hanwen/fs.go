package hanwen

import (
	"errors"
	"time"

	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"

	"github.com/beyondstorage/beyond-fs/vfs"
)

type FS struct {
	fs *vfs.FS

	logger *zap.Logger
}

type Config struct {
	FileSystem *vfs.FS
	MountPoint string

	Logger *zap.Logger
}

func New(cfg *Config) (srv *fuse.Server, err error) {
	fuseFS := &FS{
		fs: cfg.FileSystem,

		logger: cfg.Logger,
	}

	if fuseFS.logger == nil {
		fuseFS.logger, _ = zap.NewDevelopment()
	}

	return fuse.NewServer(fuseFS, cfg.MountPoint, &fuse.MountOptions{
		AllowOther:               true,
		Options:                  nil,
		MaxBackground:            0,
		MaxWrite:                 0,
		MaxReadAhead:             0,
		IgnoreSecurityLabels:     false,
		RememberInodes:           false,
		FsName:                   "",
		Name:                     "",
		SingleThreaded:           false,
		DisableXAttrs:            false,
		Debug:                    true,
		EnableLocks:              false,
		ExplicitDataCacheControl: false,
		DirectMount:              false,
		DirectMountFlags:         0,
	})
}

func fillEntryOut(i *vfs.Inode, out *fuse.EntryOut) fuse.Status {
	out.SetAttrTimeout(time.Minute)
	out.SetEntryTimeout(10 * time.Minute)

	out.NodeId = i.ID
	out.Generation = 1
	out.Ino = i.ID
	out.Size = i.Size
	out.Mode = i.Mode

	out.Blocks = (out.Size + 255) / 256
	out.Nlink = 1

	out.Gid = 1000
	out.Uid = 1000

	out.SetTimes(&i.Atime, &i.Mtime, &i.Ctime)

	return fuse.OK
}

func fillAttrOut(i *vfs.Inode, out *fuse.AttrOut) fuse.Status {
	out.SetTimeout(time.Minute)

	out.Ino = i.ID
	out.Size = i.Size
	out.Mode = i.Mode

	out.Blocks = (out.Size + 255) / 256
	out.Nlink = 1

	out.Gid = 1000
	out.Uid = 1000

	out.SetTimes(&i.Atime, &i.Mtime, &i.Ctime)

	return fuse.OK
}

func parseError(err error) fuse.Status {
	switch {
	case errors.Is(err, services.ErrObjectNotExist):
		return fuse.ENOENT
	case errors.Is(err, services.ErrPermissionDenied):
		return fuse.EACCES
	default:
		return fuse.EAGAIN
	}
}

func parseType(o types.ObjectMode) uint32 {
	var mode uint32
	if o.IsDir() {
		mode = fuse.S_IFDIR
	} else {
		mode = fuse.S_IFREG
	}
	return mode
}

func parseMode(o types.ObjectMode) uint32 {
	var mode uint32
	if o.IsDir() {
		mode = fuse.S_IFDIR | 0755
	} else {
		mode = fuse.S_IFREG | 0644
	}
	return mode
}

func (fs *FS) String() string {
	return "beyondfs"
}

func (fs *FS) SetDebug(debug bool) {
	return
}

func (fs *FS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (status fuse.Status) {
	ino, err := fs.fs.GetInode(header.NodeId)
	if err != nil {
		fs.logger.Error("internal error", zap.Error(err))
		return fuse.EAGAIN
	}
	if ino == nil {
		fs.logger.Error("parent inode not found",
			zap.Uint64("parent", header.NodeId))
		return fuse.ENOENT
	}

	if !ino.IsDir() {
		fs.logger.Error("parent inode is not a dir",
			zap.Uint64("parent", header.NodeId),
			zap.Uint32("mode", ino.Mode))
		return fuse.EINVAL
	}

	panic("implement me")

}

func (fs *FS) Forget(nodeid, nlookup uint64) {
	fs.fs.DelInode(nodeid)
}

func (fs *FS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	ino, err := fs.fs.GetInode(input.NodeId)
	if err != nil {
		fs.logger.Error("internal error",
			zap.Error(err))
		return fuse.EAGAIN
	}
	if ino == nil {
		fs.logger.Error("inode not found",
			zap.Uint64("inode", input.NodeId))
		return fuse.ENOENT
	}

	return fillAttrOut(ino, out)
}

func (fs *FS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	ino, err := fs.fs.GetInode(input.NodeId)
	if err != nil {
		fs.logger.Error("internal error",
			zap.Error(err))
		return fuse.EAGAIN
	}
	if ino == nil {
		fs.logger.Error("inode not found",
			zap.Uint64("inode", input.NodeId))
		return fuse.ENOENT
	}

	// TODO: we need to update attr

	return fillAttrOut(ino, out)
}

func (fs *FS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	panic("implement me")
}

func (fs *FS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	ino, err := fs.fs.GetInode(header.NodeId)
	if err != nil {
		fs.logger.Error("internal error",
			zap.Error(err))
		return fuse.EAGAIN
	}
	if ino == nil {
		fs.logger.Error("inode not found",
			zap.Uint64("inode", header.NodeId))
		return fuse.ENOENT
	}

	if !ino.IsDir() {
		fs.logger.Error("parent inode is not a dir",
			zap.Uint64("parent", header.NodeId),
			zap.Uint32("mode", ino.Mode))
		return fuse.EINVAL
	}

	// Implement me

	return fuse.OK
}

func (fs *FS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	ino, err := fs.fs.GetInode(header.NodeId)
	if err != nil {
		fs.logger.Error("internal error",
			zap.Error(err))
		return fuse.EAGAIN
	}
	if ino == nil {
		fs.logger.Error("inode not found",
			zap.Uint64("inode", header.NodeId))
		return fuse.ENOENT
	}

	if !ino.IsDir() {
		fs.logger.Error("parent inode is not a dir",
			zap.Uint64("parent", header.NodeId),
			zap.Uint32("mode", ino.Mode))
		return fuse.EINVAL
	}

	// Implement me

	return fuse.OK
}

func (fs *FS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FS) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (fs *FS) Access(cancel <-chan struct{}, input *fuse.AccessIn) (code fuse.Status) {
	return fuse.OK
}

func (fs *FS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (sz uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	ino, err := fs.fs.GetInode(input.NodeId)
	if err != nil {
		fs.logger.Error("internal error",
			zap.Error(err))
		return fuse.EAGAIN
	}
	if ino == nil {
		fs.logger.Error("inode not found",
			zap.Uint64("inode", input.NodeId))
		return fuse.ENOENT
	}

	if !ino.IsDir() {
		fs.logger.Error("parent inode is not a dir",
			zap.Uint64("parent", input.NodeId),
			zap.Uint32("mode", ino.Mode))
		return fuse.EINVAL
	}

	// Implement me
	return fuse.OK
}

func (fs *FS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	ino, err := fs.fs.GetInode(input.NodeId)
	if err != nil {
		fs.logger.Error("internal error",
			zap.Error(err))
		return fuse.EAGAIN
	}
	if ino == nil {
		fs.logger.Error("inode not found",
			zap.Uint64("inode", input.NodeId))
		return fuse.ENOENT
	}

	panic("implement me")
}

func (fs *FS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	panic("implement me")
}

func (fs *FS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	panic("implement me")
}

func (fs *FS) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FS) SetLk(cancel <-chan struct{}, input *fuse.LkIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FS) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	panic("implement me")
}

func (fs *FS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	panic("implement me")
}

func (fs *FS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	panic("implement me")
}

func (fs *FS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

func (fs *FS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) (code fuse.Status) {
	return fuse.OK
}

func (fs *FS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	panic("implement me")
}

func (fs *FS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	panic("implement me")
}

func (fs *FS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	panic("implement me")
}

func (fs *FS) ReleaseDir(input *fuse.ReleaseIn) {
	panic("implement me")
}

func (fs *FS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

func (fs *FS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) (code fuse.Status) {
	out.Bsize = BlockSize
	out.Blocks = MaximumBlocks
	out.Bfree = MaximumBlocks
	out.Bavail = MaximumBlocks
	out.Ffree = MaximumSpace
	return fuse.OK
}

func (fs *FS) Init(server *fuse.Server) {
	panic("implement me")
}
