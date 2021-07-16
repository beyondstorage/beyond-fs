package hanwen

import (
	"errors"
	"os"
	"time"

	"github.com/beyondstorage/go-storage/v4/services"
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
	out.Mode = parseMode(i.Mode)

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
	out.Mode = parseMode(i.Mode)

	out.Blocks = (out.Size + 255) / 256
	out.Nlink = 1

	out.Gid = 1000
	out.Uid = 1000

	out.SetTimes(&i.Atime, &i.Mtime, &i.Ctime)

	return fuse.OK
}

func fillOpenOut(fh *vfs.FileHandle, out *fuse.OpenOut) fuse.Status {
	out.Fh = fh.ID
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

func parseType(o uint32) uint32 {
	osMode := os.FileMode(o)
	var mode uint32
	if osMode.IsDir() {
		mode = fuse.S_IFDIR
	} else {
		mode = fuse.S_IFREG
	}
	return mode
}

func parseMode(o uint32) uint32 {
	osMode := os.FileMode(o)
	var mode uint32
	if osMode.IsDir() {
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

	node, err := fs.fs.GetEntry(ino.ID, name)
	if err != nil && errors.Is(err, services.ErrObjectNotExist) {
		return fuse.ENOENT
	}
	if err != nil {
		fs.logger.Error("get entry", zap.Error(err))
		return fuse.EAGAIN
	}
	if node == nil {
		return fuse.ENOENT
	}
	return fillEntryOut(node, out)
}

func (fs *FS) Forget(nodeid, nlookup uint64) {
	err := fs.fs.DeleteInodeByID(nodeid)
	if err != nil {
		fs.logger.Error("forget", zap.Error(err))
	}
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

	err = fs.fs.Delete(ino.ID, name)
	if err != nil {
		fs.logger.Error("internal error",
			zap.Error(err))
		return fuse.EAGAIN
	}
	return fuse.OK
}

func (fs *FS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	return fuse.ENOSYS
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

	i, fh, err := fs.fs.Create(ino.ID, name)
	if err != nil {
		fs.logger.Error("create", zap.Error(err))
		return fuse.EAGAIN
	}
	fs.logger.Info("start fill open out")
	fillOpenOut(fh, &out.OpenOut)
	fs.logger.Info("start fill entry out")
	fillEntryOut(i, &out.EntryOut)
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

	fh, err := fs.fs.CreateFileHandle(ino)
	if err != nil {
		fs.logger.Error("create file handle", zap.Error(err))
		return
	}
	return fillOpenOut(fh, out)
}

func (fs *FS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fh, err := fs.fs.GetFileHandle(input.Fh)
	if err != nil {
		fs.logger.Error("get file handle", zap.Error(err))
		return nil, fuse.EAGAIN
	}

	n, err := fh.Read(input.Offset, buf)
	if err != nil {
		fs.logger.Error("read", zap.Error(err))
		return nil, fuse.EAGAIN
	}
	return fuse.ReadResultData(buf[:n]), fuse.OK

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
	err := fs.fs.DeleteFileHandle(input.Fh)
	if err != nil {
		fs.logger.Error("release",
			zap.Uint64("file_handle", input.Fh),
			zap.Error(err))
	}
}

func (fs *FS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	panic("implement me")
}

func (fs *FS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	// FIXME: maybe we need to write data here.
	return fuse.OK
}

func (fs *FS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

func (fs *FS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) (code fuse.Status) {
	return fuse.OK
}

func (fs *FS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
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

	dh, err := fs.fs.CreateDirHandle(ino)
	if err != nil {
		fs.logger.Error("open dir",
			zap.Uint64("parent", input.NodeId),
			zap.Error(err))
		return fuse.EAGAIN
	}

	out.Fh = dh.ID
	return fuse.OK
}

func (fs *FS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	dh, err := fs.fs.GetDirHandle(input.Fh)
	if err != nil {
		fs.logger.Error("get dir handle", zap.Error(err))
		return fuse.EAGAIN
	}

	for {
		node, err := dh.Next()
		if err != nil {
			fs.logger.Error("get next inode", zap.Error(err))
			return fuse.EAGAIN
		}
		if node == nil {
			break
		}

		ok := out.AddDirEntry(fuse.DirEntry{
			Mode: parseMode(node.Mode),
			Name: node.Name,
			Ino:  node.ID,
		})
		if !ok {
			break
		}
	}
	return fuse.OK
}

func (fs *FS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	dh, err := fs.fs.GetDirHandle(input.Fh)
	if err != nil {
		fs.logger.Error("get dir handle", zap.Error(err))
		return fuse.EAGAIN
	}

	for {
		node, err := dh.Next()
		if err != nil {
			fs.logger.Error("get next inode", zap.Error(err))
			return fuse.EAGAIN
		}
		if node == nil {
			break
		}

		entry := out.AddDirLookupEntry(fuse.DirEntry{
			Mode: parseMode(node.Mode),
			Name: node.Name,
			Ino:  node.ID,
		})
		if entry == nil {
			break
		}
		fillEntryOut(node, entry)
	}
	return fuse.OK
}

func (fs *FS) ReleaseDir(input *fuse.ReleaseIn) {
	err := fs.fs.DeleteDirHandle(input.Fh)
	if err != nil {
		fs.logger.Error("delete dir handle", zap.Error(err))
	}
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
}
