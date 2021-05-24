package hanwen

import (
	"errors"
	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"
	"time"

	"github.com/beyondstorage/go-fs/vfs"
)

type FS struct {
	fs *vfs.FS

	inodes *InodeMap
	hs     *HandlerMap

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

		inodes: NewInodeMap(),
		hs:     NewHandlerMap(),

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

func fillEntryOut(i *Inode, out *fuse.EntryOut) fuse.Status {
	out.SetAttrTimeout(time.Minute)
	out.SetEntryTimeout(10 * time.Minute)

	out.NodeId = i.id
	out.Generation = 1
	out.Ino = i.id
	out.Size = uint64(i.attr.Size())
	out.Mode = parseMode(i.attr.Mode())

	out.Blocks = (out.Size + 255) / 256
	out.Nlink = 1

	out.Gid = 1000
	out.Uid = 1000

	mtime := i.attr.Mtime()
	out.SetTimes(&mtime, &mtime, &mtime)

	fillEntryOutPlatform(i, out)
	return fuse.OK
}

func fillAttrOut(i *Inode, out *fuse.AttrOut) fuse.Status {
	out.SetTimeout(time.Minute)

	out.Ino = i.id
	out.Size = uint64(i.attr.Size())
	out.Mode = parseMode(i.attr.Mode())

	out.Blocks = (out.Size + 255) / 256
	out.Blksize = 512
	out.Nlink = 1

	out.Gid = 1000
	out.Uid = 1000

	mtime := i.attr.Mtime()
	out.SetTimes(&mtime, &mtime, &mtime)

	fillAttrOutPlatform(i, out)
	return fuse.OK
}

func fillOpenOut(fh *Handler, out *fuse.OpenOut) fuse.Status {
	out.Fh = fh.id
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
	ino, ok := fs.inodes.Get(header.NodeId)
	if !ok {
		fs.logger.Error("parent inode not found",
			zap.Uint64("parent", header.NodeId))
		return fuse.ENOENT
	}

	if !ino.attr.Mode().IsDir() {
		fs.logger.Error("parent inode is not a dir",
			zap.Uint64("parent", header.NodeId),
			zap.Stringer("mode", ino.attr.Mode()))
		return fuse.EINVAL
	}

	path := ino.FormatPath(name)
	attr, err := fs.fs.Lookup(path)
	if err != nil {
		return parseError(err)
	}
	i := fs.inodes.New(attr)

	return fillEntryOut(i, out)
}

func (fs *FS) Forget(nodeid, nlookup uint64) {
	fs.inodes.Del(nodeid)
}

func (fs *FS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	ino, ok := fs.inodes.Get(input.NodeId)
	if !ok {
		fs.logger.Error("inode not found",
			zap.Uint64("inode", input.NodeId))
		return fuse.ENOENT
	}

	return fillAttrOut(ino, out)
}

func (fs *FS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	ino, ok := fs.inodes.Get(input.NodeId)
	if !ok {
		fs.logger.Error("inode not found",
			zap.Uint64("inode", input.NodeId))
		return fuse.ENOENT
	}

	ino.Expire()

	return fillAttrOut(ino, out)
}

func (fs *FS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	panic("implement me")
}

func (fs *FS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	ino, ok := fs.inodes.Get(input.NodeId)
	if !ok {
		fs.logger.Error("parent inode not found",
			zap.Uint64("parent", input.NodeId))
		return fuse.ENOENT
	}

	if !ino.attr.Mode().IsDir() {
		fs.logger.Error("parent inode is not a dir",
			zap.Uint64("parent", input.NodeId),
			zap.Stringer("mode", ino.attr.Mode()))
		return fuse.EINVAL
	}

	path := ino.FormatPath(name)
	attr, err := fs.fs.Mkdir(path)
	if err != nil {
		return parseError(err)
	}
	i := fs.inodes.New(attr)

	return fillEntryOut(i, out)
}

func (fs *FS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	ino, ok := fs.inodes.Get(header.NodeId)
	if !ok {
		fs.logger.Error("parent inode not found",
			zap.Uint64("parent", header.NodeId))
		return fuse.ENOENT
	}

	if !ino.attr.Mode().IsDir() {
		fs.logger.Error("parent inode is not a dir",
			zap.Uint64("parent", header.NodeId),
			zap.Stringer("mode", ino.attr.Mode()))
		return fuse.EINVAL
	}

	path := ino.FormatPath(name)
	err := fs.fs.Delete(path)
	if err != nil {
		return parseError(err)
	}

	return fuse.OK
}

func (fs *FS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	ino, ok := fs.inodes.Get(header.NodeId)
	if !ok {
		fs.logger.Error("parent inode not found",
			zap.Uint64("parent", header.NodeId))
		return fuse.ENOENT
	}

	if !ino.attr.Mode().IsDir() {
		fs.logger.Error("parent inode is not a dir",
			zap.Uint64("parent", header.NodeId),
			zap.Stringer("mode", ino.attr.Mode()))
		return fuse.EINVAL
	}

	path := ino.FormatPath(name)
	err := fs.fs.Delete(path)
	if err != nil {
		return parseError(err)
	}

	return fuse.OK
}

func (fs *FS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	panic("implement me")
}

func (fs *FS) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) (code fuse.Status) {
	panic("implement me")
}

func (fs *FS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	panic("implement me")
}

func (fs *FS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, code fuse.Status) {
	panic("implement me")
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
	ino, ok := fs.inodes.Get(input.NodeId)
	if !ok {
		fs.logger.Error("parent inode not found",
			zap.Uint64("parent", input.NodeId))
		return fuse.ENOENT
	}

	if !ino.attr.Mode().IsDir() {
		fs.logger.Error("parent inode is not a dir",
			zap.Uint64("parent", input.NodeId),
			zap.Stringer("mode", ino.attr.Mode()))
		return fuse.EINVAL
	}

	path := ino.FormatPath(name)
	attr, file, err := fs.fs.Create(path)
	if err != nil {
		return parseError(err)
	}

	i := fs.inodes.New(attr)
	fh := fs.hs.NewFile(file, ino)

	fillEntryOut(i, &out.EntryOut)
	fillOpenOut(fh, &out.OpenOut)
	return fuse.OK
}

func (fs *FS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	ino, ok := fs.inodes.Get(input.NodeId)
	if !ok {
		fs.logger.Error("parent inode not found",
			zap.Uint64("parent", input.NodeId))
		return fuse.ENOENT
	}

	file, err := fs.fs.Open(ino.Path())
	if err != nil {
		return fuse.EAGAIN
	}

	fh := fs.hs.NewFile(file, ino)
	return fillOpenOut(fh, out)
}

func (fs *FS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fh, ok := fs.hs.Get(input.Fh)
	if !ok {
		fs.logger.Error("file handle not found",
			zap.Uint64("file handle", input.Fh))
		return nil, fuse.ENOENT
	}

	_, err := fh.f.Read(int64(input.Offset), buf)
	if err != nil {
		return nil, parseError(err)
	}

	return fuse.ReadResultData(buf), fuse.OK
}

func (fs *FS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	fh, ok := fs.hs.Get(in.Fh)
	if !ok {
		fs.logger.Error("file handle not found",
			zap.Uint64("file handle", in.Fh))
		return fuse.ENOENT
	}

	off, err := fh.f.Seek(int(in.Whence), int64(in.Offset))
	if err != nil {
		return parseError(err)
	}

	out.Offset = uint64(off)
	return fuse.OK
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
	_, deleted := fs.hs.Del(input.Fh)
	if !deleted {
		fs.logger.Warn("file handle not found",
			zap.Uint64("file handle", input.Fh))
		return
	}
}

func (fs *FS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	fh, ok := fs.hs.Get(input.Fh)
	if !ok {
		fs.logger.Error("file handle not found",
			zap.Uint64("file handle", input.Fh))
		return 0, fuse.ENOENT
	}

	n, err := fh.f.Write(int64(input.Offset), data)
	if err != nil {
		return uint32(n), parseError(err)
	}

	return uint32(n), fuse.OK
}

func (fs *FS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	fh, ok := fs.hs.Get(input.Fh)
	if !ok {
		fs.logger.Error("file handle not found",
			zap.Uint64("file handle", input.Fh))
		return fuse.ENOENT
	}

	// Flush data into cache.
	fh.f.Flush()
	// Mark internal object to be expired.
	fh.ino.Expire()
	return fuse.OK
}

func (fs *FS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

func (fs *FS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) (code fuse.Status) {
	return fuse.OK
}

func (fs *FS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	ino, ok := fs.inodes.Get(input.NodeId)
	if !ok {
		fs.logger.Error("parent inode not found",
			zap.Uint64("parent", input.NodeId))
		return fuse.ENOENT
	}

	dir, err := fs.fs.OpenDir(ino.Path())
	if err != nil {
		return fuse.EAGAIN
	}

	fh := fs.hs.NewDir(dir, ino)
	return fillOpenOut(fh, out)
}

func (fs *FS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fh, ok := fs.hs.Get(input.Fh)
	if !ok {
		fs.logger.Error("file handle not found",
			zap.Uint64("file handle", input.Fh))
		return fuse.ENOENT
	}

	for {
		attr, hasNext, err := fh.d.Next()
		if err != nil {
			return fuse.EAGAIN
		}
		if !hasNext {
			break
		}

		i := fs.inodes.New(attr)

		ok := out.AddDirEntry(fuse.DirEntry{
			Mode: parseType(i.attr.Mode()),
			Name: i.Name(),
			Ino:  i.id,
		})
		if !ok {
			break
		}
	}

	return fuse.OK
}

func (fs *FS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fh, ok := fs.hs.Get(input.Fh)
	if !ok {
		fs.logger.Error("file handle not found",
			zap.Uint64("file handle", input.Fh))
		return fuse.ENOENT
	}

	for {
		attr, hasNext, err := fh.d.Next()
		if err != nil {
			return fuse.EAGAIN
		}
		if !hasNext {
			break
		}

		i := fs.inodes.New(attr)

		entry := out.AddDirLookupEntry(fuse.DirEntry{
			Mode: parseType(i.attr.Mode()),
			Name: i.Name(),
			Ino:  i.id,
		})
		if entry == nil {
			fs.inodes.Del(i.id)
			break
		}
		fillEntryOut(i, entry)
	}

	return fuse.OK
}

func (fs *FS) ReleaseDir(input *fuse.ReleaseIn) {
	_, deleted := fs.hs.Del(input.Fh)
	if !deleted {
		fs.logger.Warn("file handle not found",
			zap.Uint64("file handle", input.Fh))
		return
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
	fs.inodes.Init(fs.fs.Root())
}
