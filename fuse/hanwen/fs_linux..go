package hanwen

import "github.com/hanwen/go-fuse/v2/fuse"

func fillEntryOutPlatform(i *Inode, out *fuse.EntryOut) {
	out.Blksize = 512
}

func fillAttrOutPlatform(i *Inode, out *fuse.AttrOut) {
	out.Blksize = 512
}
