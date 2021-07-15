package meta

import "github.com/Xuanwo/go-bufferpool"

var (
	pool = bufferpool.New(128)

	// i:<ino> => Inode
	inodePrefix = []byte("i:")
	// d:<ino>:<name> => Inode
	dirPrefix = []byte("d:")
)

func InodeKey(id uint64) []byte {
	buf := pool.Get()
	defer buf.Free()

	buf.AppendBytes(inodePrefix)
	buf.AppendUint(id)

	return buf.BytesCopy()
}

func EntryKey(id uint64, name string) []byte {
	buf := pool.Get()
	defer buf.Free()

	buf.AppendBytes(dirPrefix)
	buf.AppendUint(id)
	buf.AppendByte(':')
	buf.AppendString(name)

	return buf.BytesCopy()
}

func EntryPrefix(id uint64) []byte {
	buf := pool.Get()
	defer buf.Free()

	buf.AppendBytes(dirPrefix)
	buf.AppendUint(id)
	buf.AppendByte(':')

	return buf.BytesCopy()
}
