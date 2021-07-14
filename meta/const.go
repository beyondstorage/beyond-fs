package meta

import "github.com/Xuanwo/go-bufferpool"

var (
	pool = bufferpool.New(128)

	inodePrefix = []byte("i:")
)

func InodeKey(id uint64) []byte {
	buf := pool.Get()
	defer buf.Free()

	buf.AppendBytes(inodePrefix)
	buf.AppendUint(id)

	return buf.BytesCopy()
}
