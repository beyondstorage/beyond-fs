package meta

type Service interface {
	// Get will get the value with specified key.
	//
	// value will be nil if key not found.
	Get(key []byte) (value []byte, err error)
	Set(key, value []byte) (err error)
	Del(key []byte) (err error)
	Scan(prefix []byte) Iterator
}

type Iterator interface {
	Next() bool
	Seek(key []byte)
	Entry() (key, value []byte, err error)
	Close()
}
