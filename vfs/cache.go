package vfs

import (
	"bytes"
	"fmt"
	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"io"
	"sync"
)

type op struct {
	fd   uint64
	size int64
}

type chunk struct {
	lock sync.Mutex
	wg   *sync.WaitGroup

	// The path for the chunk.
	fd            uint64
	path          string
	persistedIdx  uint64
	persistedSize int64
	nextIdx       uint64
	currentSize   int64

	// If we have CreateMultipart or CreateAppend, we will store the object here.
	// So we can check if object == nil to decide use CompleteMultipart or call Write.
	object *types.Object

	// Only valid if we have already called CreateMultipart.
	parts          map[int]*types.Part
	nextPartNumber int
}

func newChunk(fd uint64, path string) *chunk {
	return &chunk{
		wg:   &sync.WaitGroup{},
		fd:   fd,
		path: path,
	}
}

type Cache struct {
	s      types.Storager // Real data store
	c      types.Storager // Cache data store
	logger *zap.Logger

	p  *ants.Pool
	ch chan op

	chunks    map[uint64]*chunk
	chunkLock sync.Mutex
}

func NewCache(s, c types.Storager, logger *zap.Logger) *Cache {
	cache := &Cache{
		s:      s,
		c:      c,
		logger: logger,
	}

	p, err := ants.NewPool(10)
	if err != nil {
		panic(fmt.Errorf("new pool: %v", err))
	}

	cache.p = p
	cache.ch = make(chan op)
	cache.chunks = make(map[uint64]*chunk)
	return cache
}

func (c *Cache) Start() {
	for v := range c.ch {
		c.chunkLock.Lock()
		chk := c.chunks[v.fd]
		c.chunkLock.Unlock()

		chk.lock.Lock()
		chk.nextIdx += 1
		chk.currentSize += v.size
		chk.lock.Unlock()

		// Skip persistViaWriteMultipart write operation if we don't have enough data.
		if chk.currentSize-chk.persistedSize < 64*1024*1024 {
			continue
		}

		chk.lock.Lock()

		if chk.object == nil {
			o, err := c.s.(types.Multiparter).CreateMultipart(chk.path)
			if err != nil {
				c.logger.Fatal("create multipart", zap.Error(err))
			}

			chk.object = o
			chk.parts = make(map[int]*types.Part)
		}

		start := chk.persistedIdx
		end := chk.nextIdx
		size := chk.currentSize - chk.persistedSize
		partNumber := chk.nextPartNumber
		chk.persistedSize = chk.currentSize
		chk.persistedIdx = chk.nextIdx
		chk.nextPartNumber += 1
		chk.lock.Unlock()

		chk.wg.Add(1)
		err := c.p.Submit(func() {
			defer chk.wg.Done()

			err := c.persistViaWriteMultipart(chk, start, end, size, partNumber)
			if err != nil {
				c.logger.Error("persistViaWriteMultipart", zap.Error(err))
			}
		})
		if err != nil {
			c.logger.Fatal("submit task", zap.Error(err))
		}
	}
}

func (c *Cache) complete(chk *chunk) error {
	// object == nil means data is small enough to complete in single write operation.
	// We can persist it via write.
	if chk.object == nil {
		start := uint64(0)
		end := chk.nextIdx
		size := chk.currentSize

		return c.persistViaWrite(chk, start, end, size)
	}

	// Check for dirty data.
	//
	// persistedIdx < nextIdx means we still have data to write.
	if chk.persistedIdx < chk.nextIdx {
		start := chk.persistedIdx
		end := chk.nextIdx
		size := chk.currentSize - chk.persistedSize
		partNumber := chk.nextPartNumber

		chk.wg.Add(1)
		err := c.p.Submit(func() {
			defer chk.wg.Done()

			err := c.persistViaWriteMultipart(chk, start, end, size, partNumber)
			if err != nil {
				c.logger.Error("persistViaWriteMultipart", zap.Error(err))
			}
		})
		if err != nil {
			c.logger.Fatal("submit task", zap.Error(err))
		}
	}

	// It's safe to complete the multipart after wait.
	chk.wg.Wait()

	parts := make([]*types.Part, 0, len(chk.parts))
	for i := 0; i < len(chk.parts); i++ {
		parts = append(parts, chk.parts[i])
	}

	err := c.s.(types.Multiparter).CompleteMultipart(chk.object, parts)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cache) persistViaWrite(chk *chunk, start, end uint64, size int64) error {
	r, err := c.read(chk.fd, start, end)
	if err != nil {
		return err
	}
	defer func() {
		err = r.Close()
		if err != nil {
			c.logger.Error("close reader", zap.Error(err))
			return
		}
	}()

	_, err = c.s.Write(chk.path, r, size)
	if err != nil {
		c.logger.Error("write", zap.Error(err))
		return err
	}

	err = r.Close()
	if err != nil {
		c.logger.Error("close", zap.Error(err))
		return err
	}
	return nil
}

func (c *Cache) persistViaWriteMultipart(chk *chunk, start, end uint64, size int64, partNumber int) error {
	r, err := c.read(chk.fd, start, end)
	if err != nil {
		return err
	}
	defer func() {
		err = r.Close()
		if err != nil {
			c.logger.Error("close reader", zap.Error(err))
			return
		}
	}()

	_, part, err := c.s.(types.Multiparter).WriteMultipart(chk.object, r, size, partNumber)
	if err != nil {
		c.logger.Error("write", zap.Error(err))
		return err
	}

	chk.lock.Lock()
	chk.parts[partNumber] = part
	chk.lock.Unlock()
	return nil
}

func (c *Cache) read(fd, start, end uint64) (r io.ReadCloser, err error) {
	r, w := io.Pipe()

	go func() {
		for i := start; i < end; i++ {
			p := fmt.Sprintf("%d-%d", fd, i)
			_, err := c.c.Read(p, w)
			if err != nil {
				c.logger.Error("read", zap.Error(err))
				return
			}
		}
		err := w.Close()
		if err != nil {
			c.logger.Error("close writer", zap.Error(err))
			return
		}
	}()

	return r, nil
}

func (c *Cache) startWrite(fd uint64, path string) (err error) {
	c.chunkLock.Lock()
	// FIXME: maybe we need to check the fd before set.
	c.chunks[fd] = newChunk(fd, path)
	c.chunkLock.Unlock()
	return nil
}

func (c *Cache) write(fd, idx uint64, data []byte) (n int64, err error) {
	p := fmt.Sprintf("%d-%d", fd, idx)

	size := int64(len(data))
	n, err = c.c.Write(p, bytes.NewReader(data), size)
	if err != nil {
		return
	}

	c.ch <- op{
		fd:   fd,
		size: size,
	}
	return n, nil
}

func (c *Cache) endWrite(fd uint64) (err error) {
	c.chunkLock.Lock()
	chk := c.chunks[fd]
	c.chunkLock.Unlock()

	err = c.complete(chk)
	if err != nil {
		c.logger.Error("complete", zap.Error(err))
		return
	}

	c.chunkLock.Lock()
	delete(c.chunks, fd)
	c.chunkLock.Unlock()
	return nil
}

func (c *Cache) Stop() {
	close(c.ch)
}
