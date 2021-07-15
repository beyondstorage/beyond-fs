package meta

import (
	"bytes"
	"testing"
	"time"
)

func BenchmarkGet(b *testing.B) {
	srv, err := NewBadger()
	if err != nil {
		b.Error(err)
		return
	}

	key := bytes.Repeat([]byte{'a'}, 128)
	value := bytes.Repeat([]byte{'a'}, 1024)

	err = srv.Set(key, value, time.Hour)
	if err != nil {
		b.Error(err)
		return
	}

	for i := 0; i < b.N; i++ {
		_, _ = srv.Get(key)
	}
}

func BenchmarkSet(b *testing.B) {
	srv, err := NewBadger()
	if err != nil {
		b.Error(err)
		return
	}

	key := bytes.Repeat([]byte{'a'}, 128)
	value := bytes.Repeat([]byte{'a'}, 1024)

	for i := 0; i < b.N; i++ {
		_ = srv.Set(key, value, time.Hour)
	}
}
