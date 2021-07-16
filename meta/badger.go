package meta

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
)

type badgerDB struct {
	db *badger.DB
}

func NewBadger() (Service, error) {
	db, err := badger.Open(badger.DefaultOptions("").
		WithLogger(nil).
		WithMetricsEnabled(false).
		WithInMemory(true))
	if err != nil {
		return nil, fmt.Errorf("new pebble: %w", err)
	}

	return badgerDB{db: db}, nil
}

func (db badgerDB) Get(key []byte) (value []byte, err error) {
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	v, err := txn.Get(key)
	if err != nil && err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get key %s: %w", key, err)
	}

	return v.ValueCopy(nil)
}

func (db badgerDB) Set(key, value []byte) (err error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	e := &badger.Entry{
		Key:   key,
		Value: value,
	}

	err = txn.SetEntry(e)
	if err != nil {
		return err
	}
	return txn.Commit()
}

func (db badgerDB) Delete(key []byte) (err error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	err = txn.Delete(key)
	if err != nil {
		return err
	}
	return txn.Commit()
}

func (db badgerDB) PrefixDelete(prefix []byte) (err error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	it := txn.NewIterator(badger.IteratorOptions{
		Prefix: prefix,
	})
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		err = txn.Delete(item.Key())
		if err != nil {
			return err
		}
	}

	return txn.Commit()
}

func (db badgerDB) Scan(prefix []byte) Iterator {
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	it := txn.NewIterator(badger.IteratorOptions{
		Prefix: prefix,
	})
	it.Rewind()

	return badgerIterator{it: it}
}

type badgerIterator struct {
	it *badger.Iterator
}

func (b badgerIterator) Next() bool {
	b.it.Next()
	return b.it.Valid()
}

func (b badgerIterator) Seek(key []byte) {
	b.it.Seek(key)
}

func (b badgerIterator) Entry() (key, value []byte, err error) {
	item := b.it.Item()

	key = item.KeyCopy(nil)
	value, err = item.ValueCopy(nil)
	return
}

func (b badgerIterator) Close() {
	b.it.Close()
	b.it = nil
}
