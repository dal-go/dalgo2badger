package dalgo2badger

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/dal-go/dalgo/dal"
	"github.com/dgraph-io/badger/v3"
)

// ErrKeyAlreadyExists an error to be used in insert when generated key already exists
var ErrKeyAlreadyExists = errors.New("key already exists")

func (dtb database) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		t := transaction{txn: txn}
		return t.Insert(ctx, record, opts...)
	})
}

func (t transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	options := dal.NewInsertOptions(opts...)
	generateID := options.IDGenerator()
	if generateID == nil {
		return t.insert(record)
	}
	return t.insertWithGenerator(ctx, generateID, record)
}

func (t transaction) insertWithGenerator(ctx context.Context, generateID dal.IDGenerator, record dal.Record) error {
	for i := 0; i < 10; i++ {
		if err := generateID(ctx, record); err != nil {
			return err
		}
		if err := t.insert(record); err != nil {
			if err == ErrKeyAlreadyExists {
				continue
			}
			return err
		}
	}
	return nil
}

func (t transaction) insert(record dal.Record) error {
	key := record.Key()
	k := []byte(key.String())
	if _, err := t.txn.Get(k); err == nil {
		return ErrKeyAlreadyExists
	} else if err != badger.ErrKeyNotFound {
		return err
	}
	s, err := json.Marshal(record.Data())
	if err != nil {
		return err
	}
	err = t.txn.Set(k, s)
	return err
}
