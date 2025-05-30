package dalgo2badger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/dgraph-io/badger/v4"
)

func (dtb database) Exists(ctx context.Context, key *dal.Key) (exists bool, err error) {
	err = dtb.db.View(func(txn *badger.Txn) error {
		exists, err = transaction{txn: txn}.Exists(ctx, key)
		return err
	})
	return
}

func (dtb database) Get(ctx context.Context, record dal.Record) error {
	return dtb.db.View(func(txn *badger.Txn) error {
		return transaction{txn: txn}.Get(ctx, record)
	})
}

func (dtb database) GetMulti(ctx context.Context, records []dal.Record) error {
	return dtb.db.View(func(txn *badger.Txn) error {
		return transaction{txn: txn}.GetMulti(ctx, records)
	})
}

func (t transaction) Exists(_ context.Context, key *dal.Key) (exists bool, err error) {
	keyPath := key.String()
	if _, err = t.txn.Get([]byte(keyPath)); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			err = nil
			return
		}
		return
	}
	exists = true
	return
}

func (t transaction) Get(_ context.Context, record dal.Record) error {
	key := record.Key()
	keyPath := key.String()
	item, err := t.txn.Get([]byte(keyPath))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			record.SetError(fmt.Errorf("%w: %s", dal.ErrRecordNotFound, err))
			err = dal.NewErrNotFoundByKey(key, err)
		}
		return err
	} else {
		record.SetError(dal.ErrNoError)
	}
	return item.Value(func(val []byte) error {
		data := record.Data()
		return json.Unmarshal(val, data)
	})
}

func (t transaction) GetMulti(ctx context.Context, records []dal.Record) error {
	for _, record := range records {
		key := record.Key()
		keyPath := key.String()
		item, err := t.txn.Get([]byte(keyPath))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				record.SetError(fmt.Errorf("%w: %s", dal.ErrRecordNotFound, err))
				continue
			}
			record.SetError(err)
			continue
		}
		err = item.Value(func(val []byte) error {
			record.SetError(nil)
			data := record.Data()
			return json.Unmarshal(val, data)
		})
		if err != nil {
			record.SetError(fmt.Errorf("failed to umarshal record data: %w", err))
			continue
		}
		record.SetError(dal.ErrNoError)
	}
	return nil
}
