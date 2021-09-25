package dalgo2badger

import (
	"context"
	"encoding/json"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo"
)

func (dtb database) Get(ctx context.Context, record dalgo.Record) error {
	return dtb.db.View(func(txn *badger.Txn) error {
		return transaction{txn: txn}.Get(ctx, record)
	})
}

func (dtb database) GetMulti(ctx context.Context, records []dalgo.Record) error {
	return dtb.db.View(func(txn *badger.Txn) error {
		return transaction{txn: txn}.GetMulti(ctx, records)
	})
}

func (t transaction) Get(_ context.Context, record dalgo.Record) error {
	key := record.Key()
	keyPath := dalgo.GetRecordKeyPath(key)
	item, err := t.txn.Get([]byte(keyPath))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			err = dalgo.NewErrNotFoundByKey(key, err)
		}
		return err
	}
	return item.Value(func(val []byte) error {
		return json.Unmarshal(val, record.Data())
	})
}

func (t transaction) GetMulti(ctx context.Context, records []dalgo.Record) error {
	for _, record := range records {
		keyPath := dalgo.GetRecordKeyPath(record.Key())
		item, err := t.txn.Get([]byte(keyPath))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, record.Data())
		})
		if err != nil {
			return err
		}
	}
	return nil
}
