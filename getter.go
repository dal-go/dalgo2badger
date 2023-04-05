package dalgo2badger

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/dgraph-io/badger/v4"
)

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

func (t transaction) Get(_ context.Context, record dal.Record) error {
	key := record.Key()
	keyPath := key.String()
	item, err := t.txn.Get([]byte(keyPath))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			record.SetError(fmt.Errorf("%w: %s", dal.ErrRecordNotFound, err))
			err = dal.NewErrNotFoundByKey(key, err)
		}
		return err
	} else {
		record.SetError(dal.NoError)
	}
	return item.Value(func(val []byte) error {
		return json.Unmarshal(val, record.Data())
	})
}

func (t transaction) GetMulti(ctx context.Context, records []dal.Record) error {
	for _, record := range records {
		key := record.Key()
		keyPath := key.String()
		item, err := t.txn.Get([]byte(keyPath))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				record.SetError(fmt.Errorf("%w: %s", dal.ErrRecordNotFound, err))
				continue
			}
			record.SetError(err)
			continue
		}
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, record.Data())
		})
		if err != nil {
			record.SetError(fmt.Errorf("failed to umarshal record data: %w", err))
			continue
		}
		record.SetError(dal.NoError)
	}
	return nil
}
