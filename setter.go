package dalgo2badger

import (
	"context"
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"github.com/dgraph-io/badger/v4"
)

func (dtb database) Set(ctx context.Context, record dal.Record) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		return transaction{txn: txn}.Set(ctx, record)
	})
}

func (dtb database) SetMulti(ctx context.Context, records []dal.Record) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		return transaction{txn: txn}.SetMulti(ctx, records)
	})
}

func (t transaction) Set(ctx context.Context, record dal.Record) error {
	key := record.Key()
	k := []byte(key.String())
	record.SetError(nil)
	data := record.Data()
	s, err := json.Marshal(data)
	if err != nil {
		return err
	}
	err = t.txn.Set(k, s)
	return err
}

func (t transaction) SetMulti(ctx context.Context, records []dal.Record) error {
	for _, record := range records {
		if err := t.Set(ctx, record); err != nil {
			return err
		}
	}
	return nil
}
