package dalgo2badger

import (
	"context"
	"encoding/json"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo"
)

func (dtb database) Set(ctx context.Context, record dalgo.Record) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		return transaction{txn: txn}.Set(ctx, record)
	})
}

func (dtb database) SetMulti(ctx context.Context, records []dalgo.Record) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		return transaction{txn: txn}.SetMulti(ctx, records)
	})
}

func (t transaction) Set(ctx context.Context, record dalgo.Record) error {
	key := record.Key()
	k := []byte(dalgo.GetRecordKeyPath(key))
	s, err := json.Marshal(record.Data())
	if err != nil {
		return err
	}
	err = t.txn.Set(k, s)
	return err
}

func (t transaction) SetMulti(ctx context.Context, records []dalgo.Record) error {
	for _, record := range records {
		if err := t.Set(ctx, record); err != nil {
			return err
		}
	}
	return nil
}
