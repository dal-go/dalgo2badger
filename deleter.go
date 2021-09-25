package dalgo2badger

import (
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo"
)

func (dtb database) Delete(ctx context.Context, key *dalgo.Key) error {
	return dtb.db.Update(func(tx *badger.Txn) error {
		return transaction{txn: tx}.Delete(ctx, key)
	})
}

func (dtb database) DeleteMulti(_ context.Context, keys []*dalgo.Key) (err error) {
	return dtb.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			keyPath := dalgo.GetRecordKeyPath(key)
			if err = txn.Delete([]byte(keyPath)); err != nil {
				return err
			}
		}
		return err
	})
}

func (t transaction) Delete(ctx context.Context, key *dalgo.Key) error {
	keyPath := dalgo.GetRecordKeyPath(key)
	err := t.txn.Delete([]byte(keyPath))
	return err
}

func (t transaction) DeleteMulti(ctx context.Context, keys []*dalgo.Key) error {
	for _, key := range keys {
		if err := t.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
