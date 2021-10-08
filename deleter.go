package dalgo2badger

import (
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo/dal"
)

func (dtb database) Delete(ctx context.Context, key *dal.Key) error {
	return dtb.db.Update(func(tx *badger.Txn) error {
		return transaction{txn: tx}.Delete(ctx, key)
	})
}

func (dtb database) DeleteMulti(_ context.Context, keys []*dal.Key) (err error) {
	return dtb.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			keyPath := key.String()
			if err = txn.Delete([]byte(keyPath)); err != nil {
				return err
			}
		}
		return err
	})
}

func (t transaction) Delete(ctx context.Context, key *dal.Key) error {
	keyPath := key.String()
	err := t.txn.Delete([]byte(keyPath))
	return err
}

func (t transaction) DeleteMulti(ctx context.Context, keys []*dal.Key) error {
	for _, key := range keys {
		if err := t.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
