package dalgo2badger

import (
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo"
)

func (dtb database) RunInTransaction(ctx context.Context, f func(context.Context, dalgo.Transaction) error, options ...dalgo.TransactionOption) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		return f(ctx, transaction{txn: txn})
	})
}

type transaction struct {
	txn *badger.Txn
}

func (t transaction) Upsert(_ context.Context, record dalgo.Record) error {
	panic("implement me")
}

var _ dalgo.Transaction = (*transaction)(nil)
