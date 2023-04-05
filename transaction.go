package dalgo2badger

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/dgraph-io/badger/v4"
)

func (dtb database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	return dtb.db.View(func(txn *badger.Txn) error {
		return f(ctx, transaction{txn: txn})
	})
}

func (dtb database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	return dtb.db.Update(func(txn *badger.Txn) (err error) {
		if err = f(ctx, transaction{txn: txn}); err != nil {
			return err
		}
		// We should not manually commit the transaction, as the badger DB will do it for us.
		//if err = txn.Commit(); err != nil {
		//	return fmt.Errorf("failed to commit transaction: %w", err)
		//}
		return nil
	})
}

type transaction struct {
	txn     *badger.Txn
	options dal.TransactionOptions
}

func (t transaction) Options() dal.TransactionOptions {
	return t.options
}

func (t transaction) Upsert(_ context.Context, record dal.Record) error {
	panic("implement me")
}

func (t transaction) Select(_ context.Context, _ dal.Select) (dal.Reader, error) {
	panic("implement me")
}

var _ dal.Transaction = (*transaction)(nil)
