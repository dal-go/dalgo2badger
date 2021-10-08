package dalgo2badger

import (
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo/dal"
	"testing"
)

func TestDeleter_Delete(t *testing.T) {
	db := openInMemoryDB(t)

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("Test/t1"), []byte{})
	})
	if err != nil {
		t.Fatal(err)
	}
	ddb := database{
		db: db,
	}

	ctx := context.Background()

	err = ddb.Delete(ctx, dal.NewKeyWithStrID("Test", "t1"))
	if err != nil {
		t.Errorf("failed to performa delete operation: %v", err)
	}
}
