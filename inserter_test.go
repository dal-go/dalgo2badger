package dalgo2badger

import (
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo/dal"
	"testing"
)

const memory = ":memory:"

func TestInserter_Insert(t *testing.T) {
	bdb := openInMemoryDB(t)
	ctx := context.Background()
	key := dal.NewKeyWithStrID("TestKind", "test-id")
	data := new(testKind)
	record := dal.NewRecordWithData(key, data)
	db := NewDatabase(bdb)
	if err := db.Insert(ctx, record); err != nil {
		t.Errorf("expected to be successful, got error: %v", err)
	}
	if err := bdb.View(func(tx *badger.Txn) error {
		const id = "TestKind/test-id"
		if _, err := tx.Get([]byte(id)); err != nil {
			t.Errorf("Inserted record is not found by ID: " + id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
