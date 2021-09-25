package dalgo2badger

import (
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo"
	"testing"
)

type testKind struct {
	Str string
	Int int
}

func TestGetter_Get(t *testing.T) {
	ctx := context.Background()

	const k = "TestKind/test_1"
	db := openInMemoryDB(t)
	err := db.Update(func(tx *badger.Txn) error {
		err := tx.Set([]byte(k), []byte(`{"Str":"s1", "Int":1}`))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	ddb := NewDatabase(db)

	key := dalgo.NewKeyWithStrID("TestKind", "test_1")
	data := new(testKind)
	record := dalgo.NewRecord(key, data)

	if err = ddb.Get(ctx, record); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if data.Str != "s1" {
		t.Errorf("expected 's1' for Str property, got: %v", data.Str)
	}
	if data.Int != 1 {
		t.Errorf("expected 1 for Int property, got: %v", data.Int)
	}
}
