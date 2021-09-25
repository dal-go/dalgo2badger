package dalgo2badger

import (
	"github.com/dgraph-io/badger/v3"
	"testing"
)

func TestNewDatabase(t *testing.T) {
	db := openInMemoryDB(t)
	var dtb = NewDatabase(db)
	if dtb == nil {
		t.Error("NewDatabase returned nil")
	}
}

func openInMemoryDB(t *testing.T) *badger.DB {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return db
}
