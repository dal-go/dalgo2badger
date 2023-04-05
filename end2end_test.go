package dalgo2badger

import (
	end2end "github.com/dal-go/dalgo-end2end-tests"
	"testing"
)

func TestEndToEnd(t *testing.T) {
	db := NewDatabase(openInMemoryDB(t))
	end2end.TestDalgoDB(t, db)
}
