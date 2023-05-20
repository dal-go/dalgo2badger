package dalgo2badger

import (
	"errors"
	end2end "github.com/dal-go/dalgo-end2end-tests"
	"testing"
)

func TestEndToEnd(t *testing.T) {
	db := NewDatabase(openInMemoryDB(t))
	end2end.TestDalgoDB(t, db, errors.New("query not supported"), false)
}
