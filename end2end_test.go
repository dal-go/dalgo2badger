package dalgo2badger

import (
	"github.com/strongo/dalgo-end2end-tests"
	"testing"
)

func TestEndToEnd(t *testing.T) {
	db := NewDatabase(openInMemoryDB(t))
	end2end.EndToEnd(t, db)
}
