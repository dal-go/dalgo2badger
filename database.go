package dalgo2badger

import (
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo"
)

type database struct {
	db *badger.DB
}

var _ dalgo.Database = (*database)(nil)

// NewDatabase creates a new instance of DALgo adapter for BungDB
func NewDatabase(db *badger.DB) dalgo.Database {
	if db == nil {
		panic("db is a required parameter, got nil")
	}
	return database{
		db: db,
	}
}

func (dtb database) Upsert(ctx context.Context, record dalgo.Record) error {
	panic("implement me")
}
