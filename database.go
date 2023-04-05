package dalgo2badger

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/dgraph-io/badger/v3"
)

type database struct {
	db *badger.DB
}

var _ dal.Database = (*database)(nil)

// NewDatabase creates a new instance of DALgo adapter for BungDB
func NewDatabase(db *badger.DB) dal.Database {
	if db == nil {
		panic("db is a required parameter, got nil")
	}
	return database{
		db: db,
	}
}

func (dtb database) Upsert(ctx context.Context, record dal.Record) error {
	panic("implement me")
}

func (dtb database) Select(_ context.Context, _ dal.Select) (dal.Reader, error) {
	panic("implement me")
}
