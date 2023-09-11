package dalgo2badger

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/dgraph-io/badger/v4"
)

type database struct {
	db *badger.DB
}

func (dtb database) ID() string {
	//TODO implement me
	panic("implement me")
}

func (dtb database) Adapter() dal.Adapter {
	//TODO implement me
	panic("implement me")
}

func (dtb database) QueryReader(c context.Context, query dal.Query) (dal.Reader, error) {
	//TODO implement me
	panic("implement me")
}

func (dtb database) QueryAllRecords(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
	//TODO implement me
	panic("implement me")
}

var _ dal.DB = (*database)(nil)

// NewDatabase creates a new instance of DALgo adapter for BungDB
func NewDatabase(db *badger.DB) dal.DB {
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

//func (dtb database) Select(_ context.Context, _ dal.Select) (dal.Reader, error) {
//	panic("implement me")
//}
