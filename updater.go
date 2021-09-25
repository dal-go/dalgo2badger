package dalgo2badger

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/strongo/dalgo"
)

func (dtb database) Update(
	ctx context.Context,
	key *dalgo.Key,
	updates []dalgo.Update,
	preconditions ...dalgo.Precondition,
) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		return transaction{txn: txn}.Update(ctx, key, updates, preconditions...)
	})
}

func (dtb database) UpdateMulti(
	ctx context.Context,
	keys []*dalgo.Key,
	updates []dalgo.Update,
	preconditions ...dalgo.Precondition,
) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		return transaction{txn: txn}.UpdateMulti(ctx, keys, updates, preconditions...)
	})
}

func (t transaction) Update(
	ctx context.Context,
	key *dalgo.Key,
	updates []dalgo.Update,
	preconditions ...dalgo.Precondition,
) error {
	return t.update(ctx, key, updates, preconditions...)
}

func (t transaction) UpdateMulti(
	ctx context.Context,
	keys []*dalgo.Key,
	updates []dalgo.Update,
	preconditions ...dalgo.Precondition,
) error {
	for _, key := range keys {
		if err := t.update(ctx, key, updates, preconditions...); err != nil {
			return err
		}
	}
	return nil
}

func (t transaction) update(
	_ context.Context,
	key *dalgo.Key,
	updates []dalgo.Update,
	preconditions ...dalgo.Precondition,
) error {
	k := []byte(dalgo.GetRecordKeyPath(key))
	item, err := t.txn.Get(k)
	if err != nil {
		return err
	}
	data := make(map[string]interface{})
	err = item.Value(func(val []byte) error {
		if err = json.Unmarshal(val, &data); err != nil {
			return fmt.Errorf("failed to unmarshal data as JSON object: %v", err)
		}
		return nil
	})
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal data as JSON object: %v", err)
	}
	for range updates {
	}
	err = t.txn.Set(k, b)
	if err != nil {
		return err
	}
	return nil
}
