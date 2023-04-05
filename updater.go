package dalgo2badger

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/dgraph-io/badger/v3"
)

func (dtb database) Update(
	ctx context.Context,
	key *dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		return transaction{txn: txn}.Update(ctx, key, updates, preconditions...)
	})
}

func (dtb database) UpdateMulti(
	ctx context.Context,
	keys []*dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	return dtb.db.Update(func(txn *badger.Txn) error {
		tx := transaction{txn: txn}
		return tx.UpdateMulti(ctx, keys, updates, preconditions...)
	})
}

func (t transaction) Update(
	ctx context.Context,
	key *dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	// we need the t.update() method as it is reused in UpdateMulti()
	return t.update(ctx, key, updates, preconditions...)
}

func (t transaction) UpdateMulti(
	ctx context.Context,
	keys []*dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	for i, key := range keys {
		if err := t.update(ctx, key, updates, preconditions...); err != nil {
			if err == dal.ErrRecordNotFound {
				continue
			}
			return fmt.Errorf("failed to update record %d of %d: %w", i+1, len(keys), err)
		}
	}
	return nil
}

func (t transaction) update(
	_ context.Context,
	key *dal.Key,
	updates []dal.Update,
	preconditions ...dal.Precondition,
) error {
	k := []byte(key.String())
	item, err := t.txn.Get(k)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return dal.ErrRecordNotFound
		}
		return err
	}
	data := make(map[string]interface{})
	err = item.Value(func(val []byte) error {
		if err = json.Unmarshal(val, &data); err != nil {
			return fmt.Errorf("failed to unmarshal data as JSON object: %v", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to unmarshal data as JSON object: %v", err)
	}
	for _, u := range updates {
		data[u.Field] = u.Value
	}
	var b []byte
	if b, err = json.Marshal(data); err != nil {
		return fmt.Errorf("failed to marshal data as JSON object: %v", err)
	}
	if err = t.txn.Set(k, b); err != nil {
		return err
	}
	return nil
}
