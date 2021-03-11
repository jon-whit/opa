package tikv

import (
	"container/list"
	"context"
	"fmt"
	"strings"

	"github.com/open-policy-agent/opa/storage"
	"github.com/pingcap/tidb/kv"
)

type transaction struct {
	tikvtxn kv.Transaction
	updates *list.List
}

func (txn *transaction) ID() uint64 {
	return 1
}

func (tx *transaction) Commit(ctx context.Context) error {
	return fmt.Errorf("Not Implemented")
}

func (txn *transaction) ListPolicies(ctx context.Context) ([]string, error) {

	iter, err := txn.tikvtxn.Iter(kv.Key("policies/"), nil)
	if err != nil {
		return nil, err
	}

	ids := []string{}
	for iter.Valid() {
		kvKey := iter.Key()

		splitKey := strings.Split(string(kvKey), "/")

		ids = append(ids, splitKey[1])

		iter.Next()
	}
	iter.Close()

	return ids, nil
}

func (txn *transaction) GetPolicy(ctx context.Context, id string) ([]byte, error) {

	keyStr := fmt.Sprintf("policies/%s", id)
	val, err := txn.tikvtxn.Get(ctx, kv.Key([]byte(keyStr)))
	if err != nil {
		if err == kv.ErrNotExist {
			return nil, notFoundErrorf("policy id %q", id)
		}
	}

	return val, nil
}

func (txn *transaction) UpsertPolicy(ctx context.Context, id string, bs []byte) error {

	keyStr := fmt.Sprintf("policies/%s", id)
	return txn.tikvtxn.Set(kv.Key([]byte(keyStr)), bs)
}

func (txn *transaction) DeletePolicy(ctx context.Context, id string) error {

	keyStr := fmt.Sprintf("policies/%s", id)
	return txn.tikvtxn.Delete(kv.Key([]byte(keyStr)))
}

func (txn *transaction) Write(op storage.PatchOp, path storage.Path, value interface{}) error {
	if len(path) == 0 {
		return txn.updateRoot(underlying, op, *ptrval)
	}

	for curr := txn.updates.Front(); curr != nil; {

		update := curr.Value.(*update)

		_ = update
	}

	update, err := newUpdate(op, path, 0, value)
	if err != nil {
		return err
	}

	txn.updates.PushFront(update)
	return nil
}

func (txn *transaction) updateRoot(op storage.PatchOp, value interface{}) error {

	if op == storage.RemoveOp {
		return invalidPatchError(rootCannotBeRemovedMsg)
	}

	// the root '/' path must be a valid JSON object
	if _, ok := value.(map[string]interface{}); !ok {
		return invalidPatchError(rootMustBeObjectMsg)
	}

	return nil
}

type update struct {
	path   storage.Path // data path modified by the update
	remove bool         // indicates if this update removes the value at path
	value  interface{}  // value to add/replace at path (ignored if remove is true)
}
