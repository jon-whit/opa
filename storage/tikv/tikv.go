// Package tikv implements a version of the policy engine's storage layer
// for the TiKV distributed key-value database.
package tikv

import (
	"container/list"
	"context"
	"fmt"
	"strconv"

	"github.com/open-policy-agent/opa/storage"
	"github.com/open-policy-agent/opa/util"
	"github.com/pingcap/tidb/kv"
)

func New(s kv.Storage) (storage.Store, error) {
	return &store{
		store:    s,
		txn:      nil,
		triggers: map[*handle]storage.TriggerConfig{},
	}, nil
}

// NewFromObject returns a new TiKV store from the supplied data object.
func NewFromObject(s kv.Storage, data map[string]interface{}) (storage.Store, error) {
	store, err := New(s)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	txn, err := store.NewTransaction(ctx, storage.WriteParams)
	if err != nil {
		return nil, err
	}

	if err := store.Write(ctx, txn, storage.AddOp, storage.Path{}, data); err != nil {
		return nil, err
	}

	if err := store.Commit(ctx, txn); err != nil {
		return nil, err
	}

	return store, nil
}

type store struct {
	store    kv.Storage
	txn      *transaction
	triggers map[*handle]storage.TriggerConfig
}

type handle struct {
	store *store
}

func (s *store) NewTransaction(ctx context.Context, params ...storage.TransactionParams) (storage.Transaction, error) {

	txn, err := s.store.Begin()
	if err != nil {
		return nil, err
	}

	return &transaction{
		tikvtxn: txn,
		updates: list.New(),
	}, nil
}

func (s *store) ListPolicies(ctx context.Context, txn storage.Transaction) ([]string, error) {

	underlying, err := s.underlyingTxn(txn)
	if err != nil {
		return nil, err
	}

	return underlying.ListPolicies(ctx)
}

func (s *store) GetPolicy(ctx context.Context, txn storage.Transaction, id string) ([]byte, error) {

	underlying, err := s.underlyingTxn(txn)
	if err != nil {
		return nil, err
	}

	return underlying.GetPolicy(ctx, id)
}

func (s *store) UpsertPolicy(ctx context.Context, txn storage.Transaction, id string, bs []byte) error {

	underlying, err := s.underlyingTxn(txn)
	if err != nil {
		return err
	}

	return underlying.UpsertPolicy(ctx, id, bs)
}

func (s *store) DeletePolicy(ctx context.Context, txn storage.Transaction, id string) error {

	underlying, err := s.underlyingTxn(txn)
	if err != nil {
		return err
	}

	return underlying.DeletePolicy(ctx, id)
}

func (s *store) Read(ctx context.Context, txn storage.Transaction, path storage.Path) (interface{}, error) {

	underlying, err := s.underlyingTxn(txn)
	if err != nil {
		return nil, err
	}

	_ = underlying

	return nil, fmt.Errorf("Not Implemented")
}

func (s *store) Write(ctx context.Context, txn storage.Transaction, op storage.PatchOp, path storage.Path, value interface{}) error {

	underlying, err := s.underlyingTxn(txn)
	if err != nil {
		return err
	}

	ptrval := util.Reference(value)
	if err := util.RoundTrip(ptrval); err != nil {
		return err
	}

	return underlying.Write(op, path, *ptrval)
}

func (s *store) Commit(ctx context.Context, txn storage.Transaction) error {

	underlying, err := s.underlyingTxn(txn)
	if err != nil {
		return err
	}

	if err := underlying.Commit(ctx); err != nil {
		s.Abort(ctx, txn)
		return err
	}

	var event storage.TriggerEvent
	s.runOnCommitTriggers(ctx, txn, event)

	return nil
}

func (s *store) Abort(ctx context.Context, txn storage.Transaction) {

	underlying, err := s.underlyingTxn(txn)
	if err != nil {
		panic(err)
	}

	_ = underlying // use underlying
}

func (s *store) Register(ctx context.Context, txn storage.Transaction, config storage.TriggerConfig) (storage.TriggerHandle, error) {

	h := &handle{s}
	s.triggers[h] = config
	return h, nil
}

func (h *handle) Unregister(ctx context.Context, txn storage.Transaction) {
	delete(h.store.triggers, h)
}

func (s *store) underlyingTxn(txn storage.Transaction) (*transaction, error) {
	underlying, ok := txn.(*transaction)
	if !ok {
		return nil, &storage.Error{
			Code:    storage.InvalidTransactionErr,
			Message: fmt.Sprintf("unexpected transaction type %T", txn),
		}
	}

	return underlying, nil
}

func (s *store) runOnCommitTriggers(ctx context.Context, txn storage.Transaction, event storage.TriggerEvent) {
	for _, t := range s.triggers {
		t.OnCommit(ctx, txn, event)
	}
}

var doesNotExistMsg = "document does not exist"
var rootMustBeObjectMsg = "root must be object"
var rootCannotBeRemovedMsg = "root cannot be removed"
var outOfRangeMsg = "array index out of range"
var arrayIndexTypeMsg = "array index must be integer"

func invalidPatchError(f string, a ...interface{}) *storage.Error {
	return &storage.Error{
		Code:    storage.InvalidPatchErr,
		Message: fmt.Sprintf(f, a...),
	}
}

func notFoundError(path storage.Path) *storage.Error {
	return notFoundErrorHint(path, doesNotExistMsg)
}

func notFoundErrorHint(path storage.Path, hint string) *storage.Error {
	return notFoundErrorf("%v: %v", path.String(), hint)
}

func notFoundErrorf(f string, a ...interface{}) *storage.Error {
	msg := fmt.Sprintf(f, a...)
	return &storage.Error{
		Code:    storage.NotFoundErr,
		Message: msg,
	}
}

func validateArrayIndex(arr []interface{}, s string, path storage.Path) (int, error) {
	idx, err := strconv.Atoi(s)
	if err != nil {
		return 0, notFoundErrorHint(path, arrayIndexTypeMsg)
	}
	if idx < 0 || idx >= len(arr) {
		return 0, notFoundErrorHint(path, outOfRangeMsg)
	}
	return idx, nil
}
