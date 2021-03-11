package tikv

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/open-policy-agent/opa/storage"
	"github.com/open-policy-agent/opa/util"
	"github.com/pingcap/tidb/store/mockstore"
)

func TestTikvTxnPolicies(t *testing.T) {

	ctx := context.Background()

	mockStore, err := mockstore.NewMockTikvStore()
	if err != nil {
		t.Fatalf("Failed to create mock TiKV store: %v", err)
	}

	store, err := New(mockStore)
	if err != nil {
		t.Fatalf("Failed to initialize the TiKV store: %v", err)
	}

	txn := storage.NewTransactionOrDie(ctx, store, storage.WriteParams)

	if err := store.UpsertPolicy(ctx, txn, "test", []byte("package test")); err != nil {
		t.Fatalf("Unexpected error on policy insert: %v", err)
	}

	if err := store.Commit(ctx, txn); err != nil {
		t.Fatalf("Unexpected commit error: %v", err)
	}

	txn = storage.NewTransactionOrDie(ctx, store, storage.WriteParams)

	if err := store.UpsertPolicy(ctx, txn, "test", []byte("package test\nimport data.foo")); err != nil {
		t.Fatalf("Unexpected error on policy insert/update: %v", err)
	}

	ids, err := store.ListPolicies(ctx, txn)
	expectedIds := []string{"test"}
	if err != nil || !reflect.DeepEqual(expectedIds, ids) {
		t.Fatalf("Expected list policies to return %v but got: %v (err: %v)", expectedIds, ids, err)
	}

	bs, err := store.GetPolicy(ctx, txn, "test")
	expectedBytes := []byte("package test\nimport data.foo")
	if err != nil || !reflect.DeepEqual(expectedBytes, bs) {
		t.Fatalf("Expected get policy to return %v but got: %v (err: %v)", expectedBytes, bs, err)
	}

	if err := store.DeletePolicy(ctx, txn, "test"); err != nil {
		t.Fatalf("Unexpected delete policy error: %v", err)
	}

	if err := store.UpsertPolicy(ctx, txn, "test2", []byte("package test2")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ids, err = store.ListPolicies(ctx, txn)
	expectedIds = []string{"test2"}
	if err != nil || !reflect.DeepEqual(expectedIds, ids) {
		t.Fatalf("Expected list policies to return %v but got: %v (err: %v)", expectedIds, ids, err)
	}

	bs, err = store.GetPolicy(ctx, txn, "test2")
	expectedBytes = []byte("package test2")
	if err != nil || !reflect.DeepEqual(expectedBytes, bs) {
		t.Fatalf("Expected get policy to return %v but got: %v (err: %v)", expectedBytes, bs, err)
	}

	if exist, err := store.GetPolicy(ctx, txn, "test"); !storage.IsNotFound(err) {
		t.Fatalf("Expected NotFoundErr for test but got: %v (err: %v)", exist, err)
	}
}

func TestTikvWrite(t *testing.T) {

	tests := []struct {
		note        string
		op          string
		path        string
		value       string
		expected    error
		getPath     string
		getExpected interface{}
	}{
		// {"add root", "add", "/", `{"a": [1]}`, nil, "/", `{"a": [1]}`},
		// {"add", "add", "/newroot", `{"a": [[1]]}`, nil, "/newroot", `{"a": [[1]]}`},
		// {"add arr", "add", "/a/1", `"x"`, nil, "/a", `[1,"x",2,3,4]`},
		// {"add arr/arr", "add", "/h/1/2", `"x"`, nil, "/h", `[[1,2,3], [2,3,"x",4]]`},
		// {"add obj/arr", "add", "/d/e/1", `"x"`, nil, "/d", `{"e": ["bar", "x", "baz"]}`},
		// {"add obj", "add", "/b/vNew", `"x"`, nil, "/b", `{"v1": "hello", "v2": "goodbye", "vNew": "x"}`},
		// {"add obj (existing)", "add", "/b/v2", `"x"`, nil, "/b", `{"v1": "hello", "v2": "x"}`},

		// {"append arr", "add", "/a/-", `"x"`, nil, "/a", `[1,2,3,4,"x"]`},
		// {"append obj/arr", "add", `/c/0/x/-`, `"x"`, nil, "/c/0/x", `[true,false,"foo","x"]`},
		// {"append arr/arr", "add", `/h/0/-`, `"x"`, nil, `/h/0/3`, `"x"`},
		// {"append err", "remove", "/c/0/x/-", "", invalidPatchError("/c/0/x/-: invalid patch path"), "", nil},
		// {"append err-2", "replace", "/c/0/x/-", "", invalidPatchError("/c/0/x/-: invalid patch path"), "", nil},

		// {"remove", "remove", "/a", "", nil, "/a", notFoundError(storage.MustParsePath("/a"))},
		// {"remove arr", "remove", "/a/1", "", nil, "/a", "[1,3,4]"},
		// {"remove obj/arr", "remove", "/c/0/x/1", "", nil, "/c/0/x", `[true,"foo"]`},
		// {"remove arr/arr", "remove", "/h/0/1", "", nil, "/h/0", "[1,3]"},
		// {"remove obj", "remove", "/b/v2", "", nil, "/b", `{"v1": "hello"}`},

		// {"replace root", "replace", "/", `{"a": [1]}`, nil, "/", `{"a": [1]}`},
		// {"replace", "replace", "/a", "1", nil, "/a", "1"},
		// {"replace obj", "replace", "/b/v1", "1", nil, "/b", `{"v1": 1, "v2": "goodbye"}`},
		// {"replace array", "replace", "/a/1", "999", nil, "/a", "[1,999,3,4]"},

		//{"err: bad root type", "add", "/", "[1,2,3]", invalidPatchError(rootMustBeObjectMsg), "", nil},
		{"err: remove root", "remove", "/", "", invalidPatchError(rootCannotBeRemovedMsg), "", nil},
		// {"err: add arr (non-integer)", "add", "/a/foo", "1", notFoundErrorHint(storage.MustParsePath("/a/foo"), arrayIndexTypeMsg), "", nil},
		// {"err: add arr (non-integer)", "add", "/a/3.14", "1", notFoundErrorHint(storage.MustParsePath("/a/3.14"), arrayIndexTypeMsg), "", nil},
		// {"err: add arr (out of range)", "add", "/a/5", "1", notFoundErrorHint(storage.MustParsePath("/a/5"), outOfRangeMsg), "", nil},
		// {"err: add arr (out of range)", "add", "/a/-1", "1", notFoundErrorHint(storage.MustParsePath("/a/-1"), outOfRangeMsg), "", nil},
		// {"err: add arr (missing root)", "add", "/dead/beef/0", "1", notFoundError(storage.MustParsePath("/dead/beef/0")), "", nil},
		// {"err: add non-coll", "add", "/a/1/2", "1", notFoundError(storage.MustParsePath("/a/1/2")), "", nil},
		// {"err: append (missing)", "add", `/dead/beef/-`, "1", notFoundError(storage.MustParsePath("/dead/beef/-")), "", nil},
		// {"err: append obj/arr", "add", `/c/0/deadbeef/-`, `"x"`, notFoundError(storage.MustParsePath("/c/0/deadbeef/-")), "", nil},
		// {"err: append arr/arr (out of range)", "add", `/h/9999/-`, `"x"`, notFoundErrorHint(storage.MustParsePath("/h/9999/-"), outOfRangeMsg), "", nil},
		// {"err: append append+add", "add", `/a/-/b/-`, `"x"`, notFoundErrorHint(storage.MustParsePath(`/a/-/b/-`), arrayIndexTypeMsg), "", nil},
		// {"err: append arr/arr (non-array)", "add", `/b/v1/-`, "1", notFoundError(storage.MustParsePath("/b/v1/-")), "", nil},
		// {"err: remove missing", "remove", "/dead/beef/0", "", notFoundError(storage.MustParsePath("/dead/beef/0")), "", nil},
		// {"err: remove obj (missing)", "remove", "/b/deadbeef", "", notFoundError(storage.MustParsePath("/b/deadbeef")), "", nil},
		// {"err: replace root (missing)", "replace", "/deadbeef", "1", notFoundError(storage.MustParsePath("/deadbeef")), "", nil},
		// {"err: replace missing", "replace", "/dead/beef/1", "1", notFoundError(storage.MustParsePath("/dead/beef/1")), "", nil},
	}

	ctx := context.Background()

	for i, tc := range tests {

		mockStore, err := mockstore.NewMockTikvStore()
		if err != nil {
			t.Fatalf("Failed to create mock TiKV store: %v", err)
		}

		data := loadSmallTestData()
		store, err := NewFromObject(mockStore, data)
		if err != nil {
			t.Fatalf("Failed to initialize the TiKV store: %v", err)
		}

		// Perform patch and check result
		value := loadExpectedSortedResult(tc.value)

		var op storage.PatchOp
		switch tc.op {
		case "add":
			op = storage.AddOp
		case "remove":
			op = storage.RemoveOp
		case "replace":
			op = storage.ReplaceOp
		default:
			panic(fmt.Sprintf("illegal value: %v", tc.op))
		}

		err = storage.WriteOne(ctx, store, op, storage.MustParsePath(tc.path), value)
		if tc.expected == nil {
			if err != nil {
				t.Errorf("Test case %d (%v): unexpected patch error: %v", i+1, tc.note, err)
				continue
			}
		} else {
			if err == nil {
				t.Errorf("Test case %d (%v): expected patch error, but got nil instead", i+1, tc.note)
				continue
			}
			if !reflect.DeepEqual(err, tc.expected) {
				t.Errorf("Test case %d (%v): expected patch error %v but got: %v", i+1, tc.note, tc.expected, err)
				continue
			}
		}

		if tc.getPath == "" {
			continue
		}
	}
}

func loadExpectedResult(input string) interface{} {
	if len(input) == 0 {
		return nil
	}
	var data interface{}
	if err := util.UnmarshalJSON([]byte(input), &data); err != nil {
		panic(err)
	}
	return data
}

func loadExpectedSortedResult(input string) interface{} {
	data := loadExpectedResult(input)
	switch data := data.(type) {
	case []interface{}:
		return data
	default:
		return data
	}
}

func loadSmallTestData() map[string]interface{} {
	var data map[string]interface{}
	err := util.UnmarshalJSON([]byte(`{
        "a": [1,2,3,4],
        "b": {
            "v1": "hello",
            "v2": "goodbye"
        },
        "c": [{
            "x": [true, false, "foo"],
            "y": [null, 3.14159],
            "z": {"p": true, "q": false}
        }],
        "d": {
            "e": ["bar", "baz"]
        },
		"g": {
			"a": [1, 0, 0, 0],
			"b": [0, 2, 0, 0],
			"c": [0, 0, 0, 4]
		},
		"h": [
			[1,2,3],
			[2,3,4]
		]
    }`), &data)
	if err != nil {
		panic(err)
	}
	return data
}
