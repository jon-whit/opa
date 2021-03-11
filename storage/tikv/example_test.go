package tikv_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/open-policy-agent/opa/storage"
	"github.com/open-policy-agent/opa/storage/tikv"
	"github.com/pingcap/tidb/kv"
)

func Example_Read() {

}

func Example_Write() {

	// Initialize context for the example. Normally the caller would obtain the
	// context from an input parameter or instantiate their own.
	ctx := context.Background()

	// Define some json data to initialize the OPA store with.
	exampleData := `
    {
        "users": [
            {
                "name": "alice",
                "color": "red",
                "likes": ["clouds", "ships"]
            },
            {
                "name": "burt",
                "likes": ["cheese", "wine"]
            }
        ]
    }
    `

	var data map[string]interface{}

	// OPA uses Go's standard JSON library but assumes that numbers have been
	// decoded as json.Number instead of float64. You MUST decode with UseNumber
	// enabled.
	decoder := json.NewDecoder(bytes.NewBufferString(exampleData))
	decoder.UseNumber()

	if err := decoder.Decode(&data); err != nil {
		// Handle error.
	}

	var kvstore kv.Storage // initialize a TiKV Storage instance

	// Instantiate the TiKV store.
	store, err := tikv.NewFromObject(kvstore, data)
	if err != nil {
		// Handle error.
	}

	// Define dummy data to add to the DataStore.
	examplePatch := `{
        "longitude": 82.501389,
        "latitude": -62.338889
    }`

	var patch interface{}

	// See comment above regarding decoder usage.
	decoder = json.NewDecoder(bytes.NewBufferString(examplePatch))
	decoder.UseNumber()

	if err := decoder.Decode(&patch); err != nil {
		// Handle error.
	}

	txn, err := store.NewTransaction(ctx, storage.WriteParams)
	if err != nil {
		// Handle error.
	}

	// Write values into storage and read result.
	err0 := store.Write(ctx, txn, storage.AddOp, storage.MustParsePath("/users/0/location"), patch)
	v1, err1 := store.Read(ctx, txn, storage.MustParsePath("/users/0/location/latitude"))
	err2 := store.Write(ctx, txn, storage.ReplaceOp, storage.MustParsePath("/users/1/color"), "red")

	// Inspect the return values.
	fmt.Println("err0:", err0)
	fmt.Println("v1:", v1)
	fmt.Println("err1:", err1)
	fmt.Println("err2:", err2)

	// Rollback transaction because write failed.
	store.Abort(ctx, txn)

	// Output:
	// err0: <nil>
	// v1: -62.338889
	// err1: <nil>
	// err2: storage_not_found_error: /users/1/color: document does not exist
}
