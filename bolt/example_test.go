package bolt_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/thalesfsp/dal/v2/bolt"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
)

// user is the example payload stored and retrieved below.
type user struct {
	Name string `json:"name"`
}

// newExampleStorage opens a throwaway bbolt database under a fresh temp
// directory and returns it along with a cleanup func. Example functions
// can't take a *testing.T, so setup failures — which would never happen in
// a working environment — panic instead of calling t.Fatal.
func newExampleStorage() (*bolt.Bolt, func()) {
	dir, err := os.MkdirTemp("", "dal-bolt-example-*")
	if err != nil {
		panic(err)
	}

	str, err := bolt.New(context.Background(), filepath.Join(dir, "example.db"), nil)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			panic(rmErr)
		}

		panic(err)
	}

	return str, func() {
		if err := str.Close(); err != nil {
			panic(err)
		}

		if err := os.RemoveAll(dir); err != nil {
			panic(err)
		}
	}
}

// Example_createAndRetrieve stores a document under an id and reads it back
// — the "Set" then "Get" round-trip.
func Example_createAndRetrieve() {
	ctx := context.Background()

	str, cleanup := newExampleStorage()
	defer cleanup()

	if _, err := str.Create(ctx, "user-1", "users", &user{Name: "Ada"}, &create.Create{}); err != nil {
		panic(err)
	}

	var got user
	if err := str.Retrieve(ctx, "user-1", "users", &got, &retrieve.Retrieve{}); err != nil {
		panic(err)
	}

	fmt.Println(got.Name)
	// Output: Ada
}

// Example_list filters a bucket's keys with a glob Search pattern. Matches
// come back in bbolt's cursor order (lexicographic by key); they're sorted
// here anyway so the example stays correct even if that ever changes.
func Example_list() {
	ctx := context.Background()

	str, cleanup := newExampleStorage()
	defer cleanup()

	for _, id := range []string{"user-1", "user-2", "order-1"} {
		if _, err := str.Create(ctx, id, "records", &user{Name: id}, &create.Create{}); err != nil {
			panic(err)
		}
	}

	var out bolt.ResponseList[user]
	if err := str.List(ctx, "records", &out, &list.List{Search: "user-*"}); err != nil {
		panic(err)
	}

	names := make([]string, 0, len(out.Items))
	for _, item := range out.Items {
		names = append(names, item.Name)
	}

	sort.Strings(names)

	fmt.Println(names)
	// Output: [user-1 user-2]
}

// Example_count reports how many keys in a bucket match a Search glob.
func Example_count() {
	ctx := context.Background()

	str, cleanup := newExampleStorage()
	defer cleanup()

	for i := range 3 {
		id := fmt.Sprintf("item-%d", i)

		if _, err := str.Create(ctx, id, "items", &user{Name: id}, &create.Create{}); err != nil {
			panic(err)
		}
	}

	n, err := str.Count(ctx, "items", &count.Count{Search: "*"})
	if err != nil {
		panic(err)
	}

	fmt.Println(n)
	// Output: 3
}

// Example_delete removes a document by id; retrieving it afterward fails.
func Example_delete() {
	ctx := context.Background()

	str, cleanup := newExampleStorage()
	defer cleanup()

	if _, err := str.Create(ctx, "temp-1", "items", &user{Name: "temp"}, &create.Create{}); err != nil {
		panic(err)
	}

	if err := str.Delete(ctx, "temp-1", "items", &delete.Delete{}); err != nil {
		panic(err)
	}

	err := str.Retrieve(ctx, "temp-1", "items", &user{}, &retrieve.Retrieve{})

	fmt.Println(err != nil)
	// Output: true
}
