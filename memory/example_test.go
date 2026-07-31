package memory_test

import (
	"context"
	"fmt"

	"github.com/thalesfsp/dal/v3/memory"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
)

// book is the document used throughout the examples below.
type book struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

// Example_createAndRetrieve stores a document under an id and reads it back.
func Example_createAndRetrieve() {
	ctx := context.Background()

	s, err := memory.New(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	if _, err := s.Create(ctx, "book-dune", "", &book{ID: "book-dune", Title: "Dune"}, &create.Create{}); err != nil {
		fmt.Println(err)
		return
	}

	var got book

	if err := s.Retrieve(ctx, "book-dune", "", &got, &retrieve.Retrieve{}); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(got.ID, got.Title)

	// Output:
	// book-dune Dune
}

// Example_list shows that List's Search field is a glob matched against ids,
// not a value filter, and that Sort gives callers a stable result order.
func Example_list() {
	ctx := context.Background()

	s, err := memory.New(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	books := []*book{
		{ID: "book-dune", Title: "Dune"},
		{ID: "book-hobbit", Title: "The Hobbit"},
		{ID: "movie-arrival", Title: "Arrival"},
	}

	for _, b := range books {
		if _, err := s.Create(ctx, b.ID, "", b, &create.Create{}); err != nil {
			fmt.Println(err)
			return
		}
	}

	var result memory.ResponseList[book]

	if err := s.List(ctx, "", &result, &list.List{
		Search: "book-*",
		Sort:   customsort.SortSlice{{"id", customsort.Asc}},
	}); err != nil {
		fmt.Println(err)
		return
	}

	for _, item := range result.Items {
		fmt.Println(item.ID, item.Title)
	}

	// Output:
	// book-dune Dune
	// book-hobbit The Hobbit
}

// Example_count counts the ids matching a glob pattern.
func Example_count() {
	ctx := context.Background()

	s, err := memory.New(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	ids := []string{"book-dune", "book-hobbit", "movie-arrival"}

	for _, id := range ids {
		if _, err := s.Create(ctx, id, "", &book{ID: id}, &create.Create{}); err != nil {
			fmt.Println(err)
			return
		}
	}

	n, err := s.Count(ctx, "", &count.Count{Search: "book-*"})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(n)

	// Output:
	// 2
}

// Example_delete removes a document, then shows that retrieving it fails
// with a not-found error.
func Example_delete() {
	ctx := context.Background()

	s, err := memory.New(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	if _, err := s.Create(ctx, "book-dune", "", &book{ID: "book-dune", Title: "Dune"}, &create.Create{}); err != nil {
		fmt.Println(err)
		return
	}

	if err := s.Delete(ctx, "book-dune", "", &delete.Delete{}); err != nil {
		fmt.Println(err)
		return
	}

	var got book

	err = s.Retrieve(ctx, "book-dune", "", &got, &retrieve.Retrieve{})
	fmt.Println(err)

	// Output:
	// retrieve not found
}
