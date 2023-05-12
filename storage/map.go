package storage

import (
	"context"

	"github.com/thalesfsp/concurrentloop"
	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/create"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/retrieve"
	"github.com/thalesfsp/params/update"
)

//////
// Vars, consts, and types.
//////

// Map is a map of strgs
type Map map[string]IStorage

// ToSlice converts Map to Slice of IStorage.
func (m Map) ToSlice() []IStorage {
	var s []IStorage

	for _, strg := range m {
		s = append(s, strg)
	}

	return s
}

// CountMany returns a slice of int64 with the count of all DALs.
func CountMany(
	ctx context.Context,
	m Map,
	target string,
	prm *count.Count,
	options ...Func[*count.Count],
) ([]int64, error) {
	r, errs := concurrentloop.Map(ctx, m.ToSlice(), func(ctx context.Context, s IStorage) (int64, error) {
		return Count(ctx, s, target, prm, options...)
	})

	if len(errs) > 0 {
		return nil, errs[0]
	}

	return r, nil
}

// CreateMany returns a slice of string with the id of all documents inserted.
func CreateMany[T any](
	ctx context.Context,
	m Map,
	id, target string,
	t T,
	prm *create.Create,
	options ...Func[*create.Create],
) ([]string, error) {
	r, errs := concurrentloop.Map(ctx, m.ToSlice(), func(ctx context.Context, s IStorage) (string, error) {
		return Create(ctx, s, id, target, t, prm, options...)
	})

	if len(errs) > 0 {
		return nil, errs[0]
	}

	return r, nil
}

// DeleteMany deletes all documents concurrently.
func DeleteMany(
	ctx context.Context,
	m Map,
	id, target string,
	prm *delete.Delete,
	options ...Func[*delete.Delete],
) ([]bool, error) {
	r, errs := concurrentloop.Map(ctx, m.ToSlice(), func(ctx context.Context, s IStorage) (bool, error) {
		if err := Delete(ctx, s, id, target, prm, options...); err != nil {
			return false, err
		}

		return true, nil
	})

	if len(errs) > 0 {
		return nil, errs[0]
	}

	return r, nil
}

// ListMany returns a slice of T with all documents. The result of each DAL is
// flattened into a single slice.
func ListMany[T any](
	ctx context.Context,
	m Map,
	target string,
	prm *list.List,
	options ...Func[*list.List],
) ([]T, error) {
	r, errs := concurrentloop.Map(ctx, m.ToSlice(), func(ctx context.Context, s IStorage) ([]T, error) {
		return List[[]T](ctx, s, target, prm, options...)
	})

	if len(errs) > 0 {
		return nil, errs[0]
	}

	return Flatten2D(r), nil
}

// RetrieveMany returns a slice of T from all DALs.
func RetrieveMany[T any](
	ctx context.Context,
	m Map,
	id, target string,
	prm *retrieve.Retrieve,
	options ...Func[*retrieve.Retrieve],
) ([]T, error) {
	r, err := concurrentloop.Map(ctx, m.ToSlice(), func(ctx context.Context, s IStorage) (T, error) {
		return Retrieve[T](ctx, s, id, target, prm, options...)
	})

	if len(err) > 0 {
		return nil, err[0]
	}

	return r, nil
}

// UpdateMany updates all documents concurrently.
func UpdateMany(ctx context.Context,
	m Map,
	id, target string,
	v any,
	prm *update.Update,
	options ...Func[*update.Update],
) ([]bool, error) {
	r, errs := concurrentloop.Map(ctx, m.ToSlice(), func(ctx context.Context, s IStorage) (bool, error) {
		if err := Update(ctx, s, id, target, v, prm, options...); err != nil {
			return false, err
		}

		return true, nil
	})

	if len(errs) > 0 {
		return nil, errs[0]
	}

	return r, nil
}
