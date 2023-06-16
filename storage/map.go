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

//////
// Methods.
//////

// String implements the Stringer interface.
func (m Map) String() string {
	// Iterate over the map and print the storage name.
	// Output: "1, 2, 3"
	var s string

	for k := range m {
		s += k + ", "
	}

	// Remove the last comma.
	if len(s) > 0 {
		s = s[:len(s)-2]
	}

	return s
}

// ToSlice converts Map to Slice of IStorage.
//
//nolint:prealloc
func (m Map) ToSlice() []IStorage {
	var s []IStorage

	for _, strg := range m {
		s = append(s, strg)
	}

	return s
}

//////
// 1:N Operations.
//////

// CountFromMany counts documents concurrently against all DALs in the map.
func CountFromMany(
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
		return nil, errs
	}

	return r, nil
}

// CreateIntoMany creates one document concurrently against all DALs in the map.
func CreateIntoMany[T any](
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
		return nil, errs
	}

	return r, nil
}

// DeleteFromMany deletes one documents concurrently against all DALs in the map.
func DeleteFromMany(
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
		return nil, errs
	}

	return r, nil
}

// ListFromMany lists documents concurrently against all DALs in the map.
//
// NOTE: The results are flattened into a single slice.
func ListFromMany[T any](
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
		return nil, errs
	}

	return Flatten2D(r), nil
}

// RetrieveFromMany retrieves one document concurrently against all DALs in the
// map.
func RetrieveFromMany[T any](
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

// UpdateIntoMany updates one document concurrently against all DALs in the map.
func UpdateIntoMany(
	ctx context.Context,
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
		return nil, errs
	}

	return r, nil
}

//////
// N:1 Operations.
//////

// CreateMany creates many documents concurrently against the same DAL.
func CreateMany[T any](
	ctx context.Context,
	str IStorage,
	target string,
	prm *create.Create,
	itemsMap map[string]T,
) ([]string, error) {
	r, errs := concurrentloop.MapM(ctx, itemsMap, func(ctx context.Context, key string, item T) (string, error) {
		id, err := Create(ctx, str, key, target, item, prm)
		if err != nil {
			return "", err
		}

		return id, nil
	})

	if len(errs) > 0 {
		return nil, errs
	}

	return r, nil
}

// DeleteMany deletes many documents concurrently against the same DAL.
func DeleteMany(
	ctx context.Context,
	str IStorage,
	target string,
	prm *delete.Delete,
	ids ...string,
) ([]bool, error) {
	r, errs := concurrentloop.Map(ctx, ids, func(ctx context.Context, id string) (bool, error) {
		if err := Delete(ctx, str, id, target, prm); err != nil {
			return false, err
		}

		return true, nil
	})

	if len(errs) > 0 {
		return nil, errs
	}

	return r, nil
}

// RetrieveMany retrieves many documents concurrently against the same DAL.
func RetrieveMany[T any](
	ctx context.Context,
	str IStorage,
	target string,
	prm *retrieve.Retrieve,
	ids ...string,
) ([]T, error) {
	r, errs := concurrentloop.Map(ctx, ids, func(ctx context.Context, id string) (T, error) {
		return Retrieve[T](ctx, str, id, target, prm)
	})

	if len(errs) > 0 {
		return nil, errs
	}

	return r, nil
}

// UpdateMany updates many documents concurrently against the same DAL.
func UpdateMany[T any](
	ctx context.Context,
	str IStorage,
	target string,
	prm *update.Update,
	itemsMap map[string]T,
) ([]bool, error) {
	r, errs := concurrentloop.MapM(ctx, itemsMap, func(ctx context.Context, key string, item T) (bool, error) {
		if err := Update(ctx, str, key, target, item, prm); err != nil {
			return false, err
		}

		return true, nil
	})

	if len(errs) > 0 {
		return nil, errs
	}

	return r, nil
}
