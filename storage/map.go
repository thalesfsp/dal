package storage

import (
	"context"

	"github.com/thalesfsp/concurrentloop"
	"github.com/thalesfsp/params/create"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/retrieve"
	"github.com/thalesfsp/params/update"
)

//////
// Vars, consts, and types.
//////

// Map is a map of PubSubs
type Map map[string]IStorage

// CreateMany will make all DAL to concurrently create `v`.
func CreateMany(ctx context.Context, m Map, id, target string, v any, prm *create.Create, options ...Func[*create.Create]) (map[string]string, error) {
	var (
		data = map[string]string{}
		errs concurrentloop.Errors
	)

	for _, pubsub := range m {
		id, err := pubsub.Create(ctx, id, target, v, prm, options...)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		data[pubsub.GetName()] = id
	}

	if errs != nil {
		return nil, errs
	}

	return data, nil
}

// DeleteMany will make all DAL to concurrently delete `id`.
func DeleteMany(ctx context.Context, m Map, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error {
	var errs concurrentloop.Errors

	for _, pubsub := range m {
		if err := pubsub.Delete(ctx, id, target, prm, options...); err != nil {
			errs = append(errs, err)

			continue
		}
	}

	if errs != nil {
		return errs
	}

	return nil
}

// ListMany will make all DAL to concurrently list.
func ListMany[T any](ctx context.Context, m Map, target string, prm *list.List, options ...Func[*list.List]) (map[string]T, error) {
	var (
		data = map[string]T{}
		errs concurrentloop.Errors
	)

	for _, pubsub := range m {
		var t T

		if err := pubsub.List(ctx, target, t, prm, options...); err != nil {
			errs = append(errs, err)

			continue
		}

		data[pubsub.GetName()] = t
	}

	if errs != nil {
		return nil, errs
	}

	return data, nil
}

// RetrieveMany will make all DAL to concurrently retrieve `id`.
func RetrieveMany[T any](ctx context.Context, m Map, id, target string, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) (map[string]T, error) {
	var (
		data = map[string]T{}
		errs concurrentloop.Errors
	)

	for _, pubsub := range m {
		var t T

		if err := pubsub.Retrieve(ctx, id, target, t, prm, options...); err != nil {
			errs = append(errs, err)

			continue
		}

		data[pubsub.GetName()] = t
	}

	if errs != nil {
		return nil, errs
	}

	return data, nil
}

// UpdateMany will make all DAL to concurrently update `id` with `v`.
func UpdateMany(ctx context.Context, m Map, id, target string, v any, prm *update.Update, options ...Func[*update.Update]) error {
	var errs concurrentloop.Errors

	for _, pubsub := range m {
		if err := pubsub.Update(ctx, id, target, v, prm, options...); err != nil {
			errs = append(errs, err)

			continue
		}
	}

	if errs != nil {
		return errs
	}

	return nil
}
