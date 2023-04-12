package storage

import (
	"context"
	"expvar"
	"fmt"

	"github.com/thalesfsp/dal/internal/customapm"
	"github.com/thalesfsp/dal/internal/logging"
	"github.com/thalesfsp/dal/internal/metrics"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl"
	"github.com/thalesfsp/sypl/level"
	"github.com/thalesfsp/validation"
)

//////
// Vars, consts, and types.
//////

// Type is the type of the entity regarding the framework. It is used to for
// example, to identify the entity in the logs, metrics, and for tracing.
const (
	DefaultMetricCounterLabel = "counter"
	Type                      = "storage"
)

// Storage definition.
type Storage struct {
	// Logger.
	Logger sypl.ISypl `json:"-" validate:"required"`

	// Name of the storage type.
	Name string `json:"name" validate:"required,lowercase,gte=1"`

	// Metrics.
	counterCounted             *expvar.Int `json:"-" validate:"required,gte=0"`
	counterCountedFailed       *expvar.Int `json:"-" validate:"required,gte=0"`
	counterCreated             *expvar.Int `json:"-" validate:"required,gte=0"`
	counterCreatedFailed       *expvar.Int `json:"-" validate:"required,gte=0"`
	counterDeleted             *expvar.Int `json:"-" validate:"required,gte=0"`
	counterDeletedFailed       *expvar.Int `json:"-" validate:"required,gte=0"`
	counterInstantiationFailed *expvar.Int `json:"-" validate:"required,gte=0"`
	counterListed              *expvar.Int `json:"-" validate:"required,gte=0"`
	counterListedFailed        *expvar.Int `json:"-" validate:"required,gte=0"`
	counterPingFailed          *expvar.Int `json:"-" validate:"required,gte=0"`
	counterRetrieved           *expvar.Int `json:"-" validate:"required,gte=0"`
	counterRetrievedFailed     *expvar.Int `json:"-" validate:"required,gte=0"`
	counterUpdate              *expvar.Int `json:"-" validate:"required,gte=0"`
	counterUpdateFailed        *expvar.Int `json:"-" validate:"required,gte=0"`
}

//////
// Implements the IMeta interface.
//////

// GetLogger returns the logger.
func (s *Storage) GetLogger() sypl.ISypl {
	return s.Logger
}

// GetName returns the storage name.
func (s *Storage) GetName() string {
	return s.Name
}

// GetType returns its type.
func (s *Storage) GetType() string {
	return Type
}

// GetCounterCounted returns the metric.
func (s *Storage) GetCounterCounted() *expvar.Int {
	return s.counterCounted
}

// GetCounterCountedFailed returns the metric.
func (s *Storage) GetCounterCountedFailed() *expvar.Int {
	return s.counterCountedFailed
}

// GetCounterDeleted returns the metric.
func (s *Storage) GetCounterDeleted() *expvar.Int {
	return s.counterDeleted
}

// GetCounterDeletedFailed returns the metric.
func (s *Storage) GetCounterDeletedFailed() *expvar.Int {
	return s.counterDeletedFailed
}

// GetCounterRetrieved returns the metric.
func (s *Storage) GetCounterRetrieved() *expvar.Int {
	return s.counterRetrieved
}

// GetCounterRetrievedFailed returns the metric.
func (s *Storage) GetCounterRetrievedFailed() *expvar.Int {
	return s.counterRetrievedFailed
}

// GetCounterListed returns the metric.
func (s *Storage) GetCounterListed() *expvar.Int {
	return s.counterListed
}

// GetCounterListedFailed returns the metric.
func (s *Storage) GetCounterListedFailed() *expvar.Int {
	return s.counterListedFailed
}

// GetCounterPingFailed returns the metric.
func (s *Storage) GetCounterPingFailed() *expvar.Int {
	return s.counterPingFailed
}

// GetCounterCreated returns the metric.
func (s *Storage) GetCounterCreated() *expvar.Int {
	return s.counterCreated
}

// GetCounterCreatedFailed returns the metric.
func (s *Storage) GetCounterCreatedFailed() *expvar.Int {
	return s.counterCreatedFailed
}

// GetCounterUpdated returns the metric.
func (s *Storage) GetCounterUpdated() *expvar.Int {
	return s.counterUpdate
}

// GetCounterUpdatedFailed returns the metric.
func (s *Storage) GetCounterUpdatedFailed() *expvar.Int {
	return s.counterUpdateFailed
}

//////
// Factory.
//////

// New returns a new Storage.
func New(ctx context.Context, name string) (*Storage, error) {
	// Storage's individual logger.
	logger := logging.Get().New(name).SetTags(Type, name)

	a := &Storage{
		Logger: logger,
		Name:   name,

		counterCounted:             metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Counted, DefaultMetricCounterLabel)),
		counterCountedFailed:       metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Counted+"."+status.Failed, DefaultMetricCounterLabel)),
		counterCreated:             metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Created, DefaultMetricCounterLabel)),
		counterCreatedFailed:       metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Created+"."+status.Failed, DefaultMetricCounterLabel)),
		counterDeleted:             metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Deleted, DefaultMetricCounterLabel)),
		counterDeletedFailed:       metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Deleted+"."+status.Failed, DefaultMetricCounterLabel)),
		counterInstantiationFailed: metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, "instantiation."+status.Failed, DefaultMetricCounterLabel)),
		counterListed:              metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Listed, DefaultMetricCounterLabel)),
		counterListedFailed:        metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Listed+"."+status.Failed, DefaultMetricCounterLabel)),
		counterPingFailed:          metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, "ping."+status.Failed, DefaultMetricCounterLabel)),
		counterRetrieved:           metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Retrieved, DefaultMetricCounterLabel)),
		counterRetrievedFailed:     metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Retrieved+"."+status.Failed, DefaultMetricCounterLabel)),
		counterUpdate:              metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Updated, DefaultMetricCounterLabel)),
		counterUpdateFailed:        metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Updated+"."+status.Failed, DefaultMetricCounterLabel)),
	}

	// Validate the storage.
	if err := validation.Validate(a); err != nil {
		return nil, customapm.TraceError(ctx, err, logger, a.counterInstantiationFailed)
	}

	a.GetLogger().PrintlnWithOptions(level.Debug, status.Created.String())

	return a, nil
}
