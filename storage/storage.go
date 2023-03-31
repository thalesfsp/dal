package storage

import (
	"expvar"
	"fmt"

	"github.com/thalesfsp/dal/logging"
	"github.com/thalesfsp/dal/metrics"
	"github.com/thalesfsp/dal/status"
	"github.com/thalesfsp/dal/validation"
	"github.com/thalesfsp/sypl"
	"github.com/thalesfsp/sypl/level"
)

//////
// Vars, consts, and types.
//////

// Type is the type of the entity regarding the framework. It is used to for
// example, to identify the entity in the logs, metrics, and for tracing.
const (
	DefaultMetricCounterLabel = "counter"
	Type                      = "storage"

	// Operation names.
	OperationCount    = "count"
	OperationCreate   = "create"
	OperationDelete   = "delete"
	OperationList     = "list"
	OperationRetrieve = "retrieve"
	OperationUpdate   = "update"
)

// Storage definition.
type Storage struct {
	// Logger.
	Logger sypl.ISypl `json:"-" validate:"required"`

	// Name of the storage type.
	Name string `json:"name" validate:"required,lowercase,gte=1"`

	// Metrics.
	counterCount  *expvar.Int `json:"-" validate:"required,gte=0"`
	counterDelete *expvar.Int `json:"-" validate:"required,gte=0"`
	counterGet    *expvar.Int `json:"-" validate:"required,gte=0"`
	counterList   *expvar.Int `json:"-" validate:"required,gte=0"`
	counterSet    *expvar.Int `json:"-" validate:"required,gte=0"`
	counterUpdate *expvar.Int `json:"-" validate:"required,gte=0"`
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

// GetCounterCount returns the counterCount metric.
func (s *Storage) GetCounterCount() *expvar.Int {
	return s.counterCount
}

// GetCounterDelete returns the counterDelete metric.
func (s *Storage) GetCounterDelete() *expvar.Int {
	return s.counterDelete
}

// GetCounterGet returns the counterGet metric.
func (s *Storage) GetCounterGet() *expvar.Int {
	return s.counterGet
}

// GetCounterList returns the counterList metric.
func (s *Storage) GetCounterList() *expvar.Int {
	return s.counterList
}

// GetCounterSet returns the counterSet metric.
func (s *Storage) GetCounterSet() *expvar.Int {
	return s.counterSet
}

// GetCounterUpdate returns the counterUpdate metric.
func (s *Storage) GetCounterUpdate() *expvar.Int {
	return s.counterUpdate
}

//////
// Factory.
//////

// New returns a new Storage.
func New(name string) (*Storage, error) {
	// Storage's individual logger.
	logger := logging.Get().New(name).SetTags(Type, name)

	a := &Storage{
		Logger: logger,
		Name:   name,

		counterCount:  metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Counted, DefaultMetricCounterLabel)),
		counterDelete: metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Deleted, DefaultMetricCounterLabel)),
		counterGet:    metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Retrieved, DefaultMetricCounterLabel)),
		counterList:   metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Listed, DefaultMetricCounterLabel)),
		counterSet:    metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Created, DefaultMetricCounterLabel)),
		counterUpdate: metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Updated, DefaultMetricCounterLabel)),
	}

	// Validate the storage.
	if err := validation.Validate(a); err != nil {
		return nil, err
	}

	a.GetLogger().PrintlnWithOptions(level.Debug, status.Created.String())

	return a, nil
}
