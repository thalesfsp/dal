package logging

import (
	"context"
	"maps"
	"sync"

	"github.com/thalesfsp/dal/v3/tracing"
	"github.com/thalesfsp/sypl/v2"
	"github.com/thalesfsp/sypl/v2/fields"
	"github.com/thalesfsp/sypl/v2/formatter"
	"github.com/thalesfsp/sypl/v2/level"
	"github.com/thalesfsp/sypl/v2/output"
	"github.com/thalesfsp/sypl/v2/processor"
)

//////
// Vars, consts, and types.
//////

// Singleton.
var (
	once            sync.Once
	singletonLogger *Logger
)

// Logger is the application logger.
type Logger struct {
	*sypl.Sypl
}

//////
// Exported functionalities.
//////

// Get returns a setup logger, or set it up. Default level is `ERROR`.
//
// All messages will be directed to StdOut unless in case of ERROR level, which
// will be directed to StdErr.
//
// NOTE: Use `SYPL_LEVEL` env var to overwrite the max level.
func Get() *Logger {
	once.Do(func() {
		// Setup logger with default sane values. Default outputs: stdout, and
		// stderr.
		l := sypl.New(
			"dal",
			output.Console(
				level.Error,
				processor.MuteBasedOnLevel(level.Fatal, level.Error),
				processor.ChangeFirstCharCase(processor.Lowercase),
			).SetFormatter(formatter.Text()),
		)

		//////
		// Set singleton.
		//////

		singletonLogger = &Logger{l}

		//////
		// Notify end of logger setup.
		//////

		l.Debuglnf("global logger setup, outputs: %s", l.GetOutputs())
	})

	return singletonLogger
}

// ToAPM adds the installed tracing.Tracer's log-correlation fields (e.g.
// `trace.id`) to f. With the default no-op tracer it adds nothing.
func ToAPM(ctx context.Context, f fields.Fields) fields.Fields {
	if f == nil {
		f = fields.Fields{}
	}

	maps.Copy(f, tracing.Get().LogFields(ctx))

	return f
}
