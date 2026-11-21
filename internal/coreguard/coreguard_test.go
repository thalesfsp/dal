package coreguard

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// Every core package, linked into this test binary so the init test sees
	// whatever their imports start.
	_ "github.com/thalesfsp/dal/v3/bolt"
	_ "github.com/thalesfsp/dal/v3/dynamodb"
	_ "github.com/thalesfsp/dal/v3/elasticsearch"
	_ "github.com/thalesfsp/dal/v3/file"
	_ "github.com/thalesfsp/dal/v3/memory"
	_ "github.com/thalesfsp/dal/v3/mongodb"
	_ "github.com/thalesfsp/dal/v3/mysql"
	_ "github.com/thalesfsp/dal/v3/postgres"
	_ "github.com/thalesfsp/dal/v3/redis"
	_ "github.com/thalesfsp/dal/v3/s3"
	_ "github.com/thalesfsp/dal/v3/sftp"
	_ "github.com/thalesfsp/dal/v3/sqlite"
	_ "github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/dal/v3/tracing"
)

const module = "github.com/thalesfsp/dal/v3/"

// corePackages must never link Elastic code of any kind.
var corePackages = []string{
	"storage", "tracing", "memory", "file", "sqlite", "postgres", "mysql",
	"mongodb", "redis", "s3", "sftp", "bolt", "dynamodb",
}

// deps returns `go list -deps` of the given dal packages.
func deps(t *testing.T, pkgs ...string) []string {
	t.Helper()

	args := []string{"list", "-deps"}
	for _, p := range pkgs {
		args = append(args, module+p)
	}

	out, err := exec.CommandContext(t.Context(), "go", args...).CombinedOutput()
	require.NoError(t, err, "go list failed: %s", out)

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	require.Contains(t, lines, module+pkgs[0], "go list output must include the package itself")

	return lines
}

func matching(lines []string, isBad func(string) bool) []string {
	var bad []string

	for _, l := range lines {
		if isBad(l) {
			bad = append(bad, l)
		}
	}

	return bad
}

// The core packages link no Elastic code at all: not APM, not its go-sysinfo
// dependency, not the Elasticsearch client.
func TestCorePackagesLinkNoElasticCode(t *testing.T) {
	for _, pkg := range corePackages {
		t.Run(pkg, func(t *testing.T) {
			bad := matching(deps(t, pkg), func(l string) bool {
				return strings.Contains(l, "elastic")
			})

			assert.Empty(t, bad, "%s must not depend on Elastic code; use the opt-in elasticapm package", pkg)
		})
	}
}

// dal's own elasticsearch adapter keeps its client, but not APM.
func TestElasticsearchAdapterLinksNoAPM(t *testing.T) {
	lines := deps(t, "elasticsearch")

	assert.NotEmpty(t, matching(lines, func(l string) bool {
		return strings.HasPrefix(l, "github.com/elastic/go-elasticsearch/")
	}), "positive control: the adapter must still list its Elasticsearch client")

	assert.Empty(t, matching(lines, func(l string) bool {
		return strings.HasPrefix(l, "go.elastic.co/apm")
	}), "the elasticsearch adapter must not depend on Elastic APM")
}

// The opt-in package is the one place APM is linked — positive control for the
// two checks above: the same probe does see APM where it is.
func TestOptInPackageLinksAPM(t *testing.T) {
	assert.NotEmpty(t, matching(deps(t, "elasticapm"), func(l string) bool {
		return strings.HasPrefix(l, "go.elastic.co/apm")
	}))
}

// With every core package linked, nothing was started at init: no goroutine
// runs Elastic code, and dal's tracer is the no-op one.
func TestNothingStartsAtInit(t *testing.T) {
	buf := make([]byte, 1<<20)
	stacks := string(buf[:runtime.Stack(buf, true)])

	assert.NotContains(t, stacks, "go.elastic.co", "a goroutine started by Elastic APM is running")
	assert.Equal(t, tracing.Noop(), tracing.Get(), "the default tracer must be the no-op one")
}
