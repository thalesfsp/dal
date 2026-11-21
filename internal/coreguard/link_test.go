package coreguard

// Every core package, linked into this test binary so TestNothingStartsAtInit
// sees whatever their imports start at init.
import (
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
)
