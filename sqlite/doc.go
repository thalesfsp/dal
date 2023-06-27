// Package sqlite implements the sqlite storage. It's powered by `sqlx`, and
// gocu.
//
// `sqlx` is a library which provides a set of extensions on go's standard
// database/sql library. The sqlx versions of sql.DB, sql.TX, sql.Stmt, et al.
// all leave the underlying interfaces untouched, so that their interfaces are
// a superset on the standard ones. This makes it relatively painless to
// integrate existing codebases using database/sql with sqlx.
//
// gocu is a query builder.
package sqlite
