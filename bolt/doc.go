// Package bolt implements a bbolt (go.etcd.io/bbolt) backed implementation of the storage interface: an embedded, persistent, pure-Go key/value store that fills the gap between the volatile `memory` adapter and the one-file-per-document `file` adapter. Every `target` maps to a bucket inside a single on-disk database file.
package bolt
