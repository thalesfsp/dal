## dal

A highly flexible and extensible storage interface implementation in Go that provides a unified way to interact with different storage backends.

This package implements a generic storage interface that abstracts away the complexities of working with different storage systems. It provides a consistent API for basic CRUD operations while supporting various storage backends like Redis and others. The implementation includes robust error handling, metrics collection, and APM tracing capabilities.

## Features

- Unified Storage Interface: Common interface for multiple storage backends (IStorage)
- Concurrent Operations: Support for both single and multi-storage operations
- Built-in Metrics: Comprehensive metrics tracking for all operations
- APM Integration: Built-in application performance monitoring with distributed tracing
- Extensible Architecture: Easy to implement new storage backends
- Pre/Post Operation Hooks: Customizable hooks for operation lifecycle management
- Type-Safe Operations: Generic type support for type-safe data handling
- Robust Error Handling: Comprehensive error handling with customer error types
- Logging Support: Integrated logging system with different log levels

## Storages

| Package         | Backend                | Notes                                                       |
| --------------- | ---------------------- | ----------------------------------------------------------- |
| `bolt`          | bbolt                  | Embedded, persistent, single file. Holds an exclusive lock.  |
| `dynamodb`      | AWS DynamoDB           |                                                              |
| `elasticsearch` | Elasticsearch          |                                                              |
| `file`          | Local filesystem       | One file per document.                                       |
| `memory`        | In-process map         | Volatile. Cleared when the process exits.                    |
| `mongodb`       | MongoDB                |                                                              |
| `mysql`         | MySQL                  |                                                              |
| `postgres`      | PostgreSQL             |                                                              |
| `redis`         | Redis                  |                                                              |
| `s3`            | AWS S3                 |                                                              |
| `sftp`          | SFTP                   |                                                              |
| `sqlite`        | SQLite                 |                                                              |

`bolt` and `memory` need no external service, so their packages carry runnable
examples. Every other storage is exercised by integration tests that require a
running backend.

## Install

`go get github.com/thalesfsp/dal/v2/storage`

## Contributing

1. Fork
2. Clone
3. Create a branch
4. Make changes following the same standards as the project
5. Run `make ci`
6. Create a merge request
