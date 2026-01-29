# Changelog

SnowID Go is a high-performance, distributed-ready Snowflake ID generator for Go. It generates 64-bit, time-sorted, unique identifiers with zero allocations and thread-safe implementation.

## 1.0.0 (2026-01-28)

### Features

- ❄️ **Initial Release**: Core implementation of Snowflake ID generation.
- ⚡️ **Performance**: High-performance implementation using atomic operations and minimizing allocations.
- 🔄 **Flexibility**: Support for custom epochs and machine IDs.
- 🌐 **Distributed**: Designed for distributed systems with 10-bit node ID support.
- 📈 **Improved Types**: Migrated ID type to `uint64` for better compatibility and range ([768da2f](https://github.com/qeeqez/snowid-go/commit/768da2fea9758212d86c3bafda2d4320d4810be8)).
- 🔒 **Concurrency**: Fully thread-safe implementation.
- 📚 **Documentation**: Added comprehensive README and examples.

### Bug Fixes

- 🛡️ **Race Conditions**: Resolved multiple race conditions in concurrent generation ([3f64f4a](https://github.com/qeeqez/snowid-go/commit/3f64f4a97e62fbe1c56a61e186551a4b3ec99786), [797db2d](https://github.com/qeeqez/snowid-go/commit/797db2d59afb358c29db2f34540608ff3be443a8)).
- 🎯 **Precision**: Fixed edge cases in timestamp and sequence handling.
- 🔄 **Stability**: Resolved infinite loop issues when using mock time and removed redundant UTC calls ([2dc837e](https://github.com/qeeqez/snowid-go/commit/2dc837edf840533e9cf6b39657829f08b37d346d)).
- 🔍 **Security**: Suppressed false positive integer overflow warnings ([26f06b0](https://github.com/qeeqez/snowid-go/commit/26f06b0a88c55e9c64cd5a1af7d664660ef5da50)).

### Chores & CI

- 🏗️ **Infrastructure**: Setup GitHub Actions for build, test, and release ([3a225d7](https://github.com/qeeqez/snowid-go/commit/3a225d7)).
- ✨ **Quality**: Implemented coverage reporting and linting ([cf969fb](https://github.com/qeeqez/snowid-go/commit/cf969fb)).
- 📦 **Dependencies**: Regular updates to Go version and GitHub Actions.
