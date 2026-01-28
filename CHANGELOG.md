# Changelog

## 1.0.0 (2026-01-28)


### Features

* **core:** migrate ID type to uint64 ([768da2f](https://github.com/qeeqez/snowid-go/commit/768da2fea9758212d86c3bafda2d4320d4810be8))


### Bug Fixes

* avoid race condition ([3f64f4a](https://github.com/qeeqez/snowid-go/commit/3f64f4a97e62fbe1c56a61e186551a4b3ec99786))
* **core:** resolve race conditions and improve test coverage ([797db2d](https://github.com/qeeqez/snowid-go/commit/797db2d59afb358c29db2f34540608ff3be443a8))
* resolve infinite loop with mock time and remove redundant UTC calls ([2dc837e](https://github.com/qeeqez/snowid-go/commit/2dc837edf840533e9cf6b39657829f08b37d346d))
* suppress false positive gosec G115 integer overflow warnings ([26f06b0](https://github.com/qeeqez/snowid-go/commit/26f06b0a88c55e9c64cd5a1af7d664660ef5da50))
