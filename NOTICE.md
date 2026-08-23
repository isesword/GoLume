# Notice

`testdata/bench-1m.parquet` is synthetic benchmark data generated for reproducible correctness/performance tests.

Apache Arrow Go is **not vendored** in this project. Production dependency:

```text
github.com/apache/arrow-go/v18 v18.7.0
```

`tools/offline-poc/` contains only the patch/minimal shim record used to validate Arrow-Go inside an isolated sandbox that could not download transitive dependencies. Those shims are not production dependencies and must not replace the real upstream modules in a normal development or release build.
