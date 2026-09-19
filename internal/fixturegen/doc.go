// Package fixturegen generates deterministic columnar fixture datasets from a
// seeded, versioned specification. It exists so benchmarks and correctness
// tests can share reproducible data with controlled row counts, value
// distributions, distinct cardinalities, NULL rates, and NaN rates, and can
// persist that data as Parquet or consume it directly as an in-memory batch.
package fixturegen
