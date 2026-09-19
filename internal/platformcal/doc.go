// Package platformcal measures machine-specific strategy constants and
// persists them as a versioned platform profile. The engine's shipped
// thresholds — the aggregation strategy crossover, the parallel radix join
// minimums, and the worker scaling knee — were calibrated once on a single
// validation host and are wrong on different hardware. This package
// re-derives them on the current machine from deterministic micro-benchmarks
// so strategy selection stays evidence-based after a hardware change.
//
// Consumers load the profile through costmodel.LoadPlatformThresholds and
// fall back to the shipped constants when no valid profile exists, so a
// missing or corrupt profile never changes behavior.
package platformcal
