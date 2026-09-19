// Package proptest provides property-test scaffolding and a multi-backend
// consistency harness for the GoLume execution engine. It offers seeded
// table generators (int64, float64, string, and bool columns with NULL
// support), reusable invariant checkers for relational results, and a
// harness that executes one logical query on several kernel backends and
// requires row-by-row identical output, including NULL bitmaps.
package proptest
