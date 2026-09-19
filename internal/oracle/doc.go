// Package oracle implements a deliberately simple, correctness-first
// row-at-a-time reference evaluator used as a differential-testing oracle for
// the GoLume columnar engine.
//
// The oracle trades every performance concern for obviousness: tables are row
// slices, grouping uses a Go map, and sorting is sort.SliceStable. A query is
// evaluated against the oracle and against the real engine (through the lazy
// package API and an in-memory source built from the same table); the results
// must agree exactly under the rules documented by CompareTables.
//
// The supported bounded subset mirrors what the engine can execute through the
// lazy API:
//
//   - in-memory tables with int64, float64, string, and bool columns, all
//     optionally NULL;
//   - Filter with column/literal comparisons (=, <, <=, >, >=, and != written
//     as NOT (a = b) because the engine expression AST has no dedicated
//     not-equals node), AND/OR/NOT, and IS NULL / IS NOT NULL, all under
//     strict SQL three-valued logic: a predicate that evaluates to NULL does
//     not select the row;
//   - Project (column selection/reordering, explicit renames, same-type or
//     numerically coerced + - * / arithmetic, and string concatenation) with
//     NULL propagation;
//   - GroupBy with one or two keys and SUM, COUNT(*), COUNT(col), MIN, MAX,
//     AVG aggregates; SUM/MIN/MAX/AVG ignore NULL inputs and produce SQL NULL
//     for groups with no non-NULL input, while NULL keys form their own group;
//   - Sort with multiple keys, ascending/descending, nulls first/last, stable
//     on the input order (NULL placement is direction-independent, matching
//     physical/order.go); TopK is stable-sort followed by Limit;
//   - Limit preserving input order.
//
// Engine-imposed restrictions that the random query generator respects are
// documented on the generator: aggregate value columns must be int64 and all
// value aggregates of one query share a single column, group keys are int64 or
// string (string only as a single key), bool columns are neither groupable nor
// orderable, and checked int64 arithmetic must not overflow.
package oracle
