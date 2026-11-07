package lazy

import "gocolumn/expr"

// NUnique counts exact distinct values, including NULL, and returns UInt64.
func NUnique(column, alias string) expr.Aggregate { return expr.NUnique(column, alias) }
