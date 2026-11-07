package lazy

import (
	"gocolumn/expr"
)

// Var computes sample variance (ddof=1), ignoring NULLs.
func Var(column, alias string) expr.Aggregate { return expr.Var(column, alias) }

// Std computes sample standard deviation (ddof=1), ignoring NULLs.
func Std(column, alias string) expr.Aggregate { return expr.Std(column, alias) }

// VarPopulation computes population variance (ddof=0).
func VarPopulation(column, alias string) expr.Aggregate {
	return expr.VarPopulation(column, alias)
}

// StdPopulation computes population standard deviation (ddof=0).
func StdPopulation(column, alias string) expr.Aggregate {
	return expr.StdPopulation(column, alias)
}

// First retains the first logical input row, including NULL.
func First(column, alias string) expr.Aggregate { return expr.First(column, alias) }

// Last retains the last logical input row, including NULL.
func Last(column, alias string) expr.Aggregate { return expr.Last(column, alias) }
