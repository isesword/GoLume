package expr

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
)

// Var computes sample variance (ddof=1), ignoring NULLs.
func Var(column, alias string) Aggregate { return Aggregate{AggVar, column, alias} }

// Std computes sample standard deviation (ddof=1), ignoring NULLs.
func Std(column, alias string) Aggregate { return Aggregate{AggStd, column, alias} }

// VarPopulation computes population variance (ddof=0).
func VarPopulation(column, alias string) Aggregate { return Aggregate{AggVarPopulation, column, alias} }

// StdPopulation computes population standard deviation (ddof=0).
func StdPopulation(column, alias string) Aggregate { return Aggregate{AggStdPopulation, column, alias} }

// First retains the first logical input row, including NULL.
func First(column, alias string) Aggregate { return Aggregate{AggFirst, column, alias} }

// Last retains the last logical input row, including NULL.
func Last(column, alias string) Aggregate { return Aggregate{AggLast, column, alias} }

// IsStatistics reports whether kind uses fixed-size statistics state.
func IsStatistics(kind AggregateKind) bool { return kind >= AggVar && kind <= AggLast }

// StatisticsType validates an input and returns its statistics result type.
func StatisticsType(input memory.DataType, kind AggregateKind) (memory.DataType, error) {
	switch kind {
	case AggVar, AggStd, AggVarPopulation, AggStdPopulation:
		if _, err := aggregate.NewMomentsState(input); err != nil {
			return 0, err
		}
		if input == memory.TypeFloat32 {
			return memory.TypeFloat32, nil
		}
		return memory.TypeFloat64, nil
	case AggFirst, AggLast:
		if input == memory.TypeString || input == memory.TypeBinary {
			return input, nil
		}
		if _, err := aggregate.NewEndpointState(input); err != nil {
			return 0, err
		}
		return input, nil
	}
	return 0, fmt.Errorf("unsupported statistics kind %d", kind)
}
