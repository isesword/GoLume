package expr

import (
	"fmt"
	"gocolumn/memory"
)

// NUnique counts exact distinct values, including NULL. It returns UInt64.
func NUnique(column, alias string) Aggregate { return Aggregate{AggNUnique, column, alias} }

// DistinctType validates exact canonical equality support.
func DistinctType(input memory.DataType) (memory.DataType, error) {
	switch input {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
		memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32,
		memory.TypeDate64, memory.TypeTime32, memory.TypeTime64, memory.TypeTimestamp,
		memory.TypeDuration, memory.TypeString, memory.TypeBinary:
		return memory.TypeUint64, nil
	}
	return 0, fmt.Errorf("n_unique unsupported input type %s", input)
}
