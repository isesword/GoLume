package physical

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"gocolumn/memory"
)

// A binding borrows only the selected vectors for one materialized batch. It
// must not survive that batch's release or a source's next read. The bound
// array and closures grow with key count, never row count. 128 bytes per key
// covers the descriptor, numeric slice closure, and allocator rounding.
const sortBindingKeyBytes = int64(128)

type boundSortKey struct {
	compare    func(a, b int) int
	validity   memory.Bitmap
	descending bool
	nullsLast  bool
}

// sortBindingBytes accounts for arbitrarily many keys, including repeats.
func sortBindingBytes(keys int) (int64, error) {
	if int64(keys) > math.MaxInt64/sortBindingKeyBytes {
		return 0, memory.ErrBudgetExceeded
	}
	return int64(keys) * sortBindingKeyBytes, nil
}

// reserveSortBinding charges the actual batch's key bindings before allocation.
// In particular, empty and one-row inputs need no binding workspace even when
// their source statistics are unknown or conservative upper bounds.
func reserveSortBinding(ctx context.Context, manager memory.Manager, rows, keys int) (memory.Reservation, error) {
	if rows < 2 || keys == 0 || manager == nil {
		return nil, nil
	}
	bytes, err := sortBindingBytes(keys)
	if err != nil {
		return nil, err
	}
	return manager.Reserve(ctx, bytes)
}

func newBatchComparator(batch *memory.Batch, schema memory.Schema, keys []SortKey) (batchComparator, error) {
	// Empty materializations need not expose any physical vectors. One row
	// also needs no comparisons, preserving the old no-read behavior.
	if batch.Rows < 2 {
		return batchComparator{}, nil
	}
	bound := make([]boundSortKey, len(keys))
	for i, key := range keys {
		column, err := bindSortKey(batch, schema, key)
		if err != nil {
			return batchComparator{}, err
		}
		bound[i] = column
	}
	return batchComparator{keys: bound}, nil
}

func bindSortKey(batch *memory.Batch, schema memory.Schema, key SortKey) (boundSortKey, error) {
	field, ok := schema.Field(key.Column)
	if !ok {
		return boundSortKey{}, fmt.Errorf("sort column %q not found", key.Column)
	}
	var bound boundSortKey
	switch field.Type {
	case memory.TypeInt8:
		vector, err := batch.Int8(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeInt16:
		vector, err := batch.Int16(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		vector, err := batch.Int32(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		vector, err := batch.Int64(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeUint8:
		vector, err := batch.Uint8(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeUint16:
		vector, err := batch.Uint16(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeUint32, memory.TypeBool:
		vector, err := batch.Uint32(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeUint64:
		vector, err := batch.Uint64(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeFloat32:
		vector, err := batch.Float32(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeFloat64:
		vector, err := batch.Float64(key.Column)
		if err != nil {
			return boundSortKey{}, err
		}
		bound = bindSortNumbers(vector.Values, vector.Validity)
	case memory.TypeString, memory.TypeBinary:
		if field.Type == memory.TypeString && field.Encoding == memory.EncodingDictionary {
			vector, err := batch.DictionaryString(key.Column)
			if err != nil {
				return boundSortKey{}, err
			}
			bound.validity = vector.Validity
			bound.compare = func(a, b int) int {
				return bytes.Compare(vector.DictionaryValue(vector.Indices[a]), vector.DictionaryValue(vector.Indices[b]))
			}
		} else {
			vector, err := batch.String(key.Column)
			if err != nil {
				return boundSortKey{}, err
			}
			bound.validity = vector.Validity
			bound.compare = func(a, b int) int {
				return bytes.Compare(vector.ValueBytes(a), vector.ValueBytes(b))
			}
		}
	default:
		return boundSortKey{}, fmt.Errorf("sort column %q type %s is not orderable", key.Column, field.Type)
	}
	bound.descending = key.Descending
	bound.nullsLast = key.NullsLast
	return bound, nil
}

type sortNumber interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

func bindSortNumbers[T sortNumber](values []T, validity memory.Bitmap) boundSortKey {
	return boundSortKey{validity: validity, compare: func(a, b int) int {
		left, right := values[a], values[b]
		// NaNs sort after all numeric values, and all NaNs compare equal.
		// The compiler removes these checks for integer specializations.
		if left != left {
			if right != right {
				return 0
			}
			return 1
		}
		if right != right || left < right {
			return -1
		}
		if left > right {
			return 1
		}
		return 0
	}}
}

func (key boundSortKey) compareRows(a, b int) int {
	if key.validity != nil {
		av, bv := memory.IsSet(key.validity, a), memory.IsSet(key.validity, b)
		if !av || !bv {
			if av == bv {
				return 0
			}
			if av == key.nullsLast {
				return -1
			}
			return 1
		}
	}
	comparison := key.compare(a, b)
	if key.descending {
		return -comparison
	}
	return comparison
}
