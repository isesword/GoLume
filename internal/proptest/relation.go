package proptest

import (
	"fmt"
	"math"
	"strconv"

	"gocolumn/lazy"
	"gocolumn/memory"
)

// Cell is one materialized scalar result value. A NULL cell has Valid false
// and a nil Value. Valid values are one of bool, int32, int64, uint32,
// uint64, float64, or string, chosen by the column's logical type.
type Cell struct {
	Valid bool
	Value any
}

// Relation is a fully materialized query result: schema fields in output
// order plus one row slice per result row. It is the canonical comparison
// currency for invariant checkers and the multi-backend harness.
type Relation struct {
	Fields []memory.Field
	Rows   [][]Cell
}

// ColumnIndex returns the position of column name and whether it exists.
func (r *Relation) ColumnIndex(name string) (int, bool) {
	for i, f := range r.Fields {
		if f.Name == name {
			return i, true
		}
	}
	return -1, false
}

// RelationFromFrame flattens a materialized DataFrame into a Relation.
// Supported logical types are the signed and unsigned integer families,
// float32/float64, bool, and string; other types return an error.
func RelationFromFrame(frame lazy.DataFrame) (*Relation, error) {
	schema := frame.Schema()
	rel := &Relation{Fields: schema.Fields}
	rows := frame.Height()
	rel.Rows = make([][]Cell, 0, rows)
	columns := make([][]Cell, len(schema.Fields))
	for i, field := range schema.Fields {
		series, err := frame.Column(field.Name)
		if err != nil {
			return nil, fmt.Errorf("proptest: extract column %q: %w", field.Name, err)
		}
		cells, err := extractSeries(series)
		if err != nil {
			return nil, fmt.Errorf("proptest: extract column %q: %w", field.Name, err)
		}
		if int64(len(cells)) != rows {
			return nil, fmt.Errorf("proptest: column %q produced %d cells for %d rows", field.Name, len(cells), rows)
		}
		columns[i] = cells
	}
	for row := int64(0); row < rows; row++ {
		out := make([]Cell, len(schema.Fields))
		for col := range columns {
			out[col] = columns[col][row]
		}
		rel.Rows = append(rel.Rows, out)
	}
	return rel, nil
}

func extractSeries(s lazy.Series) ([]Cell, error) {
	n := s.Len()
	out := make([]Cell, 0, n)
	switch {
	case isInt32Family(s.Type()):
		chunks, err := s.Int32Chunks()
		if err != nil {
			return nil, err
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				out = append(out, Cell{Valid: valid, Value: int32(v)})
			}
		}
	case isInt64Family(s.Type()):
		chunks, err := s.Int64Chunks()
		if err != nil {
			return nil, err
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				out = append(out, Cell{Valid: valid, Value: v})
			}
		}
	case isUint32Family(s.Type()):
		chunks, err := s.Uint32Chunks()
		if err != nil {
			return nil, err
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				if s.Type() == memory.TypeBool {
					out = append(out, Cell{Valid: valid, Value: v != 0})
				} else {
					out = append(out, Cell{Valid: valid, Value: v})
				}
			}
		}
	case s.Type() == memory.TypeUint64:
		chunks, err := s.Uint64Chunks()
		if err != nil {
			return nil, err
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				out = append(out, Cell{Valid: valid, Value: v})
			}
		}
	case s.Type() == memory.TypeFloat32 || s.Type() == memory.TypeFloat64:
		chunks, err := s.Float64Chunks()
		if err != nil {
			return nil, err
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				out = append(out, Cell{Valid: valid, Value: v})
			}
		}
	case s.Type() == memory.TypeString:
		chunks, err := s.StringChunks()
		if err != nil {
			return nil, err
		}
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				out = append(out, Cell{Valid: valid, Value: v})
			}
		}
	default:
		return nil, fmt.Errorf("unsupported series type %s", s.Type())
	}
	return out, nil
}

func isInt32Family(t memory.DataType) bool {
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		return true
	}
	return false
}

func isInt64Family(t memory.DataType) bool {
	switch t {
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return true
	}
	return false
}

func isUint32Family(t memory.DataType) bool {
	switch t {
	case memory.TypeBool, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
		return true
	}
	return false
}

// cellsEqual reports exact cell equality, including NULL-ness. Floats compare
// bitwise so NaN equals NaN and -0 differs from +0, matching the harness
// contract that backends must produce bit-identical results.
func cellsEqual(a, b Cell) bool {
	if a.Valid != b.Valid {
		return false
	}
	if !a.Valid {
		return true
	}
	if af, ok := a.Value.(float64); ok {
		bf, ok := b.Value.(float64)
		return ok && math.Float64bits(af) == math.Float64bits(bf)
	}
	return a.Value == b.Value
}

// formatCell renders one cell for diagnostics.
func formatCell(c Cell) string {
	if !c.Valid {
		return "NULL"
	}
	switch v := c.Value.(type) {
	case string:
		return strconv.Quote(v)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// compareValues orders two valid non-float-NaN values of the same dynamic
// type: negative when a < b, zero when equal, positive when a > b.
func compareValues(a, b any) (int, error) {
	switch av := a.(type) {
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 0, typeMismatch(a, b)
		}
		switch {
		case av == bv:
			return 0, nil
		case !av:
			return -1, nil
		default:
			return 1, nil
		}
	case int32:
		bv, ok := b.(int32)
		if !ok {
			return 0, typeMismatch(a, b)
		}
		return compareOrdered(av, bv), nil
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0, typeMismatch(a, b)
		}
		return compareOrdered(av, bv), nil
	case uint32:
		bv, ok := b.(uint32)
		if !ok {
			return 0, typeMismatch(a, b)
		}
		return compareOrdered(av, bv), nil
	case uint64:
		bv, ok := b.(uint64)
		if !ok {
			return 0, typeMismatch(a, b)
		}
		return compareOrdered(av, bv), nil
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0, typeMismatch(a, b)
		}
		return compareOrdered(av, bv), nil
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0, typeMismatch(a, b)
		}
		return compareOrdered(av, bv), nil
	default:
		return 0, fmt.Errorf("proptest: unsupported cell type %T", a)
	}
}

func compareOrdered[T int32 | int64 | uint32 | uint64 | float64 | string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func typeMismatch(a, b any) error {
	return fmt.Errorf("proptest: cell type mismatch %T vs %T", a, b)
}
