package oracle

import (
	"fmt"

	"gocolumn/expr"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

// memoryType maps an oracle kind to the engine's logical type.
func (k Kind) memoryType() memory.DataType {
	switch k {
	case KindInt64:
		return memory.TypeInt64
	case KindUint64:
		return memory.TypeUint64
	case KindFloat64:
		return memory.TypeFloat64
	case KindString:
		return memory.TypeString
	case KindBool:
		return memory.TypeBool
	default:
		return memory.TypeInvalid
	}
}

// MemorySchema converts the table column list to an engine schema. Every
// field is nullable; string fields use the view encoding expected by the
// in-memory source.
func (t Table) MemorySchema() memory.Schema {
	fields := make([]memory.Field, len(t.Columns))
	for i, c := range t.Columns {
		fields[i] = memory.Field{Name: c.Name, Type: c.Kind.memoryType(), Nullable: true}
		if c.Kind == KindString {
			fields[i].Encoding = memory.EncodingView
		}
	}
	return memory.NewSchema(fields...)
}

// Source builds an immutable in-memory engine source holding the table,
// split into row batches of at most batchRows rows (batchRows <= 0 produces a
// single batch). Multiple batches exercise the engine's multi-partition scan,
// parallel aggregation merge, and chunked result paths.
func (t Table) Source(batchRows int) (*inmemory.Source, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if batchRows <= 0 {
		batchRows = len(t.Rows)
	}
	if batchRows == 0 {
		batchRows = 1
	}
	var batches []*memory.Batch
	for start := 0; start < len(t.Rows) || batches == nil; start += batchRows {
		end := start + batchRows
		if end > len(t.Rows) {
			end = len(t.Rows)
		}
		batches = append(batches, t.batch(start, end))
		if end == len(t.Rows) {
			break
		}
	}
	return inmemory.New(t.MemorySchema(), batches...)
}

// batch materializes rows [start, end) as one columnar engine batch.
func (t Table) batch(start, end int) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = end - start
	for col, c := range t.Columns {
		rows := t.Rows[start:end]
		switch c.Kind {
		case KindInt64:
			v := &memory.Int64Vector{Values: make([]int64, len(rows)), Encoding: memory.EncodingPlain}
			for i, row := range rows {
				v.Values[i] = row[col].I64
			}
			v.Validity = validityBitmap(rows, col)
			b.Int64s[c.Name] = v
		case KindUint64:
			v := &memory.Uint64Vector{Values: make([]uint64, len(rows)), Encoding: memory.EncodingPlain}
			for i, row := range rows {
				v.Values[i] = row[col].U64
			}
			v.Validity = validityBitmap(rows, col)
			b.Uint64s[c.Name] = v
		case KindFloat64:
			v := &memory.Float64Vector{Values: make([]float64, len(rows)), Encoding: memory.EncodingPlain}
			for i, row := range rows {
				v.Values[i] = row[col].F64
			}
			v.Validity = validityBitmap(rows, col)
			b.Float64s[c.Name] = v
		case KindString:
			v := &memory.StringViewVector{Offsets: make([]uint32, len(rows)+1), Encoding: memory.EncodingView}
			for i, row := range rows {
				v.Data = append(v.Data, row[col].Str...)
				v.Offsets[i+1] = uint32(len(v.Data))
			}
			v.Validity = validityBitmap(rows, col)
			b.Strings[c.Name] = v
		case KindBool:
			// The engine's physical representation of bool is a uint32 0/1
			// vector with a TypeBool schema field.
			v := &memory.Uint32Vector{Values: make([]uint32, len(rows)), Encoding: memory.EncodingPlain}
			for i, row := range rows {
				if row[col].Bool {
					v.Values[i] = 1
				}
			}
			v.Validity = validityBitmap(rows, col)
			b.Uint32s[c.Name] = v
		}
	}
	return b
}

// validityBitmap returns nil when every row is valid, or a bitmap with the
// valid rows set.
func validityBitmap(rows [][]Value, col int) memory.Bitmap {
	var bitmap memory.Bitmap
	for i, row := range rows {
		if row[col].Null {
			if bitmap == nil {
				bitmap = memory.EnsureBitmap(nil, len(rows))
				for j := 0; j < i; j++ {
					memory.Set(bitmap, j)
				}
			}
			continue
		}
		if bitmap != nil {
			memory.Set(bitmap, i)
		}
	}
	return bitmap
}

// Apply translates the oracle pipeline into lazy frame operations. The
// translation is one-to-one per operator, so oracle.Query.Execute and the
// engine see semantically identical plans.
func (q Query) Apply(frame lazy.LazyFrame) lazy.LazyFrame {
	for _, op := range q.Ops {
		switch o := op.(type) {
		case FilterOp:
			frame = frame.Filter(o.Predicate)
		case SelectOp:
			frame = frame.Select(o.Columns...)
		case ProjectOp:
			if o.IncludeInput {
				frame = frame.WithColumns(o.Exprs...)
			} else {
				frame = frame.SelectExprs(o.Exprs...)
			}
		case GroupByOp:
			frame = frame.GroupBy(o.Keys...).Agg(o.Aggregates...)
		case SortOp:
			frame = frame.Sort(lazySortKeys(o.Keys)...)
		case TopKOp:
			frame = frame.TopK(o.K, lazySortKeys(o.Keys)...)
		case LimitOp:
			frame = frame.Limit(o.N)
		default:
			panic(fmt.Sprintf("oracle: no lazy translation for %T", op))
		}
	}
	return frame
}

func lazySortKeys(keys []SortKey) []lazy.SortExpr {
	out := make([]lazy.SortExpr, len(keys))
	for i, k := range keys {
		out[i] = lazy.SortExpr{Expr: expr.Col(k.Column), Descending: k.Descending, NullsLast: k.NullsLast}
	}
	return out
}

// TableFromFrame flattens a materialized engine result into an oracle table.
// Supported logical types are exactly the oracle subset plus the uint64
// columns produced by COUNT aggregates.
func TableFromFrame(frame lazy.DataFrame) (Table, error) {
	schema := frame.Schema()
	out := Table{Columns: make([]Column, len(schema.Fields))}
	rows := frame.Height()
	columns := make([][]Value, len(schema.Fields))
	for i, field := range schema.Fields {
		out.Columns[i] = Column{Name: field.Name, Kind: kindFromMemoryType(field.Type)}
		series, err := frame.Column(field.Name)
		if err != nil {
			return Table{}, fmt.Errorf("oracle: extract column %q: %w", field.Name, err)
		}
		cells, err := extractSeries(series)
		if err != nil {
			return Table{}, fmt.Errorf("oracle: extract column %q: %w", field.Name, err)
		}
		if int64(len(cells)) != rows {
			return Table{}, fmt.Errorf("oracle: column %q produced %d cells for %d rows", field.Name, len(cells), rows)
		}
		columns[i] = cells
	}
	for row := int64(0); row < rows; row++ {
		r := make([]Value, len(columns))
		for col := range columns {
			r[col] = columns[col][row]
		}
		out.Rows = append(out.Rows, r)
	}
	return out, nil
}

// kindFromMemoryType maps an engine logical type to an oracle kind.
func kindFromMemoryType(t memory.DataType) Kind {
	switch t {
	case memory.TypeInt64:
		return KindInt64
	case memory.TypeUint64:
		return KindUint64
	case memory.TypeFloat64:
		return KindFloat64
	case memory.TypeString:
		return KindString
	case memory.TypeBool:
		return KindBool
	default:
		return KindInvalid
	}
}

func extractSeries(s lazy.Series) ([]Value, error) {
	switch s.Type() {
	case memory.TypeInt64:
		chunks, err := s.Int64Chunks()
		if err != nil {
			return nil, err
		}
		var out []Value
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				if valid {
					out = append(out, Int64(v))
				} else {
					out = append(out, Null(KindInt64))
				}
			}
		}
		return out, nil
	case memory.TypeUint64:
		chunks, err := s.Uint64Chunks()
		if err != nil {
			return nil, err
		}
		var out []Value
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				if valid {
					out = append(out, Uint64(v))
				} else {
					out = append(out, Null(KindUint64))
				}
			}
		}
		return out, nil
	case memory.TypeFloat64:
		chunks, err := s.Float64Chunks()
		if err != nil {
			return nil, err
		}
		var out []Value
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				if valid {
					out = append(out, Float64(v))
				} else {
					out = append(out, Null(KindFloat64))
				}
			}
		}
		return out, nil
	case memory.TypeString:
		chunks, err := s.StringChunks()
		if err != nil {
			return nil, err
		}
		var out []Value
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				if valid {
					out = append(out, String(v))
				} else {
					out = append(out, Null(KindString))
				}
			}
		}
		return out, nil
	case memory.TypeBool:
		chunks, err := s.Uint32Chunks()
		if err != nil {
			return nil, err
		}
		var out []Value
		for _, c := range chunks {
			for i := 0; i < c.Len(); i++ {
				v, valid, err := c.Value(i)
				if err != nil {
					return nil, err
				}
				if valid {
					out = append(out, Bool(v != 0))
				} else {
					out = append(out, Null(KindBool))
				}
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("oracle: cannot extract series of type %s", s.Type())
	}
}
