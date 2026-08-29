package engine

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/hashagg"
	"gocolumn/memory"
	"math/bits"
)

func (p *PreparedQuery) aggregateInput(batch *memory.Batch) (*memory.Int64Vector, error) {
	if p.cq.ValueColumn == "" {
		return nil, nil
	}
	return batch.Int64(p.cq.ValueColumn)
}

func aggregateValueAt(values *memory.Int64Vector, row int) (int64, bool) {
	if values == nil {
		return 0, false
	}
	return values.Values[row], values.IsValid(row)
}

func (p *PreparedQuery) selection(w *workerWorkspace) (memory.Bitmap, int, error) {
	b := w.batch
	bv, err := p.cq.Predicate.Evaluate(b, &w.expr, p.e.kernels)
	if err != nil {
		return nil, 0, err
	}
	return bv.Truth, memory.CountBitmap(bv.Truth), nil
}

type fixedColumn struct {
	typ      memory.DataType
	validity memory.Bitmap
	i32      []int32
	i64      []int64
	u32      []uint32
	domain   string
}

func fixedFromBatch(b *memory.Batch, g CompiledGroupColumn) (fixedColumn, error) {
	switch g.Type {
	case memory.TypeInt32:
		v, err := b.Int32(g.Name)
		if err != nil {
			return fixedColumn{}, err
		}
		return fixedColumn{typ: g.Type, i32: v.Values, validity: v.Validity}, nil
	case memory.TypeInt64:
		v, err := b.Int64(g.Name)
		if err != nil {
			return fixedColumn{}, err
		}
		return fixedColumn{typ: g.Type, i64: v.Values, validity: v.Validity}, nil
	case memory.TypeUint32:
		v, err := b.Uint32(g.Name)
		if err != nil {
			return fixedColumn{}, err
		}
		return fixedColumn{typ: g.Type, u32: v.Values, validity: v.Validity}, nil
	case memory.TypeString:
		if g.Encoding != memory.EncodingDictionary {
			return fixedColumn{}, fmt.Errorf("plain string is not fixed-width")
		}
		v, err := b.DictionaryString(g.Name)
		if err != nil {
			return fixedColumn{}, err
		}
		if v.DictionaryDomain != g.DictionaryDomain {
			return fixedColumn{}, fmt.Errorf("dictionary domain changed for %q: got %q want %q", g.Name, v.DictionaryDomain, g.DictionaryDomain)
		}
		return fixedColumn{typ: g.Type, u32: v.Indices, validity: v.Validity, domain: v.DictionaryDomain}, nil
	default:
		return fixedColumn{}, fmt.Errorf("unsupported fixed column %s", g.Type)
	}
}
func (c fixedColumn) valid(i int) bool {
	return c.validity == nil || memory.IsSet(c.validity, i)
}

func (c fixedColumn) at(i int) uint64 {
	switch c.typ {
	case memory.TypeInt32:
		return uint64(uint32(c.i32[i]))
	case memory.TypeInt64:
		return uint64(c.i64[i])
	case memory.TypeUint32, memory.TypeString:
		return uint64(c.u32[i])
	default:
		return 0
	}
}

func forSelected(sel memory.Bitmap, rows int, fn func(i int) error) error {
	for wi, word := range sel {
		base := wi << 6
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			i := base + bit
			if i >= rows {
				break
			}
			if err := fn(i); err != nil {
				return err
			}
			word &= word - 1
		}
	}
	return nil
}

func (p *PreparedQuery) decodeFixed(g CompiledGroupColumn, v uint64) KeyPart {
	return p.decodeFixedValid(g, v, true)
}

func (p *PreparedQuery) decodeFixedValid(g CompiledGroupColumn, v uint64, valid bool) KeyPart {
	if !valid {
		return KeyPart{Valid: false, Type: g.Type, DictionaryDomain: g.DictionaryDomain}
	}
	switch g.Type {
	case memory.TypeInt32:
		return KeyPart{Valid: true, Type: g.Type, Int64: int64(int32(uint32(v)))}
	case memory.TypeInt64:
		return KeyPart{Valid: true, Type: g.Type, Int64: int64(v)}
	case memory.TypeUint32:
		return KeyPart{Valid: true, Type: g.Type, Uint64: uint64(uint32(v))}
	case memory.TypeString:
		kp := KeyPart{Valid: true, Type: g.Type, Uint64: uint64(uint32(v)), DictionaryDomain: g.DictionaryDomain}
		if d, ok := p.dicts[g.Name]; ok {
			if b := d.Value(uint32(v)); b != nil {
				kp.String = string(b)
			}
		}
		return kp
	default:
		return KeyPart{Valid: true, Type: g.Type, Uint64: v}
	}
}

func sumCountAggregateState(sum int64, count uint64) aggregate.Int64Result {
	return aggregate.Int64Result{Sum: sum, SumValid: count > 0, CountRows: count}
}

func (p *PreparedQuery) materializeAggregates(st aggregate.Int64Result) []AggregateValue {
	out := make([]AggregateValue, 0, len(p.cq.Aggregates))
	for _, a := range p.cq.Aggregates {
		v := AggregateValue{Alias: a.Alias, Kind: a.Kind}
		switch a.Kind {
		case expr.AggSum:
			v.Valid, v.Int64 = st.SumValid, st.Sum
		case expr.AggCountRows:
			v.Valid, v.Int64 = true, int64(st.CountRows)
		case expr.AggCountValues:
			v.Valid, v.Int64 = true, int64(st.CountValues)
		case expr.AggMin:
			v.Valid, v.Int64 = st.MinValid, st.Min
		case expr.AggMax:
			v.Valid, v.Int64 = st.MaxValid, st.Max
		case expr.AggAvg:
			v.Valid, v.Float64, v.IsFloat = st.AvgValid, st.Avg, true
		}
		out = append(out, v)
	}
	return out
}

func (p *PreparedQuery) materializeI32Result(out *Result, t *hashagg.I32Table) {
	gs := t.Materialize(nil)
	out.Rows = make([]AggregateRow, len(gs))
	for i, g := range gs {
		out.Rows[i] = AggregateRow{Keys: []KeyPart{p.decodeFixedValid(p.cq.GroupBy[0], uint64(uint32(g.Key)), g.Valid)}, Sum: g.Sum, Count: g.Count, Aggregates: p.materializeAggregates(g.State)}
	}
}
func (p *PreparedQuery) materializeU64Result(out *Result, t *hashagg.U64Table) {
	gs := t.Materialize(nil)
	out.Rows = make([]AggregateRow, len(gs))
	gcol := p.cq.GroupBy[0]
	for i, g := range gs {
		out.Rows[i] = AggregateRow{Keys: []KeyPart{p.decodeFixedValid(gcol, g.Key, g.Valid)}, Sum: g.Sum, Count: g.Count, Aggregates: p.materializeAggregates(g.State)}
	}
}

func (p *PreparedQuery) materializeU32Result(out *Result, t *hashagg.U32Table) {
	gs := t.Materialize(nil)
	out.Rows = make([]AggregateRow, len(gs))
	gcol := p.cq.GroupBy[0]
	for i, g := range gs {
		out.Rows[i] = AggregateRow{Keys: []KeyPart{p.decodeFixedValid(gcol, uint64(g.Key), g.Valid)}, Sum: g.Sum, Count: g.Count, Aggregates: p.materializeAggregates(g.State)}
	}
}
func (p *PreparedQuery) materializePairResult(out *Result, t *hashagg.PairTable) {
	gs := t.Materialize(nil)
	out.Rows = make([]AggregateRow, len(gs))
	for i, g := range gs {
		out.Rows[i] = AggregateRow{Keys: []KeyPart{p.decodeFixedValid(p.cq.GroupBy[0], g.Key.A, g.ValidMask&1 != 0), p.decodeFixedValid(p.cq.GroupBy[1], g.Key.B, g.ValidMask&2 != 0)}, Sum: g.Sum, Count: g.Count, Aggregates: p.materializeAggregates(g.State)}
	}
}

func memorySet(b memory.Bitmap, i int) bool { return memory.IsSet(b, i) }
