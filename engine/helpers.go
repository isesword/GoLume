package engine

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
)

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
