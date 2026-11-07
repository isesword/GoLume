package engine

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"math"
)

func (p *PreparedQuery) nativeAggregate() bool {
	return p.cq.MultiAggregate != nil && p.cq.MultiAggregate.Native
}

func aggregateStateKind(kind expr.AggregateKind) aggregate.Kind {
	switch kind {
	case expr.AggSum:
		return aggregate.KindSum
	case expr.AggCountRows:
		return aggregate.KindCountRows
	case expr.AggCountValues:
		return aggregate.KindCountValues
	case expr.AggMin:
		return aggregate.KindMin
	case expr.AggMax:
		return aggregate.KindMax
	case expr.AggAvg:
		return aggregate.KindAvg
	}
	return 0
}

func aggregateNeedsValue(layout aggregate.Layout) bool {
	return layout.Sum || layout.Min || layout.Max || layout.Avg
}

// aggregateColumnInfo binds only length and validity for COUNT(column), so
// opaque/nested/string columns need not be materialized into numeric values.
func aggregateColumnInfo(b *memory.Batch, name string, typ memory.DataType, needValue bool) (int, memory.Bitmap, error) {
	switch typ {
	case memory.TypeInt8:
		if v := b.Int8s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeInt16:
		if v := b.Int16s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		if v := b.Int32s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		if v := b.Int64s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeUint8:
		if v := b.Uint8s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeUint16:
		if v := b.Uint16s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeUint32, memory.TypeBool:
		if v := b.Uint32s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeUint64:
		if v := b.Uint64s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeFloat32:
		if v := b.Float32s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeFloat64:
		if v := b.Float64s[name]; v != nil {
			return len(v.Values), v.Validity, nil
		}
	case memory.TypeString, memory.TypeBinary:
		if v := b.Strings[name]; v != nil {
			return v.Len(), v.Validity, nil
		}
		if v := b.DictStrings[name]; v != nil {
			return len(v.Indices), v.Validity, nil
		}
	case memory.TypeList:
		if v := b.StringLists[name]; v != nil {
			return v.Len(), v.Validity, nil
		}
	case memory.TypeStruct:
		if v := b.Structs[name]; v != nil {
			return v.Rows, v.Validity, nil
		}
	}
	if !needValue {
		if v := b.Opaque[name]; v != nil {
			return v.Rows, v.Validity, nil
		}
	}
	return 0, nil, fmt.Errorf("aggregate column %q has no supported %s vector", name, typ)
}

func nativeAggregateBits(b *memory.Batch, name string, typ memory.DataType, layout aggregate.Layout, row int) (uint64, bool) {
	if !aggregateNeedsValue(layout) {
		_, validity, _ := aggregateColumnInfo(b, name, typ, false)
		return 0, validity == nil || memory.IsSet(validity, row)
	}
	switch typ {
	case memory.TypeInt8:
		v := b.Int8s[name]
		return uint64(int64(v.Values[row])), v.IsValid(row)
	case memory.TypeInt16:
		v := b.Int16s[name]
		return uint64(int64(v.Values[row])), v.IsValid(row)
	case memory.TypeInt32:
		v := b.Int32s[name]
		return uint64(int64(v.Values[row])), v.IsValid(row)
	case memory.TypeInt64:
		v := b.Int64s[name]
		return uint64(v.Values[row]), v.IsValid(row)
	case memory.TypeUint8:
		v := b.Uint8s[name]
		return uint64(v.Values[row]), v.IsValid(row)
	case memory.TypeUint16:
		v := b.Uint16s[name]
		return uint64(v.Values[row]), v.IsValid(row)
	case memory.TypeUint32:
		v := b.Uint32s[name]
		return uint64(v.Values[row]), v.IsValid(row)
	case memory.TypeUint64:
		v := b.Uint64s[name]
		return v.Values[row], v.IsValid(row)
	case memory.TypeFloat32:
		v := b.Float32s[name]
		return uint64(math.Float32bits(v.Values[row])), v.IsValid(row)
	case memory.TypeFloat64:
		v := b.Float64s[name]
		return math.Float64bits(v.Values[row]), v.IsValid(row)
	}
	return 0, false
}

func (p *PreparedQuery) nativeRecordValue(record aggregateRecord, index int) (memory.DataType, uint64, bool) {
	m := p.cq.MultiAggregate
	if p.cq.Aggregates[index].Kind == expr.AggQuantile {
		return p.quantileRecordValue(record, index)
	}
	if p.cq.Aggregates[index].Kind == expr.AggNUnique {
		return memory.TypeUint64, distinctRecordCount(record, m.Distinct.OutputLane[index]), true
	}
	if expr.IsStatistics(p.cq.Aggregates[index].Kind) {
		return p.statisticsRecordValue(record, index)
	}
	lane := m.OutputLane[index]
	state := numericAggregateRecordState(record, lane, m.Types[lane], m.Layouts[lane])
	kind := aggregateStateKind(p.cq.Aggregates[index].Kind)
	typ, _ := aggregate.NumericAggregateType(m.Types[lane], kind)
	bits, valid := state.Value(kind)
	return typ, bits, valid
}

func setNativeAggregateOutput(dst *memory.Batch, name string, row int, typ memory.DataType, bits uint64, valid bool) error {
	var validity memory.Bitmap
	switch typ {
	case memory.TypeInt8:
		v := dst.Int8s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Int8", name)
		}
		v.Values[row] = int8(bits)
		validity = v.Validity
	case memory.TypeInt16:
		v := dst.Int16s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Int16", name)
		}
		v.Values[row] = int16(bits)
		validity = v.Validity
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		v := dst.Int32s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Int32", name)
		}
		v.Values[row] = int32(bits)
		validity = v.Validity
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		v := dst.Int64s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Int64", name)
		}
		v.Values[row] = int64(bits)
		validity = v.Validity
	case memory.TypeUint8:
		v := dst.Uint8s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Uint8", name)
		}
		v.Values[row] = uint8(bits)
		validity = v.Validity
	case memory.TypeUint16:
		v := dst.Uint16s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Uint16", name)
		}
		v.Values[row] = uint16(bits)
		validity = v.Validity
	case memory.TypeUint32, memory.TypeBool:
		v := dst.Uint32s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Uint32", name)
		}
		v.Values[row] = uint32(bits)
		validity = v.Validity
	case memory.TypeUint64:
		v := dst.Uint64s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Uint64", name)
		}
		v.Values[row] = bits
		validity = v.Validity
	case memory.TypeFloat32:
		v := dst.Float32s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Float32", name)
		}
		v.Values[row] = math.Float32frombits(uint32(bits))
		validity = v.Validity
	case memory.TypeFloat64:
		v := dst.Float64s[name]
		if v == nil {
			return fmt.Errorf("aggregate output %q missing Float64", name)
		}
		v.Values[row] = math.Float64frombits(bits)
		validity = v.Validity
	default:
		return fmt.Errorf("unsupported aggregate output type %s", typ)
	}
	if valid && validity != nil {
		memory.Set(validity, row)
	}
	return nil
}
