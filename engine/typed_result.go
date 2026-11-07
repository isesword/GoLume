package engine

import (
	"context"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"unsafe"
)

// TypedValue is an exact scalar with explicit dtype and SQL validity. Numeric
// Bits contains two's-complement integer or IEEE floating bits, never a float
// conversion of an unsigned integer. Text owns string or binary data. DictionaryDomain
// denotes a dictionary code in Bits when the source supplied no dictionary;
// AsString then returns false rather than inventing an empty string value.
type TypedValue struct {
	Type             memory.DataType
	Valid            bool
	Bits             uint64
	Text             string
	DictionaryDomain string
}

// AsInt64 returns a valid signed integer or temporal scalar without coercion.
func (v TypedValue) AsInt64() (int64, bool) {
	if !v.Valid {
		return 0, false
	}
	switch v.Type {
	case memory.TypeInt8:
		return int64(int8(v.Bits)), true
	case memory.TypeInt16:
		return int64(int16(v.Bits)), true
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		return int64(int32(v.Bits)), true
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return int64(v.Bits), true
	}
	return 0, false
}

// AsUint64 returns a valid unsigned integer without precision loss.
func (v TypedValue) AsUint64() (uint64, bool) {
	if !v.Valid {
		return 0, false
	}
	switch v.Type {
	case memory.TypeUint8:
		return uint64(uint8(v.Bits)), true
	case memory.TypeUint16:
		return uint64(uint16(v.Bits)), true
	case memory.TypeUint32:
		return uint64(uint32(v.Bits)), true
	case memory.TypeUint64:
		return v.Bits, true
	}
	return 0, false
}

// AsFloat64 returns a floating scalar, widening Float32 exactly when needed.
func (v TypedValue) AsFloat64() (float64, bool) {
	if !v.Valid {
		return 0, false
	}
	switch v.Type {
	case memory.TypeFloat32:
		return float64(math.Float32frombits(uint32(v.Bits))), true
	case memory.TypeFloat64:
		return math.Float64frombits(v.Bits), true
	}
	return 0, false
}

// AsBool returns a valid boolean without numeric coercion.
func (v TypedValue) AsBool() (bool, bool) {
	if !v.Valid || v.Type != memory.TypeBool {
		return false, false
	}
	return v.Bits != 0, true
}

// AsString returns owned string data, or false for NULL or unresolved codes.
func (v TypedValue) AsString() (string, bool) {
	return v.Text, v.Valid && v.Type == memory.TypeString && v.DictionaryDomain == ""
}

// TypedAggregateValue names one exactly typed aggregate result.
type TypedAggregateValue struct {
	Alias string
	Kind  expr.AggregateKind
	Value TypedValue
}

// TypedGroupKey names one exactly typed group-key component.
type TypedGroupKey struct {
	Name  string
	Value TypedValue
}

// TypedAggregateRow is a caller-owned row returned by ExecuteTyped.
type TypedAggregateRow struct {
	Keys       []TypedGroupKey
	Aggregates []TypedAggregateValue
}

// TypedResult contains typed rows and the existing execution summary. Summary.Rows
// is always nil; Rows is the sole materialized row collection. Both the rows and
// their strings remain valid after query/engine Close and hold no budget leases.
type TypedResult struct {
	Rows    []TypedAggregateRow
	Summary Result
}

// ExecuteTyped consumes the same native physical stream as Execute and returns
// exact native dtypes, including UInt64 values above MaxInt64. Legacy int64
// queries also support this API. Materialize=false returns only Summary.
func (p *PreparedQuery) ExecuteTyped(ctx context.Context, opts ExecuteOptions) (TypedResult, error) {
	var typed TypedResult
	summary, err := p.executeTyped(ctx, opts, true, &typed)
	if err != nil {
		return TypedResult{}, err
	}
	typed.Summary = summary
	return typed, nil
}

// ExecuteTyped prepares and executes one query with exact typed results.
func (e *Engine) ExecuteTyped(ctx context.Context, src source.Source, q expr.Query, opts ExecuteOptions) (TypedResult, error) {
	p, err := e.Prepare(ctx, src, q, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		return TypedResult{}, err
	}
	defer p.Close()
	var typed TypedResult
	summary, err := p.executeTyped(ctx, opts, false, &typed)
	if err != nil {
		return TypedResult{}, err
	}
	typed.Summary = summary
	return typed, nil
}

type typedResultCollector struct {
	query   *PreparedQuery
	result  *TypedResult
	account resultCollector
}

func (c *typedResultCollector) append(ctx context.Context, record aggregateRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rows := c.result.Rows
	if len(rows) == cap(rows) {
		if cap(rows) > math.MaxInt/2 {
			return fmt.Errorf("typed aggregate row capacity overflow")
		}
		capacity := max(16, cap(rows)*2)
		width := int64(unsafe.Sizeof(TypedAggregateRow{}))
		if uint64(capacity) > uint64(math.MaxInt64/width) {
			return fmt.Errorf("typed aggregate row allocation overflow")
		}
		lease, err := c.account.budget.Reserve(ctx, int64(capacity)*width)
		if err != nil {
			return err
		}
		grown := make([]TypedAggregateRow, len(rows), capacity)
		copy(grown, rows)
		if c.account.rowsLease != nil {
			c.account.rowsLease.Release()
		}
		c.account.rowsLease = lease
		rows = grown
	}
	bytes := int64(len(c.query.cq.GroupBy))*int64(unsafe.Sizeof(TypedGroupKey{})) + int64(len(c.query.cq.Aggregates))*int64(unsafe.Sizeof(TypedAggregateValue{}))
	if c.query.cq.GroupShape == GroupShapeGeneric {
		size, err := genericGroupPayloadSize(record.stringKey, c.query.cq.GenericFields, -1)
		if err != nil {
			return err
		}
		if size > math.MaxInt64-bytes {
			return fmt.Errorf("typed generic key size overflow")
		}
		bytes += size
	} else if c.query.cq.GroupShape == GroupShapeStringView {
		bytes += int64(len(record.stringKey))
	}
	for i, g := range c.query.cq.GroupBy {
		if c.query.cq.GroupShape != GroupShapeGeneric && g.Type == memory.TypeString && g.Encoding == memory.EncodingDictionary && record.validMask&(1<<i) != 0 {
			bits := record.a
			if i == 1 {
				bits = record.b
			}
			if d, ok := c.query.dicts[g.Name]; ok {
				bytes += int64(len(d.Value(uint32(bits))))
			}
		}
	}
	if err := c.account.reserve(ctx, bytes); err != nil {
		return err
	}
	row := TypedAggregateRow{Keys: make([]TypedGroupKey, len(c.query.cq.GroupBy)), Aggregates: make([]TypedAggregateValue, len(c.query.cq.Aggregates))}
	if c.query.cq.GroupShape == GroupShapeGeneric {
		if err := visitCanonicalGroupKey(record.stringKey, c.query.cq.GenericFields, func(i int, typ memory.DataType, valid bool, bits uint64, data []byte) error {
			value := TypedValue{Type: typ, Valid: valid, Bits: bits}
			if valid && (typ == memory.TypeString || typ == memory.TypeBinary) {
				value.Text = string(data)
			}
			row.Keys[i] = TypedGroupKey{Name: c.query.cq.GenericFields[i].Name, Value: value}
			return nil
		}); err != nil {
			return err
		}
	} else {
		for i, g := range c.query.cq.GroupBy {
			bits := record.a
			if i == 1 {
				bits = record.b
			}
			v := TypedValue{Type: g.Type, Valid: record.validMask&(1<<i) != 0, Bits: bits}
			if g.Type == memory.TypeString && v.Valid {
				if g.Encoding == memory.EncodingDictionary {
					if d, ok := c.query.dicts[g.Name]; ok {
						v.Text = string(d.Value(uint32(bits)))
					} else {
						v.DictionaryDomain = g.DictionaryDomain
					}
				} else {
					v.Text = string(record.stringKey)
				}
			}
			row.Keys[i] = TypedGroupKey{Name: g.Name, Value: v}
		}
	}
	for i, a := range c.query.cq.Aggregates {
		if a.Kind == expr.AggNUnique {
			row.Aggregates[i] = TypedAggregateValue{Alias: a.Alias, Kind: a.Kind, Value: TypedValue{Type: memory.TypeUint64, Valid: true, Bits: distinctRecordCount(record, c.query.distinct().OutputLane[i])}}
			continue
		}
		if expr.IsStatistics(a.Kind) {
			typ, bits, valid := c.query.statisticsRecordValue(record, i)
			row.Aggregates[i] = TypedAggregateValue{Alias: a.Alias, Kind: a.Kind, Value: TypedValue{Type: typ, Valid: valid, Bits: bits}}
			continue
		}
		lane, typ, layout := 0, memory.TypeInt64, c.query.cq.AggregateLayout
		if m := c.query.cq.MultiAggregate; m != nil {
			lane = m.OutputLane[i]
			typ = m.Types[lane]
			layout = m.Layouts[lane]
		}
		state := numericAggregateRecordState(record, lane, typ, layout)
		kind := aggregateStateKind(a.Kind)
		outputType, _ := aggregate.NumericAggregateType(typ, kind)
		bits, valid := state.Value(kind)
		row.Aggregates[i] = TypedAggregateValue{Alias: a.Alias, Kind: a.Kind, Value: TypedValue{Type: outputType, Valid: valid, Bits: bits}}
	}
	c.result.Rows = append(rows, row)
	return nil
}
