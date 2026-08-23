package engine

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
)

func compileQuery(schema memory.Schema, q expr.Query) (CompiledQuery, error) {
	if len(q.GroupBy) < 1 || len(q.GroupBy) > 2 {
		return CompiledQuery{}, fmt.Errorf("phase-2.2 supports one or two group-by columns")
	}
	prog, simple, err := expr.CompilePredicate(schema, q.Filter)
	if err != nil {
		return CompiledQuery{}, err
	}
	if len(q.Aggregates) == 0 {
		return CompiledQuery{}, fmt.Errorf("query requires at least one aggregate")
	}

	value := ""
	layout := aggregate.Layout{}
	for _, a := range q.Aggregates {
		switch a.Kind {
		case expr.AggSum:
			layout.Sum = true
		case expr.AggCountRows:
			layout.CountRows = true
			continue
		case expr.AggCountValues:
			layout.CountValues = true
		case expr.AggMin:
			layout.Min = true
		case expr.AggMax:
			layout.Max = true
		case expr.AggAvg:
			layout.Avg = true
		default:
			return CompiledQuery{}, fmt.Errorf("unsupported aggregate kind %d", a.Kind)
		}
		if a.Column == "" {
			return CompiledQuery{}, fmt.Errorf("aggregate %q requires an input column", a.Alias)
		}
		if value != "" && value != a.Column {
			return CompiledQuery{}, fmt.Errorf("phase-2.2 requires value aggregates to share one int64 column: %q vs %q", value, a.Column)
		}
		value = a.Column
	}
	valueNullable := false
	if value != "" {
		if err := schema.Require(value, memory.TypeInt64); err != nil {
			return CompiledQuery{}, err
		}
		valueField, _ := schema.Field(value)
		valueNullable = valueField.Nullable
	}

	groups := make([]CompiledGroupColumn, 0, len(q.GroupBy))
	keyWidth := 0
	for _, name := range q.GroupBy {
		f, ok := schema.Field(name)
		if !ok {
			return CompiledQuery{}, fmt.Errorf("group column %q not found", name)
		}
		g := CompiledGroupColumn{Name: name, Type: f.Type, Encoding: f.Encoding, DictionaryDomain: f.DictionaryDomain, Nullable: f.Nullable}
		groups = append(groups, g)
		switch f.Type {
		case memory.TypeInt32, memory.TypeUint32:
			keyWidth += 4
		case memory.TypeInt64:
			keyWidth += 8
		case memory.TypeString:
			if f.Encoding == memory.EncodingDictionary {
				if f.DictionaryDomain == "" {
					return CompiledQuery{}, fmt.Errorf("dictionary group column %q requires stable DictionaryDomain", name)
				}
				keyWidth += 4
			} else {
				if len(q.GroupBy) > 1 {
					return CompiledQuery{}, fmt.Errorf("plain/view string is only supported as a single group key in phase-2.2")
				}
				if q.Hints.AverageKeyBytes > 0 {
					keyWidth += q.Hints.AverageKeyBytes
				} else {
					keyWidth += 16
				}
			}
		default:
			return CompiledQuery{}, fmt.Errorf("unsupported group type %s for %q", f.Type, name)
		}
	}

	shape := GroupShapeInvalid
	radixCapable := layout.IsSumCountOnly() && value != "" && !valueNullable
	if len(groups) == 1 {
		g := groups[0]
		switch g.Type {
		case memory.TypeInt32:
			shape = GroupShapeI32
		case memory.TypeInt64:
			shape = GroupShapeI64
		case memory.TypeUint32:
			shape = GroupShapeU32
		case memory.TypeString:
			if g.Encoding == memory.EncodingDictionary {
				shape = GroupShapeDictionaryString
			} else {
				shape = GroupShapeStringView
				radixCapable = false
			}
		default:
			return CompiledQuery{}, fmt.Errorf("single group type %s not yet supported", g.Type)
		}
	} else {
		for _, g := range groups {
			if g.Type == memory.TypeString && g.Encoding != memory.EncodingDictionary {
				return CompiledQuery{}, fmt.Errorf("plain string cannot be embedded in fixed pair key")
			}
			switch g.Type {
			case memory.TypeInt32, memory.TypeInt64, memory.TypeUint32, memory.TypeString:
			default:
				return CompiledQuery{}, fmt.Errorf("unsupported pair group type %s", g.Type)
			}
		}
		shape = GroupShapePairFixed
		// Nullable pair radix carries a validity mask alongside the physical key.
	}

	card := q.Hints.Cardinality
	if card <= 0 {
		card = 1 << 16
	}
	sel := q.Hints.Selectivity
	if sel <= 0 || sel > 1 {
		sel = .5
	}
	avg := q.Hints.AverageKeyBytes
	if avg <= 0 {
		avg = 16
	}
	return CompiledQuery{
		Predicate: prog, SimpleGT: simple, GroupBy: groups, GroupShape: shape,
		ValueColumn: value, ValueNullable: valueNullable, Aggregates: append([]expr.Aggregate(nil), q.Aggregates...), AggregateLayout: layout,
		CardinalityHint: card, SelectivityHint: sel, AverageKeyBytes: avg,
		ReuseSelection: q.Hints.ReuseSelection, ForceStrategy: q.Hints.ForceStrategy,
		ForceRadixBits: q.Hints.ForceRadixBits, FusedCapable: simple != nil,
		RadixCapable: radixCapable, KeyWidthBytes: keyWidth,
	}, nil
}
