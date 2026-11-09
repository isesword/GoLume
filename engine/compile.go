package engine

import (
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func compileQuery(schema memory.Schema, q expr.Query) (CompiledQuery, error) {
	for _, field := range schema.Fields {
		if field.Type == memory.TypeDecimal && field.DecimalPrecision != 0 {
			return CompiledQuery{}, fmt.Errorf("native decimal aggregation is not supported yet for column %q", field.Name)
		}
	}
	outputNames := make(map[string]struct{}, len(q.GroupBy)+len(q.Aggregates))
	for _, name := range q.GroupBy {
		if _, duplicate := outputNames[name]; duplicate {
			return CompiledQuery{}, fmt.Errorf("aggregate contains duplicate output %q", name)
		}
		outputNames[name] = struct{}{}
	}
	for i, agg := range q.Aggregates {
		name := agg.Alias
		if name == "" {
			name = fmt.Sprintf("agg_%d", i)
		}
		if _, duplicate := outputNames[name]; duplicate {
			return CompiledQuery{}, fmt.Errorf("aggregate contains duplicate output %q", name)
		}
		outputNames[name] = struct{}{}
	}

	predicate := q.Filter
	if predicate == nil {
		predicate = expr.Bool(true)
	}
	prog, err := expr.Compile(schema, predicate)
	if err != nil {
		return CompiledQuery{}, err
	}
	if prog.Type() != memory.TypeBool {
		return CompiledQuery{}, fmt.Errorf("query predicate must be bool, got %s", prog.Type())
	}
	simple := prog.SimpleComparison()
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
		case expr.AggNUnique, expr.AggVar, expr.AggStd, expr.AggVarPopulation, expr.AggStdPopulation, expr.AggFirst, expr.AggLast:
			if a.Column == "" {
				return CompiledQuery{}, fmt.Errorf("aggregate %q requires an input column", a.Alias)
			}
			continue
		default:
			return CompiledQuery{}, fmt.Errorf("unsupported aggregate kind %d", a.Kind)
		}
		if a.Column == "" {
			return CompiledQuery{}, fmt.Errorf("aggregate %q requires an input column", a.Alias)
		}
		if value == "" {
			value = a.Column
		}
	}
	multi, err := compileMultiAggregate(schema, q.Aggregates)
	if err != nil {
		return CompiledQuery{}, err
	}
	valueNullable := false
	if value != "" {
		if multi == nil || !multi.Native {
			if err := schema.Require(value, memory.TypeInt64); err != nil {
				return CompiledQuery{}, err
			}
		}
		valueField, _ := schema.Field(value)
		valueNullable = valueField.Nullable
	}

	groups := make([]CompiledGroupColumn, 0, len(q.GroupBy))
	fields := make([]memory.Field, 0, len(q.GroupBy))
	keyWidth := 0
	generic := len(q.GroupBy) > 2 || (multi != nil && multi.Distinct != nil && len(q.GroupBy) > 0)
	for _, name := range q.GroupBy {
		f, ok := schema.Field(name)
		if !ok {
			return CompiledQuery{}, fmt.Errorf("group column %q not found", name)
		}
		width, supported := canonicalGroupWidth(f.Type)
		if !supported {
			return CompiledQuery{}, fmt.Errorf("unsupported group type %s for %q", f.Type, name)
		}
		fields = append(fields, f)
		groups = append(groups, CompiledGroupColumn{Name: name, Type: f.Type, Encoding: f.Encoding, DictionaryDomain: f.DictionaryDomain, Nullable: f.Nullable})
		if width == 0 {
			width = q.Hints.AverageKeyBytes
			if width <= 0 {
				width = 16
			}
		}
		keyWidth += width
		switch f.Type {
		case memory.TypeInt32, memory.TypeInt64, memory.TypeUint32:
		case memory.TypeString:
			if f.Encoding == memory.EncodingDictionary {
				generic = generic || f.DictionaryDomain == ""
			} else {
				generic = generic || len(q.GroupBy) > 1
			}
		default:
			generic = true
		}
	}
	shape := GroupShapeGlobal
	if generic {
		shape = GroupShapeGeneric
		keyWidth += canonicalGroupHeaderBytes * len(groups)
		if multi == nil {
			multi, err = compileMultiAggregate(schema, q.Aggregates, true)
			if err != nil {
				return CompiledQuery{}, err
			}
		}
	} else if len(groups) == 2 {
		shape = GroupShapePairFixed
	} else if len(groups) == 1 {
		switch groups[0].Type {
		case memory.TypeInt32:
			shape = GroupShapeI32
		case memory.TypeInt64:
			shape = GroupShapeI64
		case memory.TypeUint32:
			shape = GroupShapeU32
		case memory.TypeString:
			if groups[0].Encoding == memory.EncodingDictionary {
				shape = GroupShapeDictionaryString
			} else {
				shape = GroupShapeStringView
			}
		}
	}

	// Preserve specialized dictionary cost estimates; generic dictionaries use
	// decoded payload widths rather than four-byte domain-local codes.
	if !generic {
		for _, f := range fields {
			if f.Type == memory.TypeString && f.Encoding == memory.EncodingDictionary {
				width := q.Hints.AverageKeyBytes
				if width <= 0 {
					width = 16
				}
				keyWidth += 4 - width
			}
		}
	}

	card := q.Hints.Cardinality
	if shape == GroupShapeGlobal {
		card = 1
	}
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
	comparisonID, hasComparisonID := simpleComparisonKernelID(simple)
	return CompiledQuery{
		MultiAggregate: multi, Predicate: prog, SimpleComparison: simple, GroupBy: groups, GroupShape: shape, GenericFields: fields,
		ValueColumn: value, ValueNullable: valueNullable, Aggregates: append([]expr.Aggregate(nil), q.Aggregates...), AggregateLayout: layout,
		CardinalityHint: card, SelectivityHint: sel, AverageKeyBytes: avg,
		ReuseSelection: q.Hints.ReuseSelection, ForceStrategy: q.Hints.ForceStrategy,
		FusedCapable: simple != nil && multi == nil, ComparisonID: comparisonID, HasComparisonID: hasComparisonID, KeyWidthBytes: keyWidth,
	}, nil
}

func simpleComparisonKernelID(simple *expr.SimpleComparison) (kernel.KernelID, bool) {
	if simple == nil {
		return kernel.KernelID{}, false
	}
	op, err := directCompareOp(simple.Op)
	if err != nil {
		return kernel.KernelID{}, false
	}
	return kernel.ComparisonID(simple.Type, op)
}
