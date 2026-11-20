package logical

import (
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
)

// WindowFrameKind explicitly distinguishes ROWS from timestamp RANGE.
type WindowFrameKind uint8

const (
	// WindowFrameRows is the existing zero/default ROWS meaning.
	WindowFrameRows WindowFrameKind = iota
	// WindowFrameRange selects an inclusive timestamp-distance frame.
	WindowFrameRange
)

// TimestampRangeFrame is an inclusive trailing nanosecond interval.
type TimestampRangeFrame struct {
	Kind        WindowFrameKind
	PrecedingNS int64
}

// TimestampRangeSpec selects the value/key and named aggregate outputs.
type TimestampRangeSpec struct {
	OrderBy, Column                                string
	Frame                                          TimestampRangeFrame
	CountRowsAlias, CountAlias, SumAlias, AvgAlias string
}

// TimestampRange retains all requested outputs and their validation effects.
type TimestampRange struct {
	Input Plan
	Spec  TimestampRangeSpec
}

func (*TimestampRange) planNode() {}

func timestampRangeScan(p Plan, depth int) (*Scan, error) {
	if depth > 16 {
		return nil, fmt.Errorf("timestamp RANGE lineage exceeds bound")
	}
	switch n := p.(type) {
	case *Scan:
		if n == nil || n.Source == nil || n.PushedPredicate != nil {
			return nil, fmt.Errorf("timestamp RANGE requires unfiltered original scan")
		}
		plan := source.StreamPlanOf(n.Source)
		if plan.Operator != "Scan" || plan.Class != source.StreamPipeline || len(plan.Children) != 0 {
			return nil, fmt.Errorf("timestamp RANGE requires original scan lineage")
		}
		return n, nil
	case *Projection:
		return timestampRangeScan(n.Input, depth+1)
	default:
		return nil, fmt.Errorf("timestamp RANGE supports only original Scan and direct Projection upstream")
	}
}
func inferTimestampRange(n *TimestampRange) (memory.Schema, error) {
	if n == nil {
		return memory.Schema{}, fmt.Errorf("nil timestamp RANGE")
	}
	sp := n.Spec
	if sp.OrderBy == "" || sp.Column == "" || sp.OrderBy == sp.Column || sp.Frame.Kind != WindowFrameRange || sp.Frame.PrecedingNS < 0 {
		return memory.Schema{}, fmt.Errorf("timestamp RANGE requires explicit RANGE, nonnegative duration and distinct columns")
	}
	scan, err := timestampRangeScan(n.Input, 0)
	if err != nil {
		return memory.Schema{}, err
	}
	schema, err := InferSchema(n.Input)
	if err != nil {
		return memory.Schema{}, err
	}
	if len(schema.Fields) != 2 {
		return memory.Schema{}, fmt.Errorf("timestamp RANGE requires exactly two input columns")
	}
	key, k := schema.Field(sp.OrderBy)
	val, v := schema.Field(sp.Column)
	plain := func(f memory.Field) bool {
		return f.Encoding == memory.EncodingPlain && f.ArrowType == "" && f.DictionaryDomain == "" && len(f.Children) == 0 && f.DecimalPrecision == 0 && f.DecimalScale == 0
	}
	if !k || !v || key.Type != memory.TypeTimestamp || key.Nullable || val.Type != memory.TypeInt64 || !plain(key) || !plain(val) {
		return memory.Schema{}, fmt.Errorf("timestamp RANGE requires required canonical ns Timestamp and plain Int64")
	}
	provider, ok := scan.Source.(source.OrderingProvider)
	if !ok {
		return memory.Schema{}, fmt.Errorf("timestamp RANGE requires original ascending order proof")
	}
	ordering := provider.Ordering()
	if len(ordering) == 0 || ordering[0].Name != sp.OrderBy || ordering[0].Descending {
		return memory.Schema{}, fmt.Errorf("timestamp RANGE requires original ascending order proof")
	}
	fields := append([]memory.Field(nil), schema.Fields...)
	aliases := [4]string{sp.CountRowsAlias, sp.CountAlias, sp.SumAlias, sp.AvgAlias}
	outputs := 0
	for i, name := range aliases {
		if name == "" {
			continue
		}
		outputs++
		for _, f := range fields {
			if f.Name == name {
				return memory.Schema{}, fmt.Errorf("timestamp RANGE alias %q already exists", name)
			}
		}
		typ := memory.TypeInt64
		if i == 3 {
			typ = memory.TypeFloat64
		}
		fields = append(fields, memory.Field{Name: name, Type: typ, Nullable: i >= 2})
	}
	if outputs == 0 {
		return memory.Schema{}, fmt.Errorf("timestamp RANGE requires at least one output")
	}
	return memory.NewSchema(fields...), nil
}

// ContainsTimestampRange preserves admission even when outputs are projected away.
func ContainsTimestampRange(p Plan) bool {
	switch n := p.(type) {
	case *TimestampRange:
		return true
	case *Projection:
		return ContainsTimestampRange(n.Input)
	case *ExprProjection:
		return ContainsTimestampRange(n.Input)
	case *Filter:
		return ContainsTimestampRange(n.Input)
	case *Sort:
		return ContainsTimestampRange(n.Input)
	case *TopK:
		return ContainsTimestampRange(n.Input)
	case *Limit:
		return ContainsTimestampRange(n.Input)
	case *Aggregate:
		return ContainsTimestampRange(n.Input)
	case *ConfiguredAggregate:
		return ContainsTimestampRange(n.Input)
	case *Window:
		return ContainsTimestampRange(n.Input)
	case *ConfiguredWindow:
		return ContainsTimestampRange(n.Input)
	case *Join:
		return ContainsTimestampRange(n.Left) || ContainsTimestampRange(n.Right)
	default:
		return false
	}
}
