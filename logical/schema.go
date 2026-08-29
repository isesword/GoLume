package logical

import (
	"fmt"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/memory"
	"math"
)

// InferSchema validates plan column references and returns its logical output
// schema without executing the plan.
func InferSchema(plan Plan) (memory.Schema, error) {
	switch n := plan.(type) {
	case *Scan:
		if n == nil || n.Source == nil {
			return memory.Schema{}, fmt.Errorf("logical scan has nil source")
		}
		base := n.Source.Schema()
		if len(n.Projection) == 0 {
			return base, nil
		}
		fields := make([]memory.Field, 0, len(n.Projection))
		seen := make(map[string]struct{}, len(n.Projection))
		for _, name := range n.Projection {
			if _, dup := seen[name]; dup {
				return memory.Schema{}, fmt.Errorf("projection contains duplicate column %q", name)
			}
			seen[name] = struct{}{}
			f, ok := base.Field(name)
			if !ok {
				return memory.Schema{}, fmt.Errorf("projection column %q not found", name)
			}
			fields = append(fields, f)
		}
		return memory.NewSchema(fields...), nil

	case *Filter:
		if n == nil || n.Input == nil || n.Predicate == nil {
			return memory.Schema{}, fmt.Errorf("logical filter requires input and predicate")
		}
		schema, err := InferSchema(n.Input)
		if err != nil {
			return memory.Schema{}, err
		}
		if _, _, err := expr.CompilePredicate(schema, n.Predicate); err != nil {
			return memory.Schema{}, fmt.Errorf("filter predicate: %w", err)
		}
		return schema, nil

	case *Projection:
		if n == nil || n.Input == nil {
			return memory.Schema{}, fmt.Errorf("logical projection requires input")
		}
		in, err := InferSchema(n.Input)
		if err != nil {
			return memory.Schema{}, err
		}
		fields := make([]memory.Field, 0, len(n.Columns))
		seen := make(map[string]struct{}, len(n.Columns))
		for _, name := range n.Columns {
			if _, dup := seen[name]; dup {
				return memory.Schema{}, fmt.Errorf("projection contains duplicate column %q", name)
			}
			seen[name] = struct{}{}
			f, ok := in.Field(name)
			if !ok {
				return memory.Schema{}, fmt.Errorf("projection column %q not found", name)
			}
			fields = append(fields, f)
		}
		return memory.NewSchema(fields...), nil

	case *ExprProjection:
		if n == nil || n.Input == nil || len(n.Exprs) == 0 {
			return memory.Schema{}, fmt.Errorf("logical expression projection requires input and expressions")
		}
		in, err := InferSchema(n.Input)
		if err != nil {
			return memory.Schema{}, err
		}
		fields := make([]memory.Field, 0, len(in.Fields)+len(n.Exprs))
		index := make(map[string]int)
		if n.IncludeInput {
			fields = append(fields, in.Fields...)
			for i, f := range fields {
				index[f.Name] = i
			}
		}
		produced := make(map[string]struct{}, len(n.Exprs))
		for _, named := range n.Exprs {
			name, err := named.Name()
			if err != nil {
				return memory.Schema{}, err
			}
			if _, dup := produced[name]; dup {
				return memory.Schema{}, fmt.Errorf("expression projection contains duplicate output %q", name)
			}
			produced[name] = struct{}{}
			field, err := inferScalarField(in, named.Expr, name)
			if err != nil {
				return memory.Schema{}, err
			}
			if pos, ok := index[name]; ok {
				fields[pos] = field
				continue
			}
			index[name] = len(fields)
			fields = append(fields, field)
		}
		return memory.NewSchema(fields...), nil

	case *Join:
		if n == nil || n.Left == nil || n.Right == nil {
			return memory.Schema{}, fmt.Errorf("logical join requires left and right inputs")
		}
		if len(n.Keys) < 1 || len(n.Keys) > 2 {
			return memory.Schema{}, fmt.Errorf("phase-2.4 join requires one or two equality keys")
		}
		left, err := InferSchema(n.Left)
		if err != nil {
			return memory.Schema{}, err
		}
		right, err := InferSchema(n.Right)
		if err != nil {
			return memory.Schema{}, err
		}
		for _, key := range n.Keys {
			lf, lok := left.Field(key.Left)
			rf, rok := right.Field(key.Right)
			if !lok {
				return memory.Schema{}, fmt.Errorf("join left column %q not found", key.Left)
			}
			if !rok {
				return memory.Schema{}, fmt.Errorf("join right column %q not found", key.Right)
			}
			if lf.Type != rf.Type {
				return memory.Schema{}, fmt.Errorf("join key type mismatch %q=%s vs %q=%s", key.Left, lf.Type, key.Right, rf.Type)
			}
		}
		fields := append([]memory.Field(nil), left.Fields...)
		if n.Type == join.Semi || n.Type == join.Anti {
			return memory.NewSchema(fields...), nil
		}
		suffix := n.Suffix
		if suffix == "" {
			suffix = "_right"
		}
		seen := make(map[string]struct{}, len(fields))
		for _, f := range fields {
			seen[f.Name] = struct{}{}
		}
		for _, f := range right.Fields {
			out := f
			if n.Type == join.Left {
				out.Nullable = true
			}
			if _, exists := seen[out.Name]; exists {
				out.Name += suffix
			}
			if _, exists := seen[out.Name]; exists {
				return memory.Schema{}, fmt.Errorf("join output column %q remains ambiguous after suffix", out.Name)
			}
			seen[out.Name] = struct{}{}
			fields = append(fields, out)
		}
		return memory.NewSchema(fields...), nil

	case *Sort:
		if n == nil || n.Input == nil || len(n.By) == 0 {
			return memory.Schema{}, fmt.Errorf("logical sort requires input and ordering keys")
		}
		in, err := InferSchema(n.Input)
		if err != nil {
			return memory.Schema{}, err
		}
		for i, key := range n.By {
			value, err := key.Expression()
			if err != nil {
				return memory.Schema{}, fmt.Errorf("sort key %d: %w", i, err)
			}
			if _, err := expr.InferScalarType(in, value); err != nil {
				return memory.Schema{}, fmt.Errorf("sort key %d: %w", i, err)
			}
		}
		return in, nil

	case *TopK:
		if n == nil || n.Input == nil || n.K < 0 || len(n.By) == 0 {
			return memory.Schema{}, fmt.Errorf("logical top-k requires input, non-negative K, and ordering keys")
		}
		in, err := InferSchema(n.Input)
		if err != nil {
			return memory.Schema{}, err
		}
		for i, key := range n.By {
			value, err := key.Expression()
			if err != nil {
				return memory.Schema{}, fmt.Errorf("top-k key %d: %w", i, err)
			}
			if _, err := expr.InferScalarType(in, value); err != nil {
				return memory.Schema{}, fmt.Errorf("top-k key %d: %w", i, err)
			}
		}
		return in, nil

	case *Limit:
		if n == nil || n.Input == nil || n.N < 0 {
			return memory.Schema{}, fmt.Errorf("logical limit requires input and a non-negative row count")
		}
		return InferSchema(n.Input)

	case *Aggregate:
		if n == nil || n.Input == nil {
			return memory.Schema{}, fmt.Errorf("logical aggregate requires input")
		}
		in, err := InferSchema(n.Input)
		if err != nil {
			return memory.Schema{}, err
		}
		if len(n.GroupBy) == 0 {
			return memory.Schema{}, fmt.Errorf("phase-2.3 aggregate requires at least one group key")
		}
		fields := make([]memory.Field, 0, len(n.GroupBy)+len(n.Aggregates))
		for _, name := range n.GroupBy {
			f, ok := in.Field(name)
			if !ok {
				return memory.Schema{}, fmt.Errorf("group column %q not found", name)
			}
			fields = append(fields, f)
		}
		if len(n.Aggregates) == 0 {
			return memory.Schema{}, fmt.Errorf("aggregate requires at least one function")
		}
		for i, agg := range n.Aggregates {
			name := agg.Alias
			if name == "" {
				name = fmt.Sprintf("agg_%d", i)
			}
			field := memory.Field{Name: name, Encoding: memory.EncodingPlain}
			switch agg.Kind {
			case expr.AggCountRows:
				field.Type = memory.TypeUint64
			case expr.AggSum, expr.AggCountValues, expr.AggMin, expr.AggMax, expr.AggAvg:
				if agg.Column == "" {
					return memory.Schema{}, fmt.Errorf("aggregate %q requires an input column", name)
				}
				input, ok := in.Field(agg.Column)
				if !ok {
					return memory.Schema{}, fmt.Errorf("aggregate column %q not found", agg.Column)
				}
				if input.Type != memory.TypeInt64 {
					return memory.Schema{}, fmt.Errorf("aggregate column %q must be int64, got %s", agg.Column, input.Type)
				}
				switch agg.Kind {
				case expr.AggCountValues:
					field.Type = memory.TypeUint64
				case expr.AggAvg:
					field.Type = memory.TypeFloat64
					field.Nullable = true
				default:
					field.Type = memory.TypeInt64
					field.Nullable = true
				}
			default:
				return memory.Schema{}, fmt.Errorf("unsupported aggregate kind %d", agg.Kind)
			}
			fields = append(fields, field)
		}
		return memory.NewSchema(fields...), nil

	case *Window:
		if n == nil || n.Input == nil || len(n.Exprs) == 0 {
			return memory.Schema{}, fmt.Errorf("logical window requires input and expressions")
		}
		if len(n.Spec.By) == 0 {
			return memory.Schema{}, fmt.Errorf("window requires at least one ordering key")
		}
		in, err := InferSchema(n.Input)
		if err != nil {
			return memory.Schema{}, err
		}
		partitionSeen := make(map[string]struct{}, len(n.Spec.PartitionBy))
		for _, name := range n.Spec.PartitionBy {
			if _, duplicate := partitionSeen[name]; duplicate {
				return memory.Schema{}, fmt.Errorf("window partition contains duplicate column %q", name)
			}
			partitionSeen[name] = struct{}{}
			field, ok := in.Field(name)
			if !ok {
				return memory.Schema{}, fmt.Errorf("window partition column %q not found", name)
			}
			if !windowOrderable(field.Type) {
				return memory.Schema{}, fmt.Errorf("window partition column %q type %s is not orderable", name, field.Type)
			}
		}
		for i, key := range n.Spec.By {
			value, err := key.Expression()
			if err != nil {
				return memory.Schema{}, fmt.Errorf("window order key %d: %w", i, err)
			}
			info, err := expr.InferScalarType(in, value)
			if err != nil {
				return memory.Schema{}, fmt.Errorf("window order key %d: %w", i, err)
			}
			if !windowOrderable(info.Type) {
				return memory.Schema{}, fmt.Errorf("window order key %d type %s is not orderable", i, info.Type)
			}
		}
		fields := append([]memory.Field(nil), in.Fields...)
		positions := make(map[string]int, len(fields)+len(n.Exprs))
		for i, field := range fields {
			positions[field.Name] = i
		}
		aliases := make(map[string]struct{}, len(n.Exprs))
		for _, window := range n.Exprs {
			if window.Alias == "" {
				return memory.Schema{}, fmt.Errorf("window expression requires an alias")
			}
			if _, duplicate := aliases[window.Alias]; duplicate {
				return memory.Schema{}, fmt.Errorf("window contains duplicate output %q", window.Alias)
			}
			aliases[window.Alias] = struct{}{}
			field, err := inferWindowField(in, window)
			if err != nil {
				return memory.Schema{}, err
			}
			if pos, replace := positions[field.Name]; replace {
				fields[pos] = field
			} else {
				positions[field.Name] = len(fields)
				fields = append(fields, field)
			}
		}
		return memory.NewSchema(fields...), nil

	default:
		return memory.Schema{}, fmt.Errorf("unsupported logical plan node %T", plan)
	}
}

func inferWindowField(schema memory.Schema, window WindowExpr) (memory.Field, error) {
	field := memory.Field{Name: window.Alias, Encoding: memory.EncodingPlain}
	switch window.Function {
	case WindowRowNumber, WindowRank, WindowDenseRank, WindowCountRows:
		if window.Column != "" {
			return memory.Field{}, fmt.Errorf("window %q does not accept an input column", window.Alias)
		}
		field.Type = memory.TypeInt64
		return field, nil
	case WindowCountValues:
		if window.Column == "" {
			return memory.Field{}, fmt.Errorf("window count %q requires an input column", window.Alias)
		}
		if _, ok := schema.Field(window.Column); !ok {
			return memory.Field{}, fmt.Errorf("window column %q not found", window.Column)
		}
		if err := validateWindowFrame(window.Frame); err != nil {
			return memory.Field{}, fmt.Errorf("window count %q: %w", window.Alias, err)
		}
		field.Type = memory.TypeInt64
		return field, nil
	case WindowSum, WindowMin, WindowMax, WindowAvg:
		if window.Column == "" {
			return memory.Field{}, fmt.Errorf("window aggregate %q requires an input column", window.Alias)
		}
		input, ok := schema.Field(window.Column)
		if !ok {
			return memory.Field{}, fmt.Errorf("window column %q not found", window.Column)
		}
		if !windowNumeric(input.Type) {
			return memory.Field{}, fmt.Errorf("window aggregate column %q must be numeric, got %s", window.Column, input.Type)
		}
		if err := validateWindowFrame(window.Frame); err != nil {
			return memory.Field{}, fmt.Errorf("window aggregate %q: %w", window.Alias, err)
		}
		field.Nullable = true
		switch window.Function {
		case WindowSum:
			switch input.Type {
			case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
				field.Type = memory.TypeInt64
			case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64:
				field.Type = memory.TypeUint64
			default:
				field.Type = memory.TypeFloat64
			}
		case WindowAvg:
			field.Type = memory.TypeFloat64
		default:
			field.Type = input.Type
		}
		return field, nil
	default:
		return memory.Field{}, fmt.Errorf("unsupported window function %d", window.Function)
	}
}

func validateWindowFrame(frame RowsFrame) error {
	start, err := windowBoundOrder(frame.Start)
	if err != nil {
		return err
	}
	end, err := windowBoundOrder(frame.End)
	if err != nil {
		return err
	}
	if start > end {
		return fmt.Errorf("frame start is after frame end")
	}
	return nil
}

func windowBoundOrder(bound RowBound) (int64, error) {
	switch bound.Kind {
	case RowUnboundedPreceding:
		if bound.Offset != 0 {
			return 0, fmt.Errorf("unbounded preceding cannot have an offset")
		}
		return math.MinInt64, nil
	case RowPreceding:
		if bound.Offset < 0 {
			return 0, fmt.Errorf("preceding offset must be non-negative")
		}
		return -bound.Offset, nil
	case RowCurrent:
		if bound.Offset != 0 {
			return 0, fmt.Errorf("current row cannot have an offset")
		}
		return 0, nil
	case RowFollowing:
		if bound.Offset < 0 {
			return 0, fmt.Errorf("following offset must be non-negative")
		}
		return bound.Offset, nil
	case RowUnboundedFollowing:
		if bound.Offset != 0 {
			return 0, fmt.Errorf("unbounded following cannot have an offset")
		}
		return math.MaxInt64, nil
	default:
		return 0, fmt.Errorf("unknown frame boundary %d", bound.Kind)
	}
}

func windowNumeric(typ memory.DataType) bool {
	switch typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
		memory.TypeFloat32, memory.TypeFloat64:
		return true
	default:
		return false
	}
}

func windowOrderable(typ memory.DataType) bool {
	switch typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
		memory.TypeFloat32, memory.TypeFloat64, memory.TypeDate32, memory.TypeDate64,
		memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration,
		memory.TypeString, memory.TypeBinary:
		return true
	default:
		return false
	}
}

func inferScalarField(schema memory.Schema, e expr.Expr, name string) (memory.Field, error) {
	if c, ok := e.(expr.Column); ok {
		f, exists := schema.Field(c.Name)
		if !exists {
			return memory.Field{}, fmt.Errorf("projection column %q not found", c.Name)
		}
		f.Name = name
		return f, nil
	}
	if decoded, ok := e.(expr.JSONDecodeExpr); ok {
		field := memory.StructField(name, true, decoded.Schema.Fields...)
		if err := field.ValidateStruct(); err != nil {
			return memory.Field{}, fmt.Errorf("projection %q: %w", name, err)
		}
		return field, nil
	}
	info, err := expr.InferScalarType(schema, e)
	if err != nil {
		return memory.Field{}, fmt.Errorf("projection %q: %w", name, err)
	}
	field := memory.Field{Name: name, Type: info.Type, Nullable: info.Nullable, Encoding: memory.EncodingPlain}
	if info.Type == memory.TypeString || info.Type == memory.TypeBinary {
		field.Encoding = memory.EncodingView
	}
	return field, nil
}
