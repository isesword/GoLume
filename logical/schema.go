package logical

import (
	"fmt"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/memory"
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
		for _, key := range n.By {
			if _, ok := in.Field(key.Column); !ok {
				return memory.Schema{}, fmt.Errorf("sort column %q not found", key.Column)
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
		for _, key := range n.By {
			if _, ok := in.Field(key.Column); !ok {
				return memory.Schema{}, fmt.Errorf("top-k column %q not found", key.Column)
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

	default:
		return memory.Schema{}, fmt.Errorf("unsupported logical plan node %T", plan)
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
