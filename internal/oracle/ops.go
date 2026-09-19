package oracle

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"gocolumn/expr"
)

// SortKey describes one ordering key. NULL placement is direction-independent
// (NullsLast keeps NULLs last in both ascending and descending order),
// matching the engine's physical sort comparator.
type SortKey struct {
	Column     string
	Descending bool
	NullsLast  bool
}

// String renders the key for diagnostics.
func (k SortKey) String() string {
	dir := "ASC"
	if k.Descending {
		dir = "DESC"
	}
	nulls := "NULLS FIRST"
	if k.NullsLast {
		nulls = "NULLS LAST"
	}
	return fmt.Sprintf("%s %s %s", k.Column, dir, nulls)
}

// Op is one relational operator in an oracle query pipeline.
type Op interface {
	fmt.Stringer
	apply(t Table) (Table, error)
	isOp()
}

// FilterOp keeps rows whose predicate is true under SQL three-valued logic.
type FilterOp struct {
	Predicate expr.Expr
}

func (FilterOp) isOp() {}

// String renders the operator for diagnostics.
func (o FilterOp) String() string { return fmt.Sprintf("filter(%s)", o.Predicate) }

func (o FilterOp) apply(t Table) (Table, error) {
	if o.Predicate == nil {
		return Table{}, fmt.Errorf("oracle: filter requires a predicate")
	}
	ctx, err := newRowContext(t.Columns)
	if err != nil {
		return Table{}, err
	}
	out := Table{Columns: t.Columns}
	for _, row := range t.Rows {
		ctx.row = row
		verdict, err := ctx.evalPredicate(o.Predicate)
		if err != nil {
			return Table{}, err
		}
		if verdict == triTrue {
			out.Rows = append(out.Rows, row)
		}
	}
	return out, nil
}

// SelectOp projects existing columns by name, reordering allowed. It maps to
// LazyFrame.Select.
type SelectOp struct {
	Columns []string
}

func (SelectOp) isOp() {}

// String renders the operator for diagnostics.
func (o SelectOp) String() string { return fmt.Sprintf("select(%s)", strings.Join(o.Columns, ", ")) }

func (o SelectOp) apply(t Table) (Table, error) {
	if len(o.Columns) == 0 {
		return Table{}, fmt.Errorf("oracle: select requires at least one column")
	}
	idx := make([]int, len(o.Columns))
	out := Table{Columns: make([]Column, len(o.Columns))}
	seen := make(map[string]struct{}, len(o.Columns))
	for i, name := range o.Columns {
		pos, ok := t.ColumnIndex(name)
		if !ok {
			return Table{}, fmt.Errorf("oracle: select column %q not found", name)
		}
		if _, dup := seen[name]; dup {
			return Table{}, fmt.Errorf("oracle: select column %q duplicated", name)
		}
		seen[name] = struct{}{}
		idx[i] = pos
		out.Columns[i] = t.Columns[pos]
	}
	for _, row := range t.Rows {
		projected := make([]Value, len(idx))
		for i, pos := range idx {
			projected[i] = row[pos]
		}
		out.Rows = append(out.Rows, projected)
	}
	return out, nil
}

// ProjectOp evaluates named scalar expressions per row. With IncludeInput set
// it maps to LazyFrame.WithColumns (input columns are retained, a matching
// alias replaces the input column in place); otherwise it maps to
// LazyFrame.SelectExprs (only the projected expressions survive).
type ProjectOp struct {
	Exprs        []expr.NamedExpr
	IncludeInput bool
}

func (ProjectOp) isOp() {}

// String renders the operator for diagnostics.
func (o ProjectOp) String() string {
	parts := make([]string, len(o.Exprs))
	for i, e := range o.Exprs {
		name, err := e.Name()
		if err != nil {
			name = "?"
		}
		parts[i] = fmt.Sprintf("%s AS %s", e.Expr, name)
	}
	verb := "project"
	if o.IncludeInput {
		verb = "with_columns"
	}
	return fmt.Sprintf("%s(%s)", verb, strings.Join(parts, ", "))
}

// scalarResultKind computes the output kind of one expression against the
// column list, mirroring the engine's InferScalarType rules for the supported
// subset.
func scalarResultKind(columns []Column, e expr.Expr) (Kind, error) {
	ctx, err := newRowContext(columns)
	if err != nil {
		return KindInvalid, err
	}
	switch n := e.(type) {
	case expr.Column:
		i, ok := ctx.index[n.Name]
		if !ok {
			return KindInvalid, fmt.Errorf("oracle: column %q not found", n.Name)
		}
		return columns[i].Kind, nil
	case expr.Int64Literal:
		return KindInt64, nil
	case expr.Float64Literal:
		return KindFloat64, nil
	case expr.StringLiteral:
		return KindString, nil
	case expr.Binary:
		left, err := scalarResultKind(columns, n.Left)
		if err != nil {
			return KindInvalid, err
		}
		right, err := scalarResultKind(columns, n.Right)
		if err != nil {
			return KindInvalid, err
		}
		switch n.Op {
		case expr.OpAdd, expr.OpSub, expr.OpMul:
			return numericResultKind(left, right)
		case expr.OpDiv:
			if _, err := numericResultKind(left, right); err != nil {
				return KindInvalid, err
			}
			return KindFloat64, nil
		case expr.OpConcat:
			if left != KindString || right != KindString {
				return KindInvalid, fmt.Errorf("oracle: || requires string operands, got %s and %s", left, right)
			}
			return KindString, nil
		default:
			return KindInvalid, fmt.Errorf("oracle: unsupported projection operator %s", n.Op)
		}
	default:
		return KindInvalid, fmt.Errorf("oracle: unsupported projection node %T", e)
	}
}

func (o ProjectOp) apply(t Table) (Table, error) {
	if len(o.Exprs) == 0 {
		return Table{}, fmt.Errorf("oracle: projection requires at least one expression")
	}
	ctx, err := newRowContext(t.Columns)
	if err != nil {
		return Table{}, err
	}
	out := Table{}
	positions := make(map[string]int)
	if o.IncludeInput {
		out.Columns = append(out.Columns, t.Columns...)
		for i, c := range out.Columns {
			positions[c.Name] = i
		}
	}
	produced := make(map[string]struct{}, len(o.Exprs))
	for _, e := range o.Exprs {
		name, err := e.Name()
		if err != nil {
			return Table{}, err
		}
		if _, dup := produced[name]; dup {
			return Table{}, fmt.Errorf("oracle: projection output %q duplicated", name)
		}
		produced[name] = struct{}{}
		kind, err := scalarResultKind(t.Columns, e.Expr)
		if err != nil {
			return Table{}, err
		}
		if pos, replace := positions[name]; replace {
			out.Columns[pos] = Column{Name: name, Kind: kind}
			continue
		}
		positions[name] = len(out.Columns)
		out.Columns = append(out.Columns, Column{Name: name, Kind: kind})
	}
	for _, row := range t.Rows {
		var projected []Value
		if o.IncludeInput {
			projected = append([]Value(nil), row...)
		}
		ctx.row = row
		for _, e := range o.Exprs {
			name, _ := e.Name()
			value, err := ctx.evalScalar(e.Expr)
			if err != nil {
				return Table{}, err
			}
			pos := positions[name]
			if o.IncludeInput {
				if pos < len(projected) {
					projected[pos] = value
				} else {
					projected = append(projected, value)
				}
			} else {
				projected = append(projected, value)
			}
		}
		out.Rows = append(out.Rows, projected)
	}
	return out, nil
}

// GroupByOp groups rows by one or more keys (NULL keys form their own group)
// and evaluates the listed aggregates per group. Output rows are emitted in
// first-seen group order; the engine makes no ordering promise, so
// differential tests compare group results order-insensitively unless a later
// Sort or TopK re-establishes order.
type GroupByOp struct {
	Keys       []string
	Aggregates []expr.Aggregate
}

func (GroupByOp) isOp() {}

// String renders the operator for diagnostics.
func (o GroupByOp) String() string {
	parts := make([]string, len(o.Aggregates))
	for i, a := range o.Aggregates {
		parts[i] = fmt.Sprintf("%d(%s) AS %s", a.Kind, a.Column, a.Alias)
	}
	return fmt.Sprintf("group_by(%s) agg(%s)", strings.Join(o.Keys, ", "), strings.Join(parts, ", "))
}

// groupKeyStrings encodes a group key tuple so that NULL is distinct from
// every payload and different kinds never collide.
func groupKeyStrings(row []Value, keyIdx []int) string {
	var b strings.Builder
	for _, idx := range keyIdx {
		v := row[idx]
		if v.Null {
			b.WriteString("N;")
			continue
		}
		switch v.Kind {
		case KindInt64:
			fmt.Fprintf(&b, "i%d;", v.I64)
		case KindUint64:
			fmt.Fprintf(&b, "u%d;", v.U64)
		case KindFloat64:
			// Grouping uses bit equality: +0.0 and -0.0 are different groups
			// and each NaN bit pattern is its own group.
			fmt.Fprintf(&b, "f%x;", math.Float64bits(v.F64))
		case KindString:
			fmt.Fprintf(&b, "s%d:%s;", len(v.Str), v.Str)
		case KindBool:
			fmt.Fprintf(&b, "b%t;", v.Bool)
		}
	}
	return b.String()
}

// aggregateOutputColumn computes the output column for one aggregate,
// mirroring the engine's logical schema rules for the supported subset.
func aggregateOutputColumn(agg expr.Aggregate, input Table) (Column, error) {
	name := agg.Alias
	if name == "" {
		return Column{}, fmt.Errorf("oracle: aggregate requires an alias")
	}
	switch agg.Kind {
	case expr.AggCountRows:
		return Column{Name: name, Kind: KindUint64}, nil
	case expr.AggCountValues, expr.AggSum, expr.AggMin, expr.AggMax, expr.AggAvg:
		if agg.Column == "" {
			return Column{}, fmt.Errorf("oracle: aggregate %q requires an input column", name)
		}
		pos, ok := input.ColumnIndex(agg.Column)
		if !ok {
			return Column{}, fmt.Errorf("oracle: aggregate column %q not found", agg.Column)
		}
		kind := input.Columns[pos].Kind
		switch agg.Kind {
		case expr.AggCountValues:
			return Column{Name: name, Kind: KindUint64}, nil
		case expr.AggAvg:
			if kind != KindInt64 && kind != KindFloat64 {
				return Column{}, fmt.Errorf("oracle: AVG(%q) requires a numeric column, got %s", agg.Column, kind)
			}
			return Column{Name: name, Kind: KindFloat64}, nil
		default:
			if kind != KindInt64 && kind != KindFloat64 {
				return Column{}, fmt.Errorf("oracle: aggregate %q requires a numeric column, got %s", name, kind)
			}
			return Column{Name: name, Kind: kind}, nil
		}
	default:
		return Column{}, fmt.Errorf("oracle: unsupported aggregate kind %d", agg.Kind)
	}
}

func (o GroupByOp) apply(t Table) (Table, error) {
	if len(o.Keys) == 0 {
		return Table{}, fmt.Errorf("oracle: group by requires at least one key")
	}
	if len(o.Aggregates) == 0 {
		return Table{}, fmt.Errorf("oracle: group by requires at least one aggregate")
	}
	keyIdx := make([]int, len(o.Keys))
	for i, name := range o.Keys {
		pos, ok := t.ColumnIndex(name)
		if !ok {
			return Table{}, fmt.Errorf("oracle: group column %q not found", name)
		}
		keyIdx[i] = pos
	}
	out := Table{}
	for _, idx := range keyIdx {
		out.Columns = append(out.Columns, t.Columns[idx])
	}
	for _, agg := range o.Aggregates {
		column, err := aggregateOutputColumn(agg, t)
		if err != nil {
			return Table{}, err
		}
		out.Columns = append(out.Columns, column)
	}

	type group struct {
		keyValues []Value
		rows      [][]Value
	}
	groups := make(map[string]*group)
	var order []*group
	for _, row := range t.Rows {
		encoded := groupKeyStrings(row, keyIdx)
		g, ok := groups[encoded]
		if !ok {
			keyValues := make([]Value, len(keyIdx))
			for i, idx := range keyIdx {
				keyValues[i] = row[idx]
			}
			g = &group{keyValues: keyValues}
			groups[encoded] = g
			order = append(order, g)
		}
		g.rows = append(g.rows, row)
	}
	for _, g := range order {
		row := append([]Value(nil), g.keyValues...)
		for _, agg := range o.Aggregates {
			value, err := evalAggregate(agg, t.Columns, g.rows)
			if err != nil {
				return Table{}, err
			}
			row = append(row, value)
		}
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}

// evalAggregate computes one aggregate over the rows of a single group.
// SUM/MIN/MAX/AVG ignore NULL inputs and return SQL NULL when no non-NULL
// input exists; COUNT(*) counts all rows; COUNT(col) counts non-NULL values.
// int64 SUM/AVG accumulate in a checked int64 accumulator and report
// overflow, mirroring the engine. float64 aggregates are an oracle-only
// extension (the engine accepts only int64 aggregate columns): SUM/AVG
// accumulate left to right in float64, and MIN/MAX skip NaN inputs.
func evalAggregate(agg expr.Aggregate, columns []Column, rows [][]Value) (Value, error) {
	if agg.Kind == expr.AggCountRows {
		return Uint64(uint64(len(rows))), nil
	}
	pos, ok := -1, false
	for i, c := range columns {
		if c.Name == agg.Column {
			pos, ok = i, true
			break
		}
	}
	if !ok {
		return Value{}, fmt.Errorf("oracle: aggregate column %q not found", agg.Column)
	}
	kind := columns[pos].Kind
	if agg.Kind == expr.AggCountValues {
		var count uint64
		for _, row := range rows {
			if !row[pos].Null {
				count++
			}
		}
		return Uint64(count), nil
	}

	// count is the number of non-NULL inputs (all values for SUM/AVG, all
	// non-NaN values for float MIN/MAX).
	var count int64
	var i64Sum, i64Min, i64Max int64
	var f64Sum, f64Min, f64Max float64
	for _, row := range rows {
		v := row[pos]
		if v.Null {
			continue
		}
		switch kind {
		case KindInt64:
			switch agg.Kind {
			case expr.AggSum, expr.AggAvg:
				sum := i64Sum + v.I64
				if count > 0 && (i64Sum >= 0) == (v.I64 >= 0) && (sum >= 0) != (i64Sum >= 0) {
					return Value{}, fmt.Errorf("oracle: int64 overflow in %d(%q)", agg.Kind, agg.Column)
				}
				i64Sum = sum
				count++
			case expr.AggMin:
				if count == 0 || v.I64 < i64Min {
					i64Min = v.I64
				}
				count++
			case expr.AggMax:
				if count == 0 || v.I64 > i64Max {
					i64Max = v.I64
				}
				count++
			}
		case KindFloat64:
			switch agg.Kind {
			case expr.AggSum, expr.AggAvg:
				f64Sum += v.F64
				count++
			case expr.AggMin:
				if math.IsNaN(v.F64) {
					continue
				}
				if count == 0 || v.F64 < f64Min {
					f64Min = v.F64
				}
				count++
			case expr.AggMax:
				if math.IsNaN(v.F64) {
					continue
				}
				if count == 0 || v.F64 > f64Max {
					f64Max = v.F64
				}
				count++
			}
		default:
			return Value{}, fmt.Errorf("oracle: %d requires a numeric column, got %s", agg.Kind, kind)
		}
	}
	if count == 0 {
		if agg.Kind == expr.AggAvg {
			return Null(KindFloat64), nil
		}
		return Null(kind), nil
	}
	switch agg.Kind {
	case expr.AggSum:
		if kind == KindInt64 {
			return Int64(i64Sum), nil
		}
		return Float64(f64Sum), nil
	case expr.AggAvg:
		if kind == KindInt64 {
			return Float64(float64(i64Sum) / float64(count)), nil
		}
		return Float64(f64Sum / float64(count)), nil
	case expr.AggMin:
		if kind == KindInt64 {
			return Int64(i64Min), nil
		}
		return Float64(f64Min), nil
	case expr.AggMax:
		if kind == KindInt64 {
			return Int64(i64Max), nil
		}
		return Float64(f64Max), nil
	default:
		return Value{}, fmt.Errorf("oracle: unsupported aggregate kind %d", agg.Kind)
	}
}

// SortOp stably sorts rows by the listed keys; fully equal keys keep their
// input order, matching the engine's ordinal tie-break.
type SortOp struct {
	Keys []SortKey
}

func (SortOp) isOp() {}

// String renders the operator for diagnostics.
func (o SortOp) String() string {
	parts := make([]string, len(o.Keys))
	for i, k := range o.Keys {
		parts[i] = k.String()
	}
	return fmt.Sprintf("sort(%s)", strings.Join(parts, ", "))
}

// compareSortCells mirrors physical/order.go compareSortCells: NULL placement
// applies first and is direction-independent; NaN floats sort greater than
// every non-NaN float; byte order decides strings.
func compareSortCells(a, b Value, nullsLast bool) int {
	if a.Null || b.Null {
		switch {
		case a.Null && b.Null:
			return 0
		case a.Null:
			if nullsLast {
				return 1
			}
			return -1
		default:
			if nullsLast {
				return -1
			}
			return 1
		}
	}
	switch a.Kind {
	case KindInt64:
		return cmpOrdered(a.I64, b.I64)
	case KindUint64:
		return cmpOrdered(a.U64, b.U64)
	case KindFloat64:
		an, bn := math.IsNaN(a.F64), math.IsNaN(b.F64)
		if an || bn {
			switch {
			case an && bn:
				return 0
			case an:
				return 1
			default:
				return -1
			}
		}
		return cmpOrdered(a.F64, b.F64)
	case KindString:
		return strings.Compare(a.Str, b.Str)
	case KindBool:
		return cmpOrdered(boolAsInt(a.Bool), boolAsInt(b.Bool))
	default:
		return 0
	}
}

func (t Table) sortRowComparator(keys []SortKey) (func(a, b []Value) int, error) {
	idx := make([]int, len(keys))
	for i, k := range keys {
		pos, ok := t.ColumnIndex(k.Column)
		if !ok {
			return nil, fmt.Errorf("oracle: sort column %q not found", k.Column)
		}
		idx[i] = pos
	}
	return func(a, b []Value) int {
		for i, k := range keys {
			va, vb := a[idx[i]], b[idx[i]]
			cmp := compareSortCells(va, vb, k.NullsLast)
			if cmp != 0 && !va.Null && !vb.Null && k.Descending {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	}, nil
}

func (o SortOp) apply(t Table) (Table, error) {
	if len(o.Keys) == 0 {
		return Table{}, fmt.Errorf("oracle: sort requires at least one key")
	}
	cmp, err := t.sortRowComparator(o.Keys)
	if err != nil {
		return Table{}, err
	}
	out := Table{Columns: t.Columns, Rows: append([][]Value(nil), t.Rows...)}
	sort.SliceStable(out.Rows, func(i, j int) bool { return cmp(out.Rows[i], out.Rows[j]) < 0 })
	return out, nil
}

// TopKOp returns the best K rows under the given ordering. Because the
// engine's TopK breaks ties by global input ordinal, it is exactly stable
// sort followed by Limit, which is how the oracle implements it.
type TopKOp struct {
	K    int
	Keys []SortKey
}

func (TopKOp) isOp() {}

// String renders the operator for diagnostics.
func (o TopKOp) String() string {
	parts := make([]string, len(o.Keys))
	for i, k := range o.Keys {
		parts[i] = k.String()
	}
	return fmt.Sprintf("top_k(%d, %s)", o.K, strings.Join(parts, ", "))
}

func (o TopKOp) apply(t Table) (Table, error) {
	if o.K < 0 {
		return Table{}, fmt.Errorf("oracle: top-k requires a non-negative K")
	}
	sorted, err := (SortOp{Keys: o.Keys}).apply(t)
	if err != nil {
		return Table{}, err
	}
	return (LimitOp{N: int64(o.K)}).apply(sorted)
}

// LimitOp keeps the first N rows, preserving their order.
type LimitOp struct {
	N int64
}

func (LimitOp) isOp() {}

// String renders the operator for diagnostics.
func (o LimitOp) String() string { return fmt.Sprintf("limit(%d)", o.N) }

func (o LimitOp) apply(t Table) (Table, error) {
	if o.N < 0 {
		return Table{}, fmt.Errorf("oracle: limit requires a non-negative row count")
	}
	out := Table{Columns: t.Columns}
	n := o.N
	if n > int64(len(t.Rows)) {
		n = int64(len(t.Rows))
	}
	out.Rows = append(out.Rows, t.Rows[:n]...)
	return out, nil
}

// Query is an ordered pipeline of relational operators evaluated against one
// input table. ForceUnordered is generator metadata marking pipelines whose
// row order is not semantically defined even though a Sort or TopK appears
// after a GroupBy (a Select dropped group key columns, so no total ordering
// could be constructed).
type Query struct {
	Ops            []Op
	ForceUnordered bool
}

// Execute runs the pipeline against t and returns the result table.
func (q Query) Execute(t Table) (Table, error) {
	if err := t.Validate(); err != nil {
		return Table{}, err
	}
	current := t
	for i, op := range q.Ops {
		next, err := op.apply(current)
		if err != nil {
			return Table{}, fmt.Errorf("oracle: op %d (%s): %w", i, op, err)
		}
		current = next
	}
	return current, nil
}

// Ordered reports whether the query output row order is semantically
// defined: scans, filters, projections, and limits preserve order, sorts and
// top-k establish it, and group-by destroys it (the engine emits groups in an
// unspecified order). Differential comparisons must use order-insensitive
// comparison when Ordered is false.
func (q Query) Ordered() bool {
	if q.ForceUnordered {
		return false
	}
	ordered := true
	for _, op := range q.Ops {
		switch op.(type) {
		case GroupByOp:
			ordered = false
		case SortOp, TopKOp:
			ordered = true
		}
	}
	return ordered
}

// String renders the pipeline for failure diagnostics.
func (q Query) String() string {
	if len(q.Ops) == 0 {
		return "scan"
	}
	parts := make([]string, len(q.Ops))
	for i, op := range q.Ops {
		parts[i] = op.String()
	}
	return strings.Join(parts, " -> ")
}
