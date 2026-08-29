package logical

import (
	"fmt"
	"gocolumn/expr"
	"gocolumn/source"
	"strings"
)

// Explain renders plan as a stable, indented tree intended for diagnostics and
// optimizer tests rather than machine parsing.
func Explain(plan Plan) string {
	var b strings.Builder
	writePlan(&b, plan, 0)
	return strings.TrimRight(b.String(), "\n")
}

func writePlan(b *strings.Builder, plan Plan, depth int) {
	indent := strings.Repeat("  ", depth)
	switch n := plan.(type) {
	case *Scan:
		desc := fmt.Sprintf("%T", n.Source)
		if d, ok := n.Source.(source.Describer); ok {
			desc = d.Description()
		}
		cols := "ALL"
		if len(n.Projection) != 0 {
			cols = "[" + strings.Join(n.Projection, ", ") + "]"
		}
		fmt.Fprintf(b, "%sSCAN source=%s columns=%s", indent, desc, cols)
		if n.PushedPredicate != nil {
			fmt.Fprintf(b, " pushed_predicate=%s", n.PushedPredicate)
		}
		if n.Pushdown.TotalPartitions > 0 {
			fmt.Fprintf(b, " partitions=%d/%d rows=%d/%d", n.Pushdown.SelectedPartitions, n.Pushdown.TotalPartitions, n.Pushdown.SelectedRows, n.Pushdown.TotalRows)
		}
		b.WriteByte('\n')
	case *Filter:
		fmt.Fprintf(b, "%sFILTER %s\n", indent, n.Predicate)
		writePlan(b, n.Input, depth+1)
	case *Projection:
		fmt.Fprintf(b, "%sPROJECTION [%s]\n", indent, strings.Join(n.Columns, ", "))
		writePlan(b, n.Input, depth+1)
	case *ExprProjection:
		items := make([]string, 0, len(n.Exprs))
		for _, e := range n.Exprs {
			name, _ := e.Name()
			items = append(items, fmt.Sprintf("%s AS %s", e.Expr, name))
		}
		kind := "SELECT_EXPRS"
		if n.IncludeInput {
			kind = "WITH_COLUMNS"
		}
		fmt.Fprintf(b, "%s%s [%s]\n", indent, kind, strings.Join(items, ", "))
		writePlan(b, n.Input, depth+1)
	case *Join:
		keys := make([]string, 0, len(n.Keys))
		for _, k := range n.Keys {
			keys = append(keys, k.Left+"="+k.Right)
		}
		fmt.Fprintf(b, "%sJOIN type=%s keys=[%s] suffix=%q", indent, n.Type, strings.Join(keys, ", "), n.Suffix)
		if n.RadixBits != 0 {
			fmt.Fprintf(b, " radix_bits=%d workers=%d", n.RadixBits, n.Workers)
		}
		b.WriteByte('\n')
		writePlan(b, n.Left, depth+1)
		writePlan(b, n.Right, depth+1)
	case *Sort:
		keys := make([]string, 0, len(n.By))
		for _, k := range n.By {
			dir := "asc"
			if k.Descending {
				dir = "desc"
			}
			nulls := "nulls-first"
			if k.NullsLast {
				nulls = "nulls-last"
			}
			keys = append(keys, fmt.Sprintf("%s %s %s", sortKeyString(k), dir, nulls))
		}
		fmt.Fprintf(b, "%sSORT [%s]\n", indent, strings.Join(keys, ", "))
		writePlan(b, n.Input, depth+1)
	case *TopK:
		keys := make([]string, 0, len(n.By))
		for _, k := range n.By {
			dir := "asc"
			if k.Descending {
				dir = "desc"
			}
			nulls := "nulls-first"
			if k.NullsLast {
				nulls = "nulls-last"
			}
			keys = append(keys, fmt.Sprintf("%s %s %s", sortKeyString(k), dir, nulls))
		}
		fmt.Fprintf(b, "%sTOPK k=%d [%s]\n", indent, n.K, strings.Join(keys, ", "))
		writePlan(b, n.Input, depth+1)
	case *Limit:
		fmt.Fprintf(b, "%sLIMIT %d\n", indent, n.N)
		writePlan(b, n.Input, depth+1)
	case *Aggregate:
		aggs := make([]string, 0, len(n.Aggregates))
		for _, a := range n.Aggregates {
			aggs = append(aggs, aggregateString(a))
		}
		fmt.Fprintf(b, "%sAGGREGATE keys=[%s] aggs=[%s]\n", indent, strings.Join(n.GroupBy, ", "), strings.Join(aggs, ", "))
		writePlan(b, n.Input, depth+1)
	case *Window:
		order := make([]string, 0, len(n.Spec.By))
		for _, key := range n.Spec.By {
			direction := "asc"
			if key.Descending {
				direction = "desc"
			}
			nulls := "nulls-first"
			if key.NullsLast {
				nulls = "nulls-last"
			}
			order = append(order, fmt.Sprintf("%s %s %s", sortKeyString(key), direction, nulls))
		}
		items := make([]string, 0, len(n.Exprs))
		for _, window := range n.Exprs {
			items = append(items, windowExprString(window))
		}
		fmt.Fprintf(b, "%sWINDOW partition_by=[%s] order_by=[%s] outputs=[%s]\n", indent, strings.Join(n.Spec.PartitionBy, ", "), strings.Join(order, ", "), strings.Join(items, ", "))
		writePlan(b, n.Input, depth+1)
	default:
		fmt.Fprintf(b, "%s<unknown %T>\n", indent, plan)
	}
}

func windowExprString(window WindowExpr) string {
	var name string
	switch window.Function {
	case WindowRowNumber:
		name = "row_number()"
	case WindowRank:
		name = "rank()"
	case WindowDenseRank:
		name = "dense_rank()"
	case WindowCountRows:
		name = "count_rows(" + frameString(window.Frame) + ")"
	case WindowCountValues:
		name = "count(" + window.Column + ", " + frameString(window.Frame) + ")"
	case WindowSum:
		name = "sum(" + window.Column + ", " + frameString(window.Frame) + ")"
	case WindowMin:
		name = "min(" + window.Column + ", " + frameString(window.Frame) + ")"
	case WindowMax:
		name = "max(" + window.Column + ", " + frameString(window.Frame) + ")"
	case WindowAvg:
		name = "avg(" + window.Column + ", " + frameString(window.Frame) + ")"
	default:
		name = fmt.Sprintf("window(%d)", window.Function)
	}
	return name + " AS " + window.Alias
}

func frameString(frame RowsFrame) string {
	return "ROWS BETWEEN " + boundString(frame.Start) + " AND " + boundString(frame.End)
}

func boundString(bound RowBound) string {
	switch bound.Kind {
	case RowUnboundedPreceding:
		return "UNBOUNDED PRECEDING"
	case RowPreceding:
		return fmt.Sprintf("%d PRECEDING", bound.Offset)
	case RowCurrent:
		return "CURRENT ROW"
	case RowFollowing:
		return fmt.Sprintf("%d FOLLOWING", bound.Offset)
	case RowUnboundedFollowing:
		return "UNBOUNDED FOLLOWING"
	default:
		return fmt.Sprintf("<invalid %d>", bound.Kind)
	}
}

func sortKeyString(key SortKey) string {
	value, err := key.Expression()
	if err != nil {
		return "<invalid: " + err.Error() + ">"
	}
	return value.String()
}

func aggregateString(a expr.Aggregate) string {
	var fn string
	switch a.Kind {
	case expr.AggSum:
		fn = "sum(" + a.Column + ")"
	case expr.AggCountRows:
		fn = "count(*)"
	case expr.AggCountValues:
		fn = "count(" + a.Column + ")"
	case expr.AggMin:
		fn = "min(" + a.Column + ")"
	case expr.AggMax:
		fn = "max(" + a.Column + ")"
	case expr.AggAvg:
		fn = "avg(" + a.Column + ")"
	default:
		fn = fmt.Sprintf("agg(%d,%s)", a.Kind, a.Column)
	}
	if a.Alias != "" {
		return fn + " AS " + a.Alias
	}
	return fn
}
