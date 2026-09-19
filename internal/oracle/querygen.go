package oracle

import (
	"fmt"
	"math"
	"strconv"

	"gocolumn/expr"
)

// Query generation respects the engine's documented restrictions so that
// every generated query is executable by both the oracle and the engine:
//
//   - group keys are int64 or string columns; a string key is only used as a
//     single key, pairs are always int64 (engine/compile.go);
//   - all value aggregates of one query share a single int64 input column,
//     and SUM/AVG only target bounded columns so the engine's checked int64
//     accumulators cannot overflow;
//
// Query generation respects the engine's documented restrictions so that
// every generated query is executable by both the oracle and the engine:
//
//   - group keys are int64 or string columns; a string key is only used as a
//     single key, pairs are always int64 (engine/compile.go);
//   - all value aggregates of one query share a single int64 input column,
//     and SUM/AVG only target bounded columns so the engine's checked int64
//     accumulators cannot overflow;
//   - +, -, * on int64 only combine bounded columns and small literals, so
//     checked arithmetic cannot overflow;
//   - there is no not-equals node in the engine expression AST, so != is
//     generated as NOT (a = b), which is exactly SQL three-valued <>.
//
// Determinism constraints: group rows are emitted in an unspecified order, so
// after a GroupBy a Sort/TopK always prepends the (unique) group key columns
// to make the ordering total, and Limit is only emitted once such a total
// ordering exists. Earlier revisions of this generator avoided bool sort
// payloads, Select before GroupBy/Project, and TopK after sort-class
// operators; those engine bugs are fixed and the shapes are now exercised
// deliberately.
func (g *Generator) Query(t Table) Query {
	cols := append([]Column(nil), t.Columns...)
	var ops []Op
	steps := 1 + g.r.Intn(4)
	grouped := false
	sortedAfterGroup := false
	limited := false
	groupKeysIntact := false
	forceUnordered := false
	var groupKeys []string
	for i := 0; i < steps; i++ {
		choice := g.pickOp(cols, grouped)
		switch choice {
		case "filter":
			ops = append(ops, FilterOp{Predicate: g.predicate(t, cols, 2)})
		case "select":
			sel, next := g.selectOp(cols)
			ops = append(ops, sel)
			cols = next
			if grouped && groupKeysIntact {
				// A Select may drop group key columns; without them no total
				// ordering of the group output can be constructed.
				for _, name := range groupKeys {
					if _, ok := columnIndexOf(next, name); !ok {
						groupKeysIntact = false
						sortedAfterGroup = false
						forceUnordered = true
						break
					}
				}
			}
		case "project":
			project, next := g.projectOp(cols, grouped)
			ops = append(ops, project)
			cols = next
			if grouped && groupKeysIntact {
				// Replacing a group key column destroys the value uniqueness
				// that makes post-GroupBy orderings total.
				for _, e := range project.Exprs {
					name, _ := e.Name()
					for _, key := range groupKeys {
						if name == key {
							groupKeysIntact = false
							sortedAfterGroup = false
							forceUnordered = true
						}
					}
				}
			}
		case "groupby":
			group, next, ok := g.groupByOp(cols)
			if !ok {
				continue
			}
			ops = append(ops, group)
			cols = next
			grouped = true
			groupKeys = append([]string(nil), group.Keys...)
			groupKeysIntact = true
			// Grouping destroys row order; a Limit is only meaningful again
			// after a new total ordering on the group output.
			sortedAfterGroup = false
		case "sort":
			keys := g.sortKeys(cols, 1+g.r.Intn(minInt(3, len(cols))))
			if grouped {
				keys = prependGroupKeys(cols, groupKeys, keys)
				if !groupKeysIntact {
					// Without all group keys the ordering is not total;
					// downstream comparison must stay order-insensitive.
					forceUnordered = true
				}
			}
			if len(keys) == 0 {
				continue
			}
			ops = append(ops, SortOp{Keys: keys})
			sortedAfterGroup = true
		case "topk":
			if grouped && !groupKeysIntact {
				// TopK truncates; tie ambiguity at the K boundary is only
				// deterministic when the unique group keys order the output.
				continue
			}
			keys := g.sortKeys(cols, 1+g.r.Intn(minInt(2, len(cols))))
			if grouped {
				keys = prependGroupKeys(cols, groupKeys, keys)
			}
			if len(keys) == 0 {
				continue
			}
			ops = append(ops, TopKOp{K: g.r.Intn(len(t.Rows) + 3), Keys: keys})
			sortedAfterGroup = true
		case "limit":
			// Without a total ordering after a GroupBy, a Limit would keep an
			// implementation-defined subset of groups.
			if grouped && !sortedAfterGroup {
				continue
			}
			ops = append(ops, LimitOp{N: int64(g.r.Intn(len(t.Rows) + 3))})
			limited = true
		}
	}
	if len(ops) == 0 {
		ops = append(ops, LimitOp{N: int64(g.r.Intn(len(t.Rows) + 3))})
		limited = true
	}
	// Half of the grouped pipelines get a final ordering over their output
	// columns so the differential test also exercises order-sensitive group
	// comparisons; the rest exercise the order-insensitive path.
	if grouped && !sortedAfterGroup && g.r.Intn(2) == 0 {
		keys := prependGroupKeys(cols, groupKeys, g.sortKeys(cols, 1+g.r.Intn(minInt(2, len(cols)))))
		if len(keys) > 0 {
			ops = append(ops, SortOp{Keys: keys})
		}
	} else if !grouped && !limited && g.r.Intn(4) == 0 {
		ops = append(ops, LimitOp{N: int64(g.r.Intn(len(t.Rows) + 3))})
	}
	return Query{Ops: ops, ForceUnordered: forceUnordered}
}

// prependGroupKeys puts the unique group key columns ahead of the chosen sort
// keys, making the ordering total on group output. Group keys dropped by an
// intervening Select are skipped.
func prependGroupKeys(cols []Column, groupKeys []string, keys []SortKey) []SortKey {
	seen := make(map[string]struct{}, len(groupKeys)+len(keys))
	out := make([]SortKey, 0, len(groupKeys)+len(keys))
	for _, name := range groupKeys {
		if _, ok := columnIndexOf(cols, name); !ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, SortKey{Column: name})
	}
	for _, k := range keys {
		if _, dup := seen[k.Column]; dup {
			continue
		}
		seen[k.Column] = struct{}{}
		out = append(out, k)
	}
	return out
}

// pickOp chooses the next operator kind. Group-by is offered at most once;
// operators that cannot apply to the current schema are skipped.
func (g *Generator) pickOp(cols []Column, grouped bool) string {
	type weighted struct {
		name   string
		weight int
	}
	choices := []weighted{{"filter", 30}, {"select", 12}, {"project", 15}, {"limit", 12}}
	if !grouped {
		choices = append(choices, weighted{"groupby", 22})
	}
	if countOrderable(cols) > 0 {
		choices = append(choices, weighted{"sort", 12}, weighted{"topk", 6})
	}
	total := 0
	for _, c := range choices {
		total += c.weight
	}
	n := g.r.Intn(total)
	for _, c := range choices {
		if n < c.weight {
			return c.name
		}
		n -= c.weight
	}
	return "filter"
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func countOrderable(cols []Column) int {
	n := 0
	for _, c := range cols {
		if orderable(c.Kind) {
			n++
		}
	}
	return n
}

// orderable reports whether the engine's sort supports the kind as a key
// (bool is orderable: false sorts before true).
func orderable(k Kind) bool {
	switch k {
	case KindInt64, KindUint64, KindFloat64, KindString, KindBool:
		return true
	default:
		return false
	}
}

// selectOp picks a random non-empty column subset, possibly reordered.
func (g *Generator) selectOp(cols []Column) (SelectOp, []Column) {
	n := 1 + g.r.Intn(len(cols))
	perm := g.r.Perm(len(cols))[:n]
	out := SelectOp{Columns: make([]string, n)}
	next := make([]Column, n)
	for i, idx := range perm {
		out.Columns[i] = cols[idx].Name
		next[i] = cols[idx]
	}
	return out, next
}

// projectOp builds a SelectExprs or WithColumns projection with 1-3
// expressions: renames, checked int64 arithmetic over bounded operands,
// float64 arithmetic and division, and string concatenation. After a GroupBy
// only WithColumns is generated so group key columns stay available for
// total-order sorting. Aliases restart from p0 at every projection, so
// consecutive WithColumns exercise the engine's in-place column replacement.
func (g *Generator) projectOp(cols []Column, grouped bool) (ProjectOp, []Column) {
	n := 1 + g.r.Intn(3)
	includeInput := grouped || g.r.Intn(5) < 2
	op := ProjectOp{IncludeInput: includeInput, Exprs: make([]expr.NamedExpr, 0, n)}
	var next []Column
	if op.IncludeInput {
		next = append(next, cols...)
	}
	for i := 0; i < n; i++ {
		alias := "p" + strconv.Itoa(i)
		e, kind, bounded := g.projectExpr(cols)
		op.Exprs = append(op.Exprs, expr.Named(e, alias))
		// A repeated alias replaces the earlier column in place, matching
		// both the oracle and the engine's WithColumns semantics.
		replaced := false
		for j := range next {
			if next[j].Name == alias {
				next[j] = Column{Name: alias, Kind: kind, Bounded: bounded}
				replaced = true
				break
			}
		}
		if !replaced {
			next = append(next, Column{Name: alias, Kind: kind, Bounded: bounded})
		}
	}
	return op, next
}

func (g *Generator) projectExpr(cols []Column) (expr.Expr, Kind, bool) {
	type candidate func() (expr.Expr, Kind, bool)
	var candidates []candidate
	for _, c := range cols {
		c := c
		candidates = append(candidates, func() (expr.Expr, Kind, bool) {
			return expr.Col(c.Name), c.Kind, c.Bounded
		})
	}
	boundedInts := columnsOf(cols, func(c Column) bool { return c.Kind == KindInt64 && c.Bounded })
	if len(boundedInts) > 0 {
		candidates = append(candidates, func() (expr.Expr, Kind, bool) {
			left := boundedInts[g.r.Intn(len(boundedInts))]
			var right expr.Expr
			if len(boundedInts) > 1 && g.r.Intn(2) == 0 {
				other := boundedInts[g.r.Intn(len(boundedInts))]
				right = expr.Col(other.Name)
			} else {
				right = expr.Lit(int64(g.r.Intn(2001) - 1000))
			}
			op := []expr.Op{expr.OpAdd, expr.OpSub, expr.OpMul}[g.r.Intn(3)]
			return expr.Binary{Op: op, Left: expr.Col(left.Name), Right: right}, KindInt64, true
		})
	}
	numerics := columnsOf(cols, func(c Column) bool {
		return c.Kind == KindInt64 || c.Kind == KindUint64 || c.Kind == KindFloat64
	})
	if len(numerics) > 0 {
		// Division always produces float64 in the engine, including int/int.
		candidates = append(candidates, func() (expr.Expr, Kind, bool) {
			left := numerics[g.r.Intn(len(numerics))]
			right := expr.Lit(int64(1 + g.r.Intn(9)))
			return expr.Div(expr.Col(left.Name), right), KindFloat64, false
		})
	}
	floats := columnsOf(cols, func(c Column) bool { return c.Kind == KindFloat64 })
	if len(floats) > 0 {
		candidates = append(candidates, func() (expr.Expr, Kind, bool) {
			left := floats[g.r.Intn(len(floats))]
			var right expr.Expr
			switch g.r.Intn(3) {
			case 0:
				right = expr.F64(g.r.NormFloat64() * 100)
			case 1:
				right = expr.Col(floats[g.r.Intn(len(floats))].Name)
			default:
				right = expr.Lit(int64(g.r.Intn(2001) - 1000))
			}
			op := []expr.Op{expr.OpAdd, expr.OpSub, expr.OpMul, expr.OpDiv}[g.r.Intn(4)]
			return expr.Binary{Op: op, Left: expr.Col(left.Name), Right: right}, KindFloat64, false
		})
	}
	strings := columnsOf(cols, func(c Column) bool { return c.Kind == KindString })
	if len(strings) > 0 {
		candidates = append(candidates, func() (expr.Expr, Kind, bool) {
			left := strings[g.r.Intn(len(strings))]
			var right expr.Expr
			if g.r.Intn(2) == 0 {
				right = expr.Col(strings[g.r.Intn(len(strings))].Name)
			} else {
				right = expr.Str(stringPool[g.r.Intn(len(stringPool))])
			}
			return expr.Concat(expr.Col(left.Name), right), KindString, false
		})
	}
	c := candidates[g.r.Intn(len(candidates))]
	return c()
}

func columnsOf(cols []Column, match func(Column) bool) []Column {
	var out []Column
	for _, c := range cols {
		if match(c) {
			out = append(out, c)
		}
	}
	return out
}

// groupByOp builds a GroupBy legal for the engine: one or two int64 keys or a
// single string key, COUNT(*) plus value aggregates sharing one int64 column,
// with SUM/AVG restricted to bounded columns.
func (g *Generator) groupByOp(cols []Column) (GroupByOp, []Column, bool) {
	intKeys := columnsOf(cols, func(c Column) bool { return c.Kind == KindInt64 })
	stringKeys := columnsOf(cols, func(c Column) bool { return c.Kind == KindString })
	var keys []string
	if len(intKeys) >= 2 && g.r.Intn(2) == 0 {
		first := g.r.Intn(len(intKeys))
		second := g.r.Intn(len(intKeys) - 1)
		if second >= first {
			second++
		}
		keys = []string{intKeys[first].Name, intKeys[second].Name}
	} else {
		var eligible []Column
		eligible = append(eligible, intKeys...)
		eligible = append(eligible, stringKeys...)
		if len(eligible) == 0 {
			return GroupByOp{}, nil, false
		}
		keys = []string{eligible[g.r.Intn(len(eligible))].Name}
	}

	var aggs []expr.Aggregate
	alias := func(i int) string { return "a" + strconv.Itoa(i) }
	intCols := columnsOf(cols, func(c Column) bool { return c.Kind == KindInt64 })
	if g.r.Intn(2) == 0 || len(intCols) == 0 {
		aggs = append(aggs, expr.Count(alias(0)))
	}
	if len(intCols) > 0 {
		value := intCols[g.r.Intn(len(intCols))]
		choices := []expr.Aggregate{expr.CountColumn(value.Name, alias(len(aggs))), expr.Min(value.Name, alias(len(aggs)+1)), expr.Max(value.Name, alias(len(aggs)+2))}
		if value.Bounded {
			choices = append(choices, expr.Sum(value.Name, alias(len(aggs)+3)), expr.Avg(value.Name, alias(len(aggs)+4)))
		}
		perm := g.r.Perm(len(choices))
		take := 1 + g.r.Intn(minInt(3, len(choices)))
		for _, idx := range perm[:take] {
			aggs = append(aggs, choices[idx])
		}
	}
	if len(aggs) == 0 {
		return GroupByOp{}, nil, false
	}

	op := GroupByOp{Keys: keys, Aggregates: aggs}
	var next []Column
	for _, name := range keys {
		idx, _ := columnIndexOf(cols, name)
		next = append(next, cols[idx])
	}
	for _, a := range aggs {
		switch a.Kind {
		case expr.AggCountRows, expr.AggCountValues:
			next = append(next, Column{Name: a.Alias, Kind: KindUint64, Bounded: true})
		case expr.AggAvg:
			next = append(next, Column{Name: a.Alias, Kind: KindFloat64})
		default:
			idx, _ := columnIndexOf(cols, a.Column)
			next = append(next, Column{Name: a.Alias, Kind: KindInt64, Bounded: cols[idx].Bounded})
		}
	}
	return op, next, true
}

func columnIndexOf(cols []Column, name string) (int, bool) {
	for i, c := range cols {
		if c.Name == name {
			return i, true
		}
	}
	return -1, false
}

// sortKeys picks n distinct orderable columns with random direction and NULL
// placement.
func (g *Generator) sortKeys(cols []Column, n int) []SortKey {
	eligible := columnsOf(cols, func(c Column) bool { return orderable(c.Kind) })
	if len(eligible) == 0 || n == 0 {
		return nil
	}
	if n > len(eligible) {
		n = len(eligible)
	}
	perm := g.r.Perm(len(eligible))[:n]
	keys := make([]SortKey, n)
	for i, idx := range perm {
		keys[i] = SortKey{Column: eligible[idx].Name, Descending: g.r.Intn(2) == 0, NullsLast: g.r.Intn(2) == 0}
	}
	return keys
}

// predicate builds a random predicate over cols with bounded boolean depth.
// Literals are sampled from base table data when possible so predicates are
// selective instead of trivially false.
func (g *Generator) predicate(base Table, cols []Column, depth int) expr.Expr {
	if depth > 0 && g.r.Intn(10) < 4 {
		left := g.predicate(base, cols, depth-1)
		switch g.r.Intn(5) {
		case 0:
			return expr.Not(left)
		default:
			right := g.predicate(base, cols, depth-1)
			if g.r.Intn(2) == 0 {
				return expr.And(left, right)
			}
			return expr.Or(left, right)
		}
	}
	if g.r.Intn(8) == 0 {
		c := cols[g.r.Intn(len(cols))]
		if g.r.Intn(2) == 0 {
			return expr.IsNull(expr.Col(c.Name))
		}
		return expr.IsNotNull(expr.Col(c.Name))
	}
	return g.comparison(base, cols)
}

// comparison builds one column/literal or column/column comparison.
func (g *Generator) comparison(base Table, cols []Column) expr.Expr {
	c := cols[g.r.Intn(len(cols))]
	switch c.Kind {
	case KindInt64:
		if other, ok := g.peerColumn(cols, c, KindInt64); ok && g.r.Intn(6) == 0 {
			return g.compareOp(expr.Col(c.Name), expr.Col(other.Name))
		}
		left := expr.Col(c.Name)
		var right expr.Expr = expr.Lit(g.intLiteral(base, c.Name))
		if g.r.Intn(4) == 0 {
			// != is spelled NOT (a = b): the engine AST has no OpNE.
			return expr.Not(expr.EQ(left, right))
		}
		return g.compareOp(left, right)
	case KindUint64:
		return g.compareOp(expr.Col(c.Name), expr.Lit(int64(g.r.Intn(8))))
	case KindFloat64:
		if other, ok := g.peerColumn(cols, c, KindFloat64); ok && g.r.Intn(6) == 0 {
			return g.compareOp(expr.Col(c.Name), expr.Col(other.Name))
		}
		return g.compareOp(expr.Col(c.Name), expr.F64(g.floatLiteral(base, c.Name)))
	case KindString:
		return g.compareOp(expr.Col(c.Name), expr.Str(g.stringLiteral(base, c.Name)))
	case KindBool:
		// Bools compile through the engine's unsigned comparison family; only
		// equality-style literal comparisons against 0/1 are generated.
		return g.compareOp(expr.Col(c.Name), expr.Lit(int64(g.r.Intn(2))))
	default:
		return expr.IsNotNull(expr.Col(c.Name))
	}
}

// peerColumn finds a different column of the same kind for column/column
// comparisons.
func (g *Generator) peerColumn(cols []Column, c Column, kind Kind) (Column, bool) {
	peers := columnsOf(cols, func(o Column) bool { return o.Kind == kind && o.Name != c.Name })
	if len(peers) == 0 {
		return Column{}, false
	}
	return peers[g.r.Intn(len(peers))], true
}

// compareOp picks one of =, <, <=, >, >= between the given operands.
func (g *Generator) compareOp(left, right expr.Expr) expr.Expr {
	switch g.r.Intn(5) {
	case 0:
		return expr.EQ(left, right)
	case 1:
		return expr.LT(left, right)
	case 2:
		return expr.LE(left, right)
	case 3:
		return expr.GT(left, right)
	default:
		return expr.GE(left, right)
	}
}

// intLiteral samples a value from the base table column when possible, else
// draws from the int64 domain including extremes.
func (g *Generator) intLiteral(base Table, name string) int64 {
	if v, ok := g.sampleColumnValue(base, name); ok && !v.Null && v.Kind == KindInt64 {
		return v.I64
	}
	if g.r.Intn(4) == 0 {
		return int64Extremes[g.r.Intn(len(int64Extremes))]
	}
	return g.r.Int63n(2001) - 1000
}

// floatLiteral samples a non-NaN value from the base table column when
// possible, else draws a moderate normal value.
func (g *Generator) floatLiteral(base Table, name string) float64 {
	if v, ok := g.sampleColumnValue(base, name); ok && !v.Null && v.Kind == KindFloat64 && !math.IsNaN(v.F64) {
		return v.F64
	}
	return g.r.NormFloat64() * 100
}

// stringLiteral samples a value from the base table column when possible,
// else draws from the string pool.
func (g *Generator) stringLiteral(base Table, name string) string {
	if v, ok := g.sampleColumnValue(base, name); ok && !v.Null && v.Kind == KindString {
		return v.Str
	}
	return stringPool[g.r.Intn(len(stringPool))]
}

// sampleColumnValue returns a random non-NULL cell of the named base table
// column, or false when the column is absent or has no non-NULL values.
func (g *Generator) sampleColumnValue(base Table, name string) (Value, bool) {
	idx, ok := base.ColumnIndex(name)
	if !ok || len(base.Rows) == 0 {
		return Value{}, false
	}
	for range base.Rows {
		v := base.Rows[g.r.Intn(len(base.Rows))][idx]
		if !v.Null {
			return v, true
		}
	}
	return Value{}, false
}

// Describe renders a generated query with its seed for failure reports.
func Describe(seed int64, queryIndex int, q Query) string {
	return fmt.Sprintf("seed=%d query=%d: %s", seed, queryIndex, q)
}
