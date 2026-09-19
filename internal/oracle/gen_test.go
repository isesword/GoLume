package oracle

import (
	"math"
	"testing"
)

func TestGeneratorDeterministic(t *testing.T) {
	const seed = 42
	a, b := NewGenerator(seed), NewGenerator(seed)
	ta, tb := a.Table(TableOptions{}), b.Table(TableOptions{})
	if err := CompareTables(ta, tb, true); err != nil {
		t.Fatalf("same seed produced different tables: %v", err)
	}
	for i := 0; i < 20; i++ {
		qa, qb := a.Query(ta), b.Query(tb)
		if qa.String() != qb.String() {
			t.Fatalf("query %d diverged:\n%s\n%s", i, qa, qb)
		}
	}
}

func TestGeneratorValueDomainCoverage(t *testing.T) {
	var (
		sawEmptyTable, sawSingleRow, sawAllNullColumn        bool
		sawMinInt, sawMaxInt, sawNegative                    bool
		sawNaN, sawPosInf, sawNegInf, sawHuge, sawSignedZero bool
		sawEmptyString, sawUnicode                           bool
		sawTrue, sawFalse                                    bool
		sawKinds                                             = map[Kind]bool{}
	)
	for seed := int64(0); seed < 400; seed++ {
		g := NewGenerator(seed)
		table := g.Table(TableOptions{})
		if err := table.Validate(); err != nil {
			t.Fatalf("seed %d: invalid table: %v", seed, err)
		}
		switch table.NumRows() {
		case 0:
			sawEmptyTable = true
		case 1:
			sawSingleRow = true
		}
		for _, c := range table.Columns {
			sawKinds[c.Kind] = true
			if table.NumRows() > 0 {
				allNull := true
				for _, row := range table.Rows {
					if !row[table.columnIndexMust(c.Name)].Null {
						allNull = false
						break
					}
				}
				sawAllNullColumn = sawAllNullColumn || allNull
			}
		}
		for _, row := range table.Rows {
			for _, cell := range row {
				if cell.Null {
					continue
				}
				switch cell.Kind {
				case KindInt64:
					sawMinInt = sawMinInt || cell.I64 == math.MinInt64
					sawMaxInt = sawMaxInt || cell.I64 == math.MaxInt64
					sawNegative = sawNegative || cell.I64 < 0
				case KindFloat64:
					sawNaN = sawNaN || math.IsNaN(cell.F64)
					sawPosInf = sawPosInf || math.IsInf(cell.F64, 1)
					sawNegInf = sawNegInf || math.IsInf(cell.F64, -1)
					sawHuge = sawHuge || math.Abs(cell.F64) == math.MaxFloat64
					sawSignedZero = sawSignedZero || cell.F64 == 0 && math.Signbit(cell.F64)
				case KindString:
					sawEmptyString = sawEmptyString || cell.Str == ""
					sawUnicode = sawUnicode || cell.Str == "张三" || cell.Str == "🙂🚀"
				case KindBool:
					sawTrue = sawTrue || cell.Bool
					sawFalse = sawFalse || !cell.Bool
				}
			}
		}
	}
	checks := map[string]bool{
		"empty table": sawEmptyTable, "single row": sawSingleRow, "all-null column": sawAllNullColumn,
		"MinInt64": sawMinInt, "MaxInt64": sawMaxInt, "negative": sawNegative,
		"NaN": sawNaN, "+Inf": sawPosInf, "-Inf": sawNegInf, "MaxFloat64": sawHuge, "-0.0": sawSignedZero,
		"empty string": sawEmptyString, "unicode": sawUnicode, "true": sawTrue, "false": sawFalse,
		"int64 column": sawKinds[KindInt64], "float64 column": sawKinds[KindFloat64],
		"string column": sawKinds[KindString], "bool column": sawKinds[KindBool],
	}
	for name, ok := range checks {
		if !ok {
			t.Errorf("generator never produced: %s", name)
		}
	}
}

// columnIndexMust fails the enclosing test via panic; generator tests call it
// only with names taken from the table itself.
func (t Table) columnIndexMust(name string) int {
	idx, ok := t.ColumnIndex(name)
	if !ok {
		panic("column vanished: " + name)
	}
	return idx
}

func TestGeneratorBoundedColumnInvariant(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		table := NewGenerator(seed).Table(TableOptions{})
		for col, c := range table.Columns {
			if !c.Bounded {
				continue
			}
			if c.Kind != KindInt64 {
				t.Fatalf("seed %d: non-int64 column marked bounded", seed)
			}
			for row, r := range table.Rows {
				v := r[col]
				if !v.Null && (v.I64 > GeneratorBoundedMagnitude || v.I64 < -GeneratorBoundedMagnitude) {
					t.Fatalf("seed %d: bounded column %q row %d has %d", seed, c.Name, row, v.I64)
				}
			}
		}
	}
}

func TestGeneratorQueriesExecuteOnOracle(t *testing.T) {
	opCounts := map[string]int{}
	for seed := int64(0); seed < 300; seed++ {
		g := NewGenerator(seed)
		table := g.Table(TableOptions{})
		for qi := 0; qi < 6; qi++ {
			q := g.Query(table)
			if _, err := q.Execute(table); err != nil {
				t.Fatalf("%s: oracle rejected generated query: %v\ntable:\n%s", Describe(seed, qi, q), err, table)
			}
			for _, op := range q.Ops {
				switch op.(type) {
				case FilterOp:
					opCounts["filter"]++
				case SelectOp:
					opCounts["select"]++
				case ProjectOp:
					opCounts["project"]++
				case GroupByOp:
					opCounts["groupby"]++
				case SortOp:
					opCounts["sort"]++
				case TopKOp:
					opCounts["topk"]++
				case LimitOp:
					opCounts["limit"]++
				}
			}
		}
	}
	// The generator must exercise every operator over the seed sweep.
	for _, name := range []string{"filter", "select", "project", "groupby", "sort", "topk", "limit"} {
		if opCounts[name] == 0 {
			t.Errorf("generator never produced %s", name)
		}
	}
	t.Logf("operator counts: %v", opCounts)
}
