package lazy

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/optimizer"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// Exposing only Source exercises physical projection without source pushdown.
type projectedScanPlainSource struct {
	source.Source
	batchRows int
}

func (s projectedScanPlainSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	opts.BatchRows = s.batchRows
	return s.Source.NewBatchStream(ctx, opts)
}

type projectedScanPushdownSource struct {
	projectedScanPlainSource
	requests *int
}

func (s projectedScanPushdownSource) ApplyPushdown(ctx context.Context, req source.PushdownRequest) (source.Source, source.PushdownReport, error) {
	*s.requests++
	if len(req.Projection) == 0 {
		return nil, source.PushdownReport{}, fmt.Errorf("projected scan lost its explicit output")
	}
	for _, name := range req.Projection {
		if name == "hidden" {
			return nil, source.PushdownReport{}, fmt.Errorf("excluded source column resurrected")
		}
	}
	next, report, err := s.Source.(source.PushdownSource).ApplyPushdown(ctx, req)
	if err != nil {
		return nil, report, err
	}
	return projectedScanPlainSource{Source: next, batchRows: s.batchRows}, report, nil
}

func TestProjectedScanPreparedExecution(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, pushdown := range []bool{false, true} {
			for _, batchRows := range []int{1, 3} {
				for _, empty := range []bool{false, true} {
					for _, variant := range []string{"bare", "reorder", "with_columns", "replace_all", "literal", "filter", "topk", "window"} {
						t.Run(fmt.Sprintf("%s/pushdown%v/batch%d/empty%v/%s", mode, pushdown, batchRows, empty, variant), func(t *testing.T) {
							schema := memory.NewSchema(memory.Field{Name: "hidden", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt64})
							batch := memory.NewBatch()
							batch.Reset(5)
							batch.Int64s["hidden"] = &memory.Int64Vector{Values: []int64{100, 200, 300, 400, 500}}
							batch.Int64s["value"] = &memory.Int64Vector{Values: []int64{3, 1, 0, 2, 4}, Validity: memory.Bitmap{27}}
							batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{10, 20, 30, 40, 50}}
							batches := []*memory.Batch{batch}
							if empty {
								batches = nil
							}
							base, err := inmemory.New(schema, batches...)
							if err != nil {
								t.Fatal(err)
							}
							var src source.Source = projectedScanPlainSource{Source: base, batchRows: batchRows}
							requests := 0
							if pushdown {
								src = projectedScanPushdownSource{projectedScanPlainSource: src.(projectedScanPlainSource), requests: &requests}
							}
							opts := engine.DefaultOptions()
							opts.KernelMode = mode
							opts.MemoryLimitBytes = 2 << 20
							session := NewSession(opts)
							defer session.Close()
							query := session.FromSource(src).Select("value")
							first, err := optimizer.Optimize(query.plan)
							if err != nil {
								t.Fatal(err)
							}
							if _, ok := first.Plan.(*logical.Scan); !ok {
								t.Fatalf("expected removed projection, got %T", first.Plan)
							}
							query.plan = first.Plan
							want := map[string][]any{"value": {int64(3), int64(1), nil, int64(2), int64(4)}}
							switch variant {
							case "reorder":
								query.plan = &logical.Scan{Source: src, Projection: []string{"id", "value"}}
								want["id"] = []any{int64(10), int64(20), int64(30), int64(40), int64(50)}
							case "with_columns":
								query = query.WithColumns(expr.Named(expr.Col("value").Mul(expr.I64(2)), "doubled"))
								want["doubled"] = []any{int64(6), int64(2), nil, int64(4), int64(8)}
							case "replace_all":
								query = query.WithColumns(expr.Named(expr.I64(9), "value"))
								want["value"] = []any{int64(9), int64(9), int64(9), int64(9), int64(9)}
							case "literal":
								query = query.SelectExprs(expr.Named(expr.I64(7), "answer"))
								want = map[string][]any{"answer": {int64(7), int64(7), int64(7), int64(7), int64(7)}}
							case "filter":
								query = query.Filter(expr.GT(expr.Col("value"), expr.I64(1)))
								want["value"] = []any{int64(3), int64(2), int64(4)}
							case "topk":
								query = query.TopKBy(2, Desc("value"))
								want["value"] = []any{int64(4), int64(3)}
							case "window":
								query = query.WithWindows(NewWindow(nil, OrderBy(AscExpr(expr.I64(1)))), RowNumber("value"))
								want["value"] = []any{int64(1), int64(2), int64(3), int64(4), int64(5)}
							}
							expected, err := query.Schema()
							if err != nil {
								t.Fatal(err)
							}
							for pass := 0; pass < 3; pass++ {
								optimized, err := optimizer.Optimize(query.plan)
								if err != nil {
									t.Fatal(err)
								}
								query.plan = optimized.Plan
								actual, err := query.Schema()
								if err != nil {
									t.Fatal(err)
								}
								if !reflect.DeepEqual(actual, expected) {
									t.Fatalf("pass %d schema=%+v want=%+v", pass, actual, expected)
								}
							}
							prepared, err := query.Prepare(context.Background())
							if err != nil {
								t.Fatal(err)
							}
							defer prepared.Close()
							if !reflect.DeepEqual(prepared.Schema(), expected) {
								t.Fatalf("prepared schema=%+v want=%+v", prepared.Schema(), expected)
							}
							for repeat := 0; repeat < 3; repeat++ {
								result, err := prepared.Execute(context.Background())
								if err != nil {
									t.Fatal(err)
								}
								if !reflect.DeepEqual(result.Schema(), expected) {
									result.Close()
									t.Fatalf("result schema=%+v want=%+v", result.Schema(), expected)
								}
								for name, values := range want {
									series, err := result.Column(name)
									if err != nil {
										result.Close()
										t.Fatal(err)
									}
									got, err := series.ToAnySlice()
									if err != nil {
										result.Close()
										t.Fatal(err)
									}
									if empty {
										if len(got) != 0 {
											t.Errorf("%s=%v want empty", name, got)
										}
									} else if !reflect.DeepEqual(got, values) {
										t.Errorf("%s=%v want=%v", name, got, values)
									}
								}
								if err := result.Close(); err != nil {
									t.Fatal(err)
								}
								if used := session.Engine().Memory().Used(); used != 0 {
									t.Fatalf("repeat %d retained %d bytes", repeat, used)
								}
							}
							if err := prepared.Close(); err != nil {
								t.Fatal(err)
							}
							if used := session.Engine().Memory().Used(); used != 0 {
								t.Fatalf("closed plan retained %d bytes", used)
							}
							if pushdown && requests == 0 {
								t.Fatal("source projection was not applied")
							}
							if !reflect.DeepEqual(batch.Int64s["value"].Values, []int64{3, 1, 0, 2, 4}) || batch.Int64s["value"].Validity[0] != 27 {
								t.Fatal("borrowed input mutated")
							}
						})
					}
				}
			}
		}
	}
}
