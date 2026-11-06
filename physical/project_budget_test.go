package physical

import (
	"context"
	"errors"
	"testing"

	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func projectBudgetInput(t *testing.T, copies int) source.Source {
	t.Helper()
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	payload := `{"value":"abc"}`
	strings := &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
	for i := 0; i < 4; i++ {
		strings.Data = append(strings.Data, payload...)
		strings.Offsets = append(strings.Offsets, uint32(len(strings.Data)))
	}
	batch.Strings["payload"] = strings
	batch.Reset(4)
	batches := make([]*memory.Batch, copies)
	for i := range batches {
		batches[i] = batch
	}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "x", Type: memory.TypeInt64},
		memory.Field{Name: "payload", Type: memory.TypeString, Encoding: memory.EncodingView},
	), batches...)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func projectBudgetExpressions() map[string]expr.Expr {
	return map[string]expr.Expr{
		"numeric": expr.Add(expr.Col("x"), expr.I64(5)),
		"string":  expr.Concat(expr.Col("payload"), expr.Str("tail")),
		"json":    expr.JSONDecode(expr.Col("payload"), memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeString})),
	}
}

func TestProjectBudgetFailureClosesStreamAndClearsOutput(t *testing.T) {
	for name, expression := range projectBudgetExpressions() {
		t.Run(name, func(t *testing.T) {
			manager := memory.NewBudgetManager(1)
			projected, err := NewProjectSource(projectBudgetInput(t, 1), []expr.NamedExpr{expr.Named(expression, "result")}, false, manager)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := projected.NewBatchStream(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			ok, err := stream.Next(context.Background(), out)
			if ok || !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("Next=%t, %v", ok, err)
			}
			if manager.Used() != 0 || out.Rows != 0 {
				t.Fatalf("failed projection retained bytes=%d rows=%d", manager.Used(), out.Rows)
			}
		})
	}
}

func TestProjectBudgetRepeatedBatchesAndTermination(t *testing.T) {
	for name, expression := range projectBudgetExpressions() {
		for _, cancel := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/eof", true: "/cancel"}[cancel], func(t *testing.T) {
				manager := memory.NewBudgetManager(1 << 20)
				projected, err := NewProjectSource(projectBudgetInput(t, 3), []expr.NamedExpr{expr.Named(expression, "result")}, false, manager)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := projected.NewBatchStream(context.Background(), testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				var retained int64
				for i := 0; i < 3; i++ {
					if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
						t.Fatalf("batch %d: %t, %v", i, ok, err)
					}
					if out.Rows != 4 {
						t.Fatalf("unexpected output rows %d", out.Rows)
					}
					used := manager.Used()
					if used <= 0 {
						t.Fatal("computed projection has no retained charge")
					}
					if i > 0 && used != retained {
						t.Fatalf("repeated batch charge grew %d -> %d", retained, used)
					}
					retained = used
				}
				ctx := context.Background()
				if cancel {
					var stop context.CancelFunc
					ctx, stop = context.WithCancel(ctx)
					stop()
				}
				ok, err := stream.Next(ctx, out)
				if ok || (!cancel && err != nil) || (cancel && !errors.Is(err, context.Canceled)) {
					t.Fatalf("termination=%t, %v", ok, err)
				}
				if manager.Used() != 0 {
					t.Fatalf("termination retained %d bytes", manager.Used())
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestProjectBudgetEarlyCloseReleasesOwnedBuffers(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	projected, err := NewProjectSource(projectBudgetInput(t, 3), []expr.NamedExpr{
		expr.Named(expr.Add(expr.Col("x"), expr.I64(5)), "result"),
	}, false, manager)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := projected.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("Next=%t, %v", ok, err)
	}
	if manager.Used() == 0 {
		t.Fatal("computed projection was not charged")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("early Close retained %d bytes", manager.Used())
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}
