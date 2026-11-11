package lazy

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"gocolumn/engine"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestJSONRowReusePreparedBatches(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
			target := memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true}, memory.ListStringField("items", true, true))
			payloads := []string{`{"n":7,"s":"one","items":[null,"", "tail"]}`, `null`, `{"n":null,"s":null,"items":null}`, `{"n":9,"s":"中","items":[]}`, `{"n":0,"s":"","items":["after"]}`}
			first := jsonInputBatch([]int64{0, 1, 2, 3, 4}, payloads, nil)
			second := jsonInputBatch([]int64{5}, []string{`{"n":11,"s":"last","items":["x",null]}`}, nil)
			saved := append([]byte(nil), first.Strings["payload"].Data...)
			src, err := inmemory.New(schema, first, second)
			if err != nil {
				t.Fatal(err)
			}
			opts := engine.DefaultOptions()
			opts.KernelMode = mode
			opts.MemoryLimitBytes = 2 << 20
			session := NewSession(opts)
			defer session.Close()
			prepared, err := session.FromSource(src).SelectExprs(JSONDecode(Col("payload"), target).Alias("decoded")).Prepare(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			want := []any{
				map[string]any{"n": int64(7), "s": "one", "items": []any{nil, "", "tail"}}, nil,
				map[string]any{"n": nil, "s": nil, "items": nil},
				map[string]any{"n": int64(9), "s": "中", "items": []any{}},
				map[string]any{"n": int64(0), "s": "", "items": []any{"after"}},
				map[string]any{"n": int64(11), "s": "last", "items": []any{"x", nil}},
			}
			for run := 0; run < 3; run++ {
				frame, err := prepared.Execute(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				series, err := frame.Column("decoded")
				if err != nil {
					t.Fatal(err)
				}
				got, err := series.ToAnySlice()
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("run %d got=%#v want=%#v", run, got, want)
				}
				if err := frame.Close(); err != nil {
					t.Fatal(err)
				}
				if session.Engine().Memory().Used() != 0 {
					t.Fatalf("run %d leaked memory: %d", run, session.Engine().Memory().Used())
				}
			}
			if !reflect.DeepEqual(saved, first.Strings["payload"].Data) {
				t.Fatal("borrowed source changed")
			}
		})
	}
}

func BenchmarkJSONRowReusePreparedCollect(b *testing.B) {
	for _, rows := range []int{1, 256, 4096} {
		b.Run(fmt.Sprint(rows), func(b *testing.B) {
			payloads := make([]string, rows)
			ids := make([]int64, rows)
			for i := range payloads {
				payloads[i] = `{"n":7,"s":"hello world","items":["alpha",null,"beta"]}`
				ids[i] = int64(i)
			}
			input := jsonInputBatch(ids, payloads, nil)
			schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeString, Encoding: memory.EncodingView})
			target := memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt64}, memory.Field{Name: "s", Type: memory.TypeString}, memory.ListStringField("items", false, true))
			src, err := inmemory.New(schema, input)
			if err != nil {
				b.Fatal(err)
			}
			opts := engine.DefaultOptions()
			opts.MemoryLimitBytes = 64 << 20
			session := NewSession(opts)
			defer session.Close()
			prepared, err := session.FromSource(src).SelectExprs(JSONDecode(Col("payload"), target).Alias("decoded")).Prepare(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			defer prepared.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				frame, err := prepared.Execute(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if err := frame.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if session.Engine().Memory().Used() != 0 {
				b.Fatalf("memory leaked: %d", session.Engine().Memory().Used())
			}
		})
	}
}
