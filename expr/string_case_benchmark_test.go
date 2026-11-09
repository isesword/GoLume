package expr

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

// Measures the public workspace, including validity/input binding and accounting.
// Cold runs include workspace creation and cleanup; warm runs retain all owned
// buffers. Dictionary fixtures reuse eight codes but outputs are materialized.
func BenchmarkStringCaseWorkspace(b *testing.B) {
	for _, profile := range []struct{ name, text string }{
		{"ascii", "Mixed ASCII Product-Key 01234567"},
		{"unchanged", "0123456789 -=+ 0123456789"},
		{"unicode", "İıſȺȾⱥⱦKµ Go语言 Über Σσς"},
		{"long", strings.Repeat("Mixed ASCII Product-Key ", 32)},
	} {
		for _, dict := range []bool{false, true} {
			for _, upper := range []bool{false, true} {
				for _, cold := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/dict=%t/upper=%t/cold=%t", profile.name, dict, upper, cold), func(b *testing.B) {
						const rows = 4096
						texts, valid := make([]string, rows), make([]bool, rows)
						for i := range texts {
							texts[i], valid[i] = profile.text, i%17 != 0
						}
						schema, batch := caseBatch(texts, valid, dict)
						if dict {
							v := batch.DictStrings["s"]
							v.Data, v.Offsets = v.Data[:8*len(profile.text)], v.Offsets[:9]
							for i := range v.Indices {
								v.Indices[i] = uint32(i % 8)
							}
						}
						e, want := Lower(Col("s")), strings.ToLower(profile.text)
						if upper {
							e, want = Upper(Col("s")), strings.ToUpper(profile.text)
						}
						p, err := Compile(schema, e)
						if err != nil {
							b.Fatal(err)
						}
						m, ctx, kernels := memory.NewBudgetManager(0), context.Background(), kernel.New(kernel.ModeAuto)
						w, err := NewWorkspace(ctx, p, m)
						if err != nil {
							b.Fatal(err)
						}
						v, err := w.Evaluate(ctx, batch, kernels)
						if err != nil || string(v[0].String.ValueBytes(1)) != want {
							b.Fatalf("oracle: %v", err)
						}
						if cold {
							w.Close()
						}
						b.ReportAllocs()
						b.ResetTimer()
						var retained int64
						for range b.N {
							if cold {
								w, err = NewWorkspace(ctx, p, m)
								if err != nil {
									b.Fatal(err)
								}
							}
							if _, err := w.Evaluate(ctx, batch, kernels); err != nil {
								b.Fatal(err)
							}
							if cold {
								retained = w.MemoryBytes()
								w.Close()
							}
						}
						b.StopTimer()
						if !cold {
							retained = w.MemoryBytes()
							w.Close()
						}
						if m.Used() != 0 {
							b.Fatal("budget leak")
						}
						b.ReportMetric(float64(retained), "retained-B")
					})
				}
			}
		}
	}
}
