package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func BenchmarkCollectV4(b *testing.B) {
	for _, kind := range []string{"int64", "nullable8", "string", "list"} {
		for _, shape := range []struct{ rows, batches int }{{128, 1}, {8192, 1}, {128, 16}, {1, 1024}} {
			b.Run(fmt.Sprintf("%s/rows%d_batches%d", kind, shape.rows, shape.batches), func(b *testing.B) {
				var fields []memory.Field
				batches := make([]*memory.Batch, shape.batches)
				for i := range batches {
					batch := memory.NewBatch()
					batch.Rows = shape.rows
					switch kind {
					case "int64", "nullable8":
						width := 1
						if kind == "nullable8" {
							width = 8
						}
						for col := 0; col < width; col++ {
							name := fmt.Sprintf("c%d", col)
							v := &memory.Int64Vector{Values: make([]int64, shape.rows)}
							if kind == "nullable8" {
								v.Validity = memory.EnsureBitmap(nil, shape.rows)
							}
							for r := range v.Values {
								v.Values[r] = int64(r + i)
								if v.Validity != nil && r%3 != 0 {
									memory.Set(v.Validity, r)
								}
							}
							batch.Int64s[name] = v
							if i == 0 {
								fields = append(fields, memory.Field{Name: name, Type: memory.TypeInt64, Nullable: v.Validity != nil})
							}
						}
					case "string":
						v := &memory.StringViewVector{Offsets: make([]uint32, shape.rows+1), Data: make([]byte, shape.rows*16)}
						for r := range v.Offsets {
							v.Offsets[r] = uint32(r * 16)
						}
						for j := range v.Data {
							v.Data[j] = 'a'
						}
						batch.Strings["s"] = v
						if i == 0 {
							fields = []memory.Field{{Name: "s", Type: memory.TypeString}}
						}
					case "list":
						v := &memory.Int64ListVector{Offsets: make([]uint32, shape.rows+1), Values: &memory.Int64Vector{Values: make([]int64, shape.rows*3)}}
						for r := range v.Offsets {
							v.Offsets[r] = uint32(r * 3)
						}
						batch.Int64Lists["l"] = v
						if i == 0 {
							fields = []memory.Field{{Name: "l", Type: memory.TypeList, Children: []memory.Field{{Name: "item", Type: memory.TypeInt64}}}}
						}
					}
					batches[i] = batch
				}
				src, err := inmemory.New(memory.NewSchema(fields...), batches...)
				if err != nil {
					b.Fatal(err)
				}
				opts := engine.DefaultOptions()
				s := NewSession(opts)
				defer s.Close()
				p, err := s.FromSource(src).Prepare(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				defer p.Close()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					df, err := p.Execute(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					if df.Height() != int64(shape.rows*shape.batches) {
						b.Fatal("height")
					}
					df.Close()
				}
			})
		}
	}
}
