package lazy

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"path/filepath"
	"testing"
)

func TestReviewMixedDecimalTransport(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 141
	f, _ := memory.Decimal128Field("d", true, 38, 30)
	g, _ := memory.Decimal128Field("e", false, 9, 0)
	d := &memory.Decimal128Vector{Values: make([]memory.Decimal128, b.Rows), Validity: memory.EnsureBitmap(nil, b.Rows), Precision: 38, Scale: 30}
	e := &memory.Decimal128Vector{Values: make([]memory.Decimal128, b.Rows), Precision: 9}
	x := &memory.Int64Vector{Values: make([]int64, b.Rows), Validity: memory.EnsureBitmap(nil, b.Rows)}
	for i := 0; i < b.Rows; i++ {
		d.Values[i] = decimal128.FromI64(int64(i) * -7654321)
		e.Values[i] = decimal128.FromI64(int64(i))
		x.Values[i] = int64(i)
		if i%7 != 0 {
			memory.Set(d.Validity, i)
		}
		if i%3 != 0 {
			memory.Set(x.Validity, i)
		}
	}
	b.Decimals["d"] = d
	b.Decimals["e"] = e
	b.Int64s["x"] = x
	src, err := inmemory.New(memory.NewSchema(f, g, memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}), b)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 1
	s := NewSession(opts)
	defer s.Close()
	path := filepath.Join(t.TempDir(), "mixed.parquet")
	if _, err = s.FromSource(src).SinkParquet(context.Background(), path, ParquetSinkOptions{}); err != nil {
		t.Fatal(err)
	}
	scan, err := s.ScanParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	out, err := scan.Limit(129).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	for _, name := range []string{"d", "e"} {
		ser, err := out.Column(name)
		if err != nil {
			t.Fatal(err)
		}
		chunks, err := ser.Decimal128Chunks()
		if err != nil {
			t.Fatal(err)
		}
		i := 0
		for _, c := range chunks {
			for j := 0; j < c.Len(); j++ {
				v, valid, err := c.Value(j)
				original := b.Decimals[name]
				if err != nil || v != original.Values[i] && valid || valid != original.IsValid(i) {
					t.Fatalf("%s row%d %v %v %v", name, i, v, valid, err)
				}
				i++
			}
		}
		if i != 129 {
			t.Fatal(i)
		}
	}
}
