package lazy

import (
	"context"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestJSONStructSurvivesLazyPipelineAndSeries(t *testing.T) {
	target := memory.NewSchema(
		memory.Field{Name: "name", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.ListStringField("tags", true, true),
	)
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "payload", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
	)
	first := jsonInputBatch([]int64{1, 2}, []string{`{"name":"A","tags":["go",null]}`, `null`}, nil)
	second := jsonInputBatch([]int64{3, 4}, []string{`{"name":"中","tags":[]}`, `{"name":null}`}, nil)
	src, err := inmemory.New(schema, first, second)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	df, err := session.FromSource(src).
		WithColumns(JSONDecode(Col("payload"), target).Alias("decoded")).
		Filter(Col("id").Gt(Lit(1))).
		Sort(Desc("id")).
		Limit(3).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	series, err := df.Column("decoded")
	if err != nil {
		t.Fatal(err)
	}
	if series.Type() != memory.TypeStruct || series.Len() != 3 || series.NullCount() != 1 {
		t.Fatalf("struct series type=%s len=%d nulls=%d", series.Type(), series.Len(), series.NullCount())
	}
	chunks, err := series.StructChunks()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) == 0 {
		t.Fatal("missing struct chunks")
	}
	row, valid, err := chunks[0].Value(0)
	if err != nil || !valid {
		t.Fatalf("row=(%+v,%v,%v)", row, valid, err)
	}
	name, nameValid, err := row.Field("name")
	if err != nil || nameValid || name != "" {
		t.Fatalf("name=(%v,%v,%v)", name, nameValid, err)
	}
	values, err := series.ToAnySlice()
	if err != nil || len(values) != 3 || values[2] != nil {
		t.Fatalf("any=%v err=%v", values, err)
	}
	text := df.String()
	for _, want := range []string{"{name: NULL, tags: NULL}", "{name: 中, tags: []}", "NULL"} {
		if !strings.Contains(text, want) {
			t.Fatalf("display missing %q:\n%s", want, text)
		}
	}
}

func TestStructPayloadSurvivesLeftJoinMaterialization(t *testing.T) {
	target := memory.NewSchema(memory.Field{Name: "label", Type: memory.TypeString, Encoding: memory.EncodingView})
	leftBatch := memory.NewBatch()
	leftBatch.Int64s["k"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	leftBatch.Reset(3)
	left, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64}), leftBatch)
	if err != nil {
		t.Fatal(err)
	}
	rightSchema := memory.NewSchema(memory.Field{Name: "rk", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeString, Encoding: memory.EncodingView})
	rightBatch := jsonInputBatch([]int64{2, 3}, []string{`{"label":"two"}`, `{"label":"three"}`}, nil)
	delete(rightBatch.Int64s, "id")
	rightBatch.Int64s["rk"] = &memory.Int64Vector{Values: []int64{2, 3}}
	right, err := inmemory.New(rightSchema, rightBatch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	df, err := session.FromSource(left).
		Join(session.FromSource(right).WithColumns(JSONDecode(Col("payload"), target).Alias("decoded")), JoinOptions{Type: LeftJoin, LeftOn: []string{"k"}, RightOn: []string{"rk"}}).
		SelectExprs(Col("k").Alias("k"), Col("decoded").Alias("decoded")).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	series, err := df.Column("decoded")
	if err != nil {
		t.Fatal(err)
	}
	values, err := series.ToAnySlice()
	if err != nil {
		t.Fatal(err)
	}
	if values[0] != nil || values[1].(map[string]any)["label"] != "two" || values[2].(map[string]any)["label"] != "three" {
		t.Fatalf("joined struct values=%v", values)
	}
}

func jsonInputBatch(ids []int64, payloads []string, validity []bool) *memory.Batch {
	b := memory.NewBatch()
	b.Int64s["id"] = &memory.Int64Vector{Values: append([]int64(nil), ids...)}
	v := &memory.StringViewVector{Offsets: make([]uint32, len(payloads)+1), Encoding: memory.EncodingView}
	if validity != nil {
		v.Validity = memory.EnsureBitmap(nil, len(payloads))
	}
	for i, payload := range payloads {
		if validity == nil || validity[i] {
			v.Data = append(v.Data, payload...)
			if v.Validity != nil {
				memory.Set(v.Validity, i)
			}
		}
		v.Offsets[i+1] = uint32(len(v.Data))
	}
	b.Strings["payload"] = v
	b.Reset(len(ids))
	return b
}
