package expr

import (
	"gocolumn/memory"
	"strings"
	"testing"
)

func TestN3TerminalListMetadataAdmission(t *testing.T) {
	for _, depth := range []int{1, 2, 8} {
		for _, kind := range []string{"list_domain", "item_domain", "list_encoding", "item_encoding"} {
			list := memory.ListInt64Field("xs", true, true)
			switch kind {
			case "list_domain":
				list.DictionaryDomain = "bad"
			case "item_domain":
				list.Children[0].DictionaryDomain = "bad"
			case "list_encoding":
				list.Encoding = memory.EncodingDictionary
			case "item_encoding":
				list.Children[0].Encoding = memory.EncodingDictionary
			}
			f := memory.StructField("root", true, list)
			for d := 1; d < depth; d++ {
				f = memory.StructField("root", true, f)
			}
			if err := f.ValidateRecursiveStruct(); err == nil {
				t.Fatalf("depth%d accepted %s", depth, kind)
			}
			if _, err := Compile(memory.NewSchema(f), Col("root")); err == nil {
				t.Fatalf("bind depth%d accepted %s", depth, kind)
			}
		}
	}
}

func TestN3StructGetShortActiveMask(t *testing.T) {
	schema, b := structGetFixture(structGetKinds[0], 65, true, true)
	f := schema.Fields[0]
	// Locate the Struct independently of fixture field order.
	for _, field := range schema.Fields {
		if field.Type == memory.TypeStruct {
			f = field
			break
		}
	}
	input := scalarRegister{strct: b.Structs[f.Name]}
	out := scalarRegister{}
	plan := structGetPlan{parent: f, child: f.Children[0], index: 0}
	err := evalStructGet(&out, &input, &plan, 65, memory.Bitmap{1})
	if err == nil || !strings.Contains(err.Error(), "active bitmap is too short") {
		t.Fatalf("short mask: %v", err)
	}
}

func TestN3AnonymousConstructedParentStillBinds(t *testing.T) {
	f := memory.Field{Name: "x", Type: memory.TypeInt64}
	_, err := Compile(memory.NewSchema(f), StructGet(StructFrom(Col("x").Alias("v")), "v"))
	if err != nil {
		t.Fatal(err)
	}
}
