package expr

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"gocolumn/memory"
)

func TestBindUnifiedCompositionAndDependencies(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "i", Type: memory.TypeInt8, Nullable: true},
		memory.Field{Name: "s", Type: memory.TypeString},
		memory.Field{Name: "u", Type: memory.TypeUint64},
	)
	value := And(
		GT(Add(Col("i"), I64(2)), I64(3)),
		Contains(Concat(Col("s"), Str("!")), Upper(Str("go"))),
	)
	bound, err := Bind(schema, value)
	if err != nil {
		t.Fatal(err)
	}
	if bound.Field.Type != memory.TypeBool || !bound.Field.Nullable || !bound.MayError {
		t.Fatalf("bound result=%+v", bound)
	}
	if want := []string{"i", "s"}; !reflect.DeepEqual(bound.Columns, want) || !reflect.DeepEqual(Columns(value), want) {
		t.Fatalf("dependencies bound=%v AST=%v", bound.Columns, Columns(value))
	}
	info, err := InferScalarType(schema, value)
	if err != nil || info.Type != bound.Field.Type || info.Nullable != bound.Field.Nullable {
		t.Fatalf("shared inference=%+v err=%v", info, err)
	}
	// Typed comparison children must retain exact native domains rather than
	// receiving the lossy arithmetic common type.
	for _, expression := range []Expr{EQ(Col("u"), I64(math.MaxInt64)), LT(Col("u"), I64(-1)), GT(Col("i"), F64(1.5))} {
		comparison, err := Bind(schema, expression)
		if err != nil {
			t.Fatal(err)
		}
		if comparison.Children[0].Field.Type != boundTypeForColumn(t, schema, expression.(Binary).Left.(Column).Name) {
			t.Fatal("comparison changed native input type")
		}
		if comparison.MayError {
			t.Fatal("native comparison must be total on valid inputs")
		}
	}
}

func boundTypeForColumn(t *testing.T, schema memory.Schema, name string) memory.DataType {
	t.Helper()
	field, ok := schema.Field(name)
	if !ok {
		t.Fatal(name)
	}
	return field.Type
}

func TestBindFullFieldsAndIndependentOwnership(t *testing.T) {
	structField := memory.StructField("record", true,
		memory.Field{Name: "n", Type: memory.TypeInt8, Nullable: true},
		memory.ListStringField("items", true, true),
	)
	structField.ArrowType = "struct<n: int8, items: list<item: utf8>>"
	schema := memory.NewSchema(structField,
		memory.Field{Name: "dictionary", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "domain-a"},
		memory.Field{Name: "json", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
	)
	column, err := Bind(schema, Col("record"))
	if err != nil {
		t.Fatal(err)
	}
	field := column.OutputField("renamed")
	want := structField
	want.Name = "renamed"
	if !reflect.DeepEqual(field, want) {
		t.Fatalf("field=%+v want=%+v", field, want)
	}
	field.Children[1].Children[0].Nullable = false
	schema.Fields[0].Children[0].Type = memory.TypeInt64
	if !column.Field.Children[1].Children[0].Nullable || column.Field.Children[0].Type != memory.TypeInt8 {
		t.Fatal("bound field aliases caller-owned nested schema")
	}
	for _, tc := range []struct {
		value Expr
		want  memory.Field
	}{
		{Col("dictionary"), memory.Field{Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "domain-a"}},
		{CastTo(Col("dictionary"), memory.TypeString), memory.Field{Type: memory.TypeString, Encoding: memory.EncodingView}},
		{Split(Col("json"), Str(",")), memory.ListStringField("", true, false)},
		{RegexExtractAll(Col("json"), Str("(a)?"), I64(1)), memory.ListStringField("", true, true)},
		{JSONDecode(Col("json"), memory.NewSchema(memory.ListStringField("items", false, true))), memory.StructField("", true, memory.ListStringField("items", false, true))},
	} {
		bound, err := Bind(schema, tc.value)
		if err != nil {
			t.Fatal(err)
		}
		if got := bound.OutputField(""); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s field=%+v want=%+v", tc.value, got, tc.want)
		}
	}
}

func TestBindConditionalNullabilityAndNativeTypes(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true},
		memory.Field{Name: "n", Type: memory.TypeInt16, Nullable: true},
		memory.Field{Name: "v", Type: memory.TypeInt16},
		memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "d"},
	)
	for _, tc := range []struct {
		value    Expr
		typ      memory.DataType
		nullable bool
	}{
		{If(Col("c"), Col("v"), Col("v")), memory.TypeInt16, false},
		{If(Col("c"), Col("v"), Col("n")), memory.TypeInt16, true},
		{If(Col("c"), Col("v"), F64(2)), memory.TypeFloat64, false},
		{Coalesce(Col("n"), Col("v")), memory.TypeInt16, false},
		{Coalesce(Col("n"), Null(memory.TypeInt16)), memory.TypeInt16, true},
		{Case([]WhenThen{{When: Col("c"), Then: Col("v")}}, nil), memory.TypeInt16, true},
		{Case([]WhenThen{{When: Col("c"), Then: Col("v")}}, Col("v")), memory.TypeInt16, false},
		{In(Col("v"), I64(3), Null(memory.TypeInt64)), memory.TypeBool, true},
		{In(Col("v"), I64(3), I64(4)), memory.TypeBool, false},
		{IsNull(Add(Col("n"), I64(1))), memory.TypeBool, false},
		{If(Col("c"), Col("s"), Str("fallback")), memory.TypeString, false},
	} {
		bound, err := Bind(schema, tc.value)
		if err != nil {
			t.Fatal(err)
		}
		if bound.Field.Type != tc.typ || bound.Field.Nullable != tc.nullable {
			t.Fatalf("%s field=%+v want type=%s nullable=%v", tc.value, bound.Field, tc.typ, tc.nullable)
		}
		if tc.typ == memory.TypeString && (bound.Field.Encoding != memory.EncodingView || bound.Field.DictionaryDomain != "") {
			t.Fatalf("computed conditional retained dictionary encoding: %+v", bound.Field)
		}
	}
	bound, err := Bind(schema, Case([]WhenThen{{When: Col("c"), Then: Col("v")}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.Children) != 3 || bound.Children[2].Expr != (NullLiteral{Type: memory.TypeInt16}) {
		t.Fatalf("CASE ELSE is not a typed NULL: %+v", bound.Children)
	}
	if bound.Expr.(CaseExpr).Else == nil {
		t.Fatal("bound CASE syntax did not preserve synthesized ELSE")
	}
}

func TestBindDoesNotExecuteValueComputations(t *testing.T) {
	schema := memory.NewSchema()
	for _, value := range []Expr{
		Add(I64(math.MaxInt64), I64(1)),
		CastTo(Str("not a timestamp"), memory.TypeTimestamp),
		CastTo(F64(math.NaN()), memory.TypeInt64),
		Slice(Str("abc"), I64(0), I64(-1)),
		JSONDecode(Str("not JSON"), memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt8})),
		If(Bool(true), I64(1), Add(I64(math.MaxInt64), I64(1))),
		Coalesce(I64(1), CastTo(F64(math.Inf(1)), memory.TypeInt64)),
		In(I64(1), I64(1), CastTo(F64(math.NaN()), memory.TypeInt64)),
	} {
		bound, err := Bind(schema, value)
		if err != nil {
			t.Fatalf("value computation ran at bind for %s: %v", value, err)
		}
		if !bound.MayError {
			t.Fatalf("partial value lost MayError: %s", value)
		}
	}
	// Structural validation still applies to statically dead branches.
	for _, value := range []Expr{
		If(Bool(true), I64(1), Col("missing")),
		If(Bool(true), Bool(true), RegexMatch(Str("x"), Str("["))),
		JSONDecode(I64(1), memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt8})),
		JSONDecode(Str("{}"), memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt8}, memory.Field{Name: "n", Type: memory.TypeInt64})),
		TimestampSeconds(math.MaxInt64),
	} {
		if _, err := Bind(schema, value); err == nil {
			t.Fatalf("invalid structure bound successfully: %s", value)
		}
	}
}

func TestBindErrorEffectsAndComparisonDomains(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "i", Type: memory.TypeInt8},
		memory.Field{Name: "u", Type: memory.TypeUint64},
		memory.Field{Name: "f", Type: memory.TypeFloat32},
		memory.Field{Name: "s", Type: memory.TypeString},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp},
	)
	for _, tc := range []struct {
		value Expr
		may   bool
	}{
		{Col("i"), false},
		{Div(F64(0), F64(0)), false},
		{Add(Col("f"), F64(2)), false},
		{Add(Col("i"), I64(1)), true},
		{Add(Col("u"), Col("u")), true},
		{Add(Col("ts"), I64(1)), true},
		{CastTo(Col("i"), memory.TypeInt64), false},
		{CastTo(Col("u"), memory.TypeFloat32), false},
		{CastTo(Col("f"), memory.TypeFloat64), false},
		{CastTo(Col("f"), memory.TypeInt64), true},
		{CastTo(Col("s"), memory.TypeTimestamp), true},
		{And(Bool(false), GT(Add(Col("i"), I64(1)), I64(0))), true},
		{IsNull(JSONDecode(Col("s"), memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt8}))), true},
		{LT(Col("ts"), I64(1)), false},
		{GT(I64(1), Col("ts")), false},
		{EQ(Col("u"), Col("i")), false},
	} {
		bound, err := Bind(schema, tc.value)
		if err != nil || bound.MayError != tc.may {
			t.Fatalf("%s bound=%+v err=%v want MayError=%v", tc.value, bound, err, tc.may)
		}
	}
	for _, value := range []Expr{
		If(I64(1), I64(2), I64(3)),
		If(Bool(true), I64(2), Str("wrong")),
		Coalesce(),
		Case(nil, I64(1)),
		In(I64(1)),
		EQ(Col("ts"), Col("i")),
		EQ(Col("i"), TimestampLiteral{Value: 1}),
		EQ(Col("ts"), Date32Literal{Days: 1}),
		And(I64(1), Bool(true)),
		Null(memory.TypeInvalid),
		Null(memory.TypeStruct),
		If(Bool(true), Split(Col("s"), Str(",")), Split(Col("s"), Str(","))),
	} {
		if _, err := Bind(schema, value); err == nil {
			t.Fatalf("incompatible expression accepted: %s", value)
		}
	}
}

func TestBindControlDependenciesAndSchemaSnapshot(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeBool},
		memory.Field{Name: "b", Type: memory.TypeInt64},
		memory.Field{Name: "c", Type: memory.TypeInt64, Nullable: true},
	)
	values := []Expr{Col("b"), Col("c")}
	whens := []WhenThen{{When: Col("a"), Then: Coalesce(values...)}}
	value := Case(whens, I64(0))
	bound, err := Bind(schema, value)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(bound.Columns, want) || !reflect.DeepEqual(Columns(value), want) {
		t.Fatalf("dependencies=%v AST=%v", bound.Columns, Columns(value))
	}
	values[0] = Col("missing")
	whens[0] = WhenThen{When: Col("missing"), Then: I64(2)}
	if strings.Contains(bound.Expr.String(), "missing") {
		t.Fatal("bound syntax aliases mutable constructor slices")
	}
}
