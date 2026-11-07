package expr

import (
	"fmt"
	"gocolumn/memory"
	"reflect"
	"testing"
)

// Validate every fluent construction wrapper against its free constructor.
// Evaluation correctness is covered independently by the operator matrices.
func TestAllFluentConstructorsMatchFreeFunctions(t *testing.T) {
	constructors := map[string]any{
		"Abs": Abs, "Round": Round, "Floor": Floor, "Ceil": Ceil, "Sqrt": Sqrt, "Log": Log, "Mod": Mod, "FloorDiv": FloorDiv, "Pow": Pow,
		"Add": Add, "Sub": Sub, "Mul": Mul, "Div": Div, "Concat": Concat,
		"Contains": Contains, "StartsWith": StartsWith, "EndsWith": EndsWith,
		"Length": Length, "ByteLength": ByteLength, "Trim": Trim, "TrimStart": TrimStart, "TrimEnd": TrimEnd,
		"Lower": Lower, "Upper": Upper, "Find": Find, "Replace": Replace, "ReplaceAll": ReplaceAll, "Slice": Slice,
		"RegexMatch": RegexMatch, "RegexExtract": RegexExtract, "RegexExtractAll": RegexExtractAll, "Split": Split,
		"Year": Year, "Month": Month, "Day": Day, "Hour": Hour, "Minute": Minute, "Second": Second,
		"Gt": GT, "Ge": GE, "Lt": LT, "Le": LE, "Eq": EQ, "IsNull": IsNull, "IsNotNull": IsNotNull,
		"Cast": CastTo, "TryCast": TryCastTo, "Alias": Named, "JSONDecode": JSONDecode,
	}
	receivers := []Expr{Col("x"), I64(1), F64(2), Str("s"), Date32(1), Timestamp(1), Duration(1), Bool(true), Null(memory.TypeInt64), Add(Col("x"), I64(1)), Length(Str("a")), Replace(Str("a"), Str("a"), Str("b")), CastTo(I64(1), memory.TypeInt8), TryCastTo(Str("1"), memory.TypeInt8), If(Bool(true), I64(1), I64(2)), Case([]WhenThen{{When: Bool(true), Then: I64(1)}}, I64(2)), Coalesce(I64(1), I64(2)), In(I64(1), I64(2)), JSONDecode(Str("{}"), memory.NewSchema())}
	count := 0
	for _, receiver := range receivers {
		rv := reflect.ValueOf(receiver)
		for i := 0; i < rv.NumMethod(); i++ {
			name := rv.Type().Method(i).Name
			if name == "String" || name == "Name" || name == "Nanoseconds" {
				continue
			}
			free, ok := constructors[name]
			if !ok {
				t.Fatalf("missing constructor oracle for %T.%s", receiver, name)
			}
			t.Run(fmt.Sprintf("%T/%s", receiver, name), func(t *testing.T) {
				method := rv.Method(i)
				args := make([]reflect.Value, method.Type().NumIn())
				for j := range args {
					typ := method.Type().In(j)
					switch typ {
					case reflect.TypeFor[Expr]():
						args[j] = reflect.ValueOf(Col(fmt.Sprintf("arg%d", j)))
					case reflect.TypeFor[string]():
						args[j] = reflect.ValueOf("alias")
					case reflect.TypeFor[memory.DataType]():
						args[j] = reflect.ValueOf(memory.TypeFloat64)
					case reflect.TypeFor[memory.Schema]():
						args[j] = reflect.ValueOf(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}))
					default:
						t.Fatalf("unsupported parameter %s", typ)
					}
				}
				got := method.Call(args)[0].Interface()
				want := reflect.ValueOf(free).Call(append([]reflect.Value{rv}, args...))[0].Interface()
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %#v want %#v", got, want)
				}
			})
			count++
		}
	}
	t.Logf("checked %d fluent constructors", count)
}
