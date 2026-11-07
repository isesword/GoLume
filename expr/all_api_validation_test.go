package expr

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

// Independent standard-library oracles exercise public compilation, output
// export and reuse, rather than comparing two modes of the same executor.
func TestAllAPIStringOracleReuse(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}, memory.Field{Name: "d", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
	type check struct {
		name       string
		expression Expr
		oracle     func(string, string) any
		binary     bool
	}
	tests := []check{
		{"concat", Concat(Col("s"), Col("d")), func(s, d string) any { return s + d }, true},
		{"contains", Contains(Col("s"), Col("d")), func(s, d string) any { return strings.Contains(s, d) }, true},
		{"starts", StartsWith(Col("s"), Col("d")), func(s, d string) any { return strings.HasPrefix(s, d) }, true},
		{"ends", EndsWith(Col("s"), Col("d")), func(s, d string) any { return strings.HasSuffix(s, d) }, true},
		{"length", Length(Col("s")), func(s, d string) any { return int64(utf8.RuneCountInString(s)) }, false},
		{"bytes", ByteLength(Col("s")), func(s, d string) any { return int64(len(s)) }, false},
		{"trim", Trim(Col("s")), func(s, d string) any { return strings.TrimSpace(s) }, false},
		{"trim_start", TrimStart(Col("s")), func(s, d string) any { return strings.TrimLeftFunc(s, unicode.IsSpace) }, false},
		{"trim_end", TrimEnd(Col("s")), func(s, d string) any { return strings.TrimRightFunc(s, unicode.IsSpace) }, false},
		{"lower", Lower(Col("s")), func(s, d string) any { return strings.ToLower(s) }, false},
		{"upper", Upper(Col("s")), func(s, d string) any { return strings.ToUpper(s) }, false},
		{"find", Find(Col("s"), Col("d")), func(s, d string) any {
			i := strings.Index(s, d)
			if i < 0 {
				return int64(-1)
			}
			return int64(utf8.RuneCountInString(s[:i]))
		}, true},
		{"replace", Replace(Col("s"), Col("d"), Str("界")), func(s, d string) any { return strings.Replace(s, d, "界", 1) }, true},
		{"replace_all", ReplaceAll(Col("s"), Col("d"), Str("界")), func(s, d string) any { return strings.ReplaceAll(s, d, "界") }, true},
		{"slice", Slice(Col("s"), I64(-2), I64(99)), func(s, d string) any {
			r := []rune(s)
			if len(r) < 2 {
				return ""
			}
			return string(r[len(r)-2:])
		}, false},
		{"regex_match", RegexMatch(Col("s"), Str(`(中)?文|a+`)), func(s, d string) any { return regexp.MustCompile(`(中)?文|a+`).MatchString(s) }, false},
	}
	named := make([]NamedExpr, len(tests))
	for i, tc := range tests {
		named[i] = Named(tc.expression, tc.name)
	}
	p, err := CompileExpressions(schema, named)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	samples := []string{"", "\u3000 Ü中文 aa \t", "aaaa", "文中文", "🙂é", "a\x00b", "\n\t"}
	delims := []string{"", "中", "aa", "文", "é", "\x00", "x"}
	for _, n := range []int{0, 1, 65, 129, 3, 0, 70} {
		ss, ds := make([]string, n), make([]string, n)
		sv, dv := make([]bool, n), make([]bool, n)
		for i := range ss {
			ss[i] = samples[i%len(samples)]
			ds[i] = delims[i%len(delims)]
			sv[i] = i%11 != 7
			dv[i] = i%13 != 9
		}
		b := memory.NewBatch()
		b.Strings["s"] = testStringVector(ss, sv)
		b.Strings["d"] = testStringVector(ds, dv)
		b.Reset(n)
		got, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatalf("rows=%d: %v", n, err)
		}
		for j, tc := range tests {
			for i := range ss {
				valid := sv[i] && (!tc.binary || dv[i])
				var actual any
				var actualValid bool
				switch want := tc.oracle(ss[i], ds[i]).(type) {
				case string:
					actualValid = got[j].String.IsValid(i)
					actual = string(got[j].String.ValueBytes(i))
					if valid && actual != want {
						t.Fatalf("%s n=%d row=%d got=%q want=%q", tc.name, n, i, actual, want)
					}
				case int64:
					actualValid = got[j].Int64.IsValid(i)
					actual = got[j].Int64.Values[i]
					if valid && actual != want {
						t.Fatalf("%s row=%d got=%v want=%v", tc.name, i, actual, want)
					}
				case bool:
					actualValid = got[j].Uint32.IsValid(i)
					actual = got[j].Uint32.Values[i] != 0
					if valid && actual != want {
						t.Fatalf("%s row=%d got=%v want=%v", tc.name, i, actual, want)
					}
				}
				if actualValid != valid {
					t.Fatalf("%s row=%d validity=%v want=%v", tc.name, i, actualValid, valid)
				}
			}
		}
	}
}

func TestAllAPICalendarTypesOracle(t *testing.T) {
	constructors := []func(Expr) Unary{Year, Month, Day, Hour, Minute, Second}
	for _, typ := range []memory.DataType{memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp} {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "v", Type: typ, Nullable: true})
			b := memory.NewBatch()
			n := 67
			b.Reset(n)
			stamps := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2000, 2, 29, 12, 34, 56, 0, time.UTC), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
			validity := memory.EnsureBitmap(nil, n)
			v32 := make([]int32, n)
			v64 := make([]int64, n)
			wants := make([]time.Time, n)
			for i := 0; i < n; i++ {
				stamp := stamps[i%3]
				if i%9 != 4 {
					memory.Set(validity, i)
				}
				switch typ {
				case memory.TypeDate32:
					v32[i] = daysUTC(stamp)
					wants[i] = time.Unix(int64(v32[i])*86400, 0).UTC()
				case memory.TypeDate64:
					v64[i] = stamp.UnixMilli()
					wants[i] = stamp
				default:
					v64[i] = stamp.UnixNano()
					wants[i] = stamp
				}
			}
			if typ == memory.TypeDate32 {
				b.Int32s["v"] = &memory.Int32Vector{Values: v32, Validity: validity}
			} else {
				b.Int64s["v"] = &memory.Int64Vector{Values: v64, Validity: validity}
			}
			active := constructors
			if typ != memory.TypeTimestamp {
				active = constructors[:3]
				for _, f := range constructors[3:] {
					if _, err := Compile(schema, f(Col("v"))); err == nil {
						t.Fatal("date time-of-day extraction must reject")
					}
				}
			}
			named := make([]NamedExpr, len(active))
			for i, f := range active {
				named[i] = Named(f(Col("v")), fmt.Sprint(i))
			}
			p, err := CompileExpressions(schema, named)
			if err != nil {
				t.Fatal(err)
			}
			w, err := NewWorkspace(context.Background(), p, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			got, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			for i, stamp := range wants {
				expected := []int{stamp.Year(), int(stamp.Month()), stamp.Day(), stamp.Hour(), stamp.Minute(), stamp.Second()}
				for j, want := range expected[:len(active)] {
					v := got[j].Int64
					if v.IsValid(i) != (i%9 != 4) {
						t.Fatal("validity")
					}
					if v.IsValid(i) && v.Values[i] != int64(want) {
						t.Fatalf("row=%d component=%d got=%d want=%d", i, j, v.Values[i], want)
					}
				}
			}
		})
	}
}

func TestAllAPIListRegexPublicReuse(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
	re := regexp.MustCompile(`(中)?文|a+`)
	p, err := CompileExpressions(schema, []NamedExpr{Named(Split(Col("s"), Str("")), "split"), Named(RegexExtract(Col("s"), Str(re.String()), I64(1)), "first"), Named(RegexExtractAll(Col("s"), Str(re.String()), I64(1)), "all")})
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	samples := []string{"", "a中文 文", "中文中文", "🙂é", "a", "文"}
	for _, n := range []int{0, 67, 1, 129, 0, 7} {
		ss := make([]string, n)
		valid := make([]bool, n)
		for i := range ss {
			ss[i] = samples[i%len(samples)]
			valid[i] = i%7 != 5
		}
		b := memory.NewBatch()
		b.Reset(n)
		b.Strings["s"] = testStringVector(ss, valid)
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for i, s := range ss {
			splits := strings.Split(s, "")
			matches := re.FindAllStringSubmatchIndex(s, -1)
			for j := 0; j < 3; j += 2 {
				list := out[j].StringList
				if list.IsValid(i) != valid[i] {
					t.Fatalf("list %d row %d validity", j, i)
				}
				if !valid[i] {
					continue
				}
				start, end, ok := list.ValueRange(i)
				wantLen := len(splits)
				if j == 2 {
					wantLen = len(matches)
				}
				if !ok || end-start != wantLen {
					t.Fatalf("list %d row %d range %d:%d want %d", j, i, start, end, wantLen)
				}
				for k := 0; k < wantLen; k++ {
					want := ""
					childValid := true
					if j == 0 {
						want = splits[k]
					} else if matches[k][2] < 0 {
						childValid = false
					} else {
						want = s[matches[k][2]:matches[k][3]]
					}
					if list.Values.IsValid(start+k) != childValid {
						t.Fatal("child validity")
					}
					if childValid && string(list.Values.ValueBytes(start+k)) != want {
						t.Fatal("child value")
					}
				}
			}
			first := out[1].String
			wantValid := valid[i] && len(matches) > 0 && matches[0][2] >= 0
			if first.IsValid(i) != wantValid {
				t.Fatal("first validity")
			}
			if wantValid && string(first.ValueBytes(i)) != s[matches[0][2]:matches[0][3]] {
				t.Fatal("first value")
			}
		}
	}
}

func TestAllAPIJSONPublicGuardAndRecovery(t *testing.T) {
	target := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}, memory.Field{Name: "ok", Type: memory.TypeBool})
	decoded := JSONDecode(Col("s"), target)
	// Selecting a safe string before JSON decoding must never touch malformed rows.
	guarded := JSONDecode(If(Col("ok"), Col("s"), Str(`{"x":9}`)), target)
	for _, tc := range []struct {
		name string
		e    Expr
		bad  bool
	}{{"guarded", guarded, false}, {"direct", decoded, true}} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Compile(schema, tc.e)
			if err != nil {
				t.Fatal(err)
			}
			w, err := NewWorkspace(context.Background(), p, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			for _, broken := range []bool{false, true, false} {
				values := []string{`{"x":1}`, `{"x":2}`, `null`}
				if broken {
					values[1] = "{"
				}
				b := memory.NewBatch()
				b.Reset(3)
				b.Strings["s"] = testStringVector(values, nil)
				b.Uint32s["ok"] = &memory.Uint32Vector{Values: []uint32{1, 0, 1}}
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if tc.bad && broken {
					if err == nil {
						t.Fatal("malformed JSON accepted")
					}
					if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "closed") {
						t.Fatal("failed evaluation must close workspace")
					}
					w, err = NewWorkspace(context.Background(), p, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				v := out[0].Struct
				if !v.IsValid(0) || !v.IsValid(1) || v.IsValid(2) {
					t.Fatal("struct validity")
				}
				want := int64(2)
				if !tc.bad {
					want = 9
				}
				if v.Values.Int64s["x"].Values[1] != want {
					t.Fatal("struct value")
				}
			}
		})
	}
}
