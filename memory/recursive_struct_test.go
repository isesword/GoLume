package memory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// Construct valid bounded trees without calling the API under test.
func recursiveTestField(depth int) Field {
	f := Field{Name: "node", Type: TypeStruct, Nullable: true, Children: []Field{{Name: "n", Type: TypeInt64}, {Name: "text", Type: TypeString}}}
	for i := 1; i < depth; i++ {
		f.Name = "inner"
		f = Field{Name: "node", Type: TypeStruct, Nullable: true, Children: []Field{f}}
	}
	return f
}
func recursiveTestVector(depth int) (Field, *StructVector) {
	b := NewBatch()
	b.Rows = 3
	b.Int64s["n"] = &Int64Vector{Values: []int64{11, 99, 33}, Validity: Bitmap{5}}
	b.Strings["text"] = &StringViewVector{Offsets: []uint32{0, 1, 7, 8}, Data: []byte("aHIDDENc"), Validity: Bitmap{5}, Encoding: EncodingView}
	f := Field{Name: "node", Type: TypeStruct, Nullable: true, Children: []Field{{Name: "n", Type: TypeInt64}, {Name: "text", Type: TypeString}}}
	v := &StructVector{Rows: 3, Fields: f.Children, Values: b, Validity: Bitmap{5}, Encoding: EncodingView}
	for i := 1; i < depth; i++ {
		f.Name = "inner"
		f.Nullable = false
		child := NewBatch()
		child.Rows = 3
		child.Structs["inner"] = v
		f = Field{Name: "node", Type: TypeStruct, Nullable: true, Children: []Field{f}}
		v = &StructVector{Rows: 3, Fields: f.Children, Values: child, Validity: Bitmap{5}, Encoding: EncodingView}
	}
	return f, v
}
func recursiveTestLeaf(v *StructVector) *StructVector {
	for len(v.Values.Structs) > 0 {
		v = v.Values.Structs["inner"]
	}
	return v
}

func TestRecursiveStructSchemaDepthAndSnapshot(t *testing.T) {
	if MaxStructDepth != 8 {
		t.Fatal("public depth contract", MaxStructDepth)
	}
	for _, depth := range []int{1, 2, 7, 8, 9} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			f := recursiveTestField(depth)
			err := f.ValidateRecursiveStruct()
			s, e := NewSchemaChecked(f)
			if depth == 9 {
				if err == nil || e == nil {
					t.Fatal("depth9 accepted")
				}
				return
			}
			if err != nil || e != nil {
				t.Fatal(err, e)
			}
			if len(s.Fields) != 1 || !reflect.DeepEqual(s.Fields[0], f) {
				t.Fatal("schema changed")
			}
			original := s.Fields[0]
			cursor := &f
			for cursor.Children[0].Type == TypeStruct {
				cursor = &cursor.Children[0]
			}
			cursor.Children[0].Name = "mutated"
			got := original
			for got.Children[0].Type == TypeStruct {
				got = got.Children[0]
			}
			if got.Children[0].Name != "n" {
				t.Fatal("deep snapshot aliases input")
			}
		})
	}
}
func TestRecursiveStructSchemaCycleAndSharedDAG(t *testing.T) {
	children := make([]Field, 1)
	f := Field{Name: "cycle", Type: TypeStruct, Children: children}
	children[0] = f
	if e := f.ValidateRecursiveStruct(); e == nil {
		t.Fatal("cycle validator accepted")
	}
	if _, e := NewSchemaChecked(f); e == nil {
		t.Fatal("cycle checked schema accepted")
	}
	shared := []Field{{Name: "value", Type: TypeInt64}}
	a := Field{Name: "a", Type: TypeStruct, Children: shared}
	b := Field{Name: "b", Type: TypeStruct, Children: shared}
	root := Field{Name: "root", Type: TypeStruct, Children: []Field{a, b}}
	s, e := NewSchemaChecked(root)
	if e != nil {
		t.Fatal("acyclic shared subtree rejected", e)
	}
	shared[0].Name = "changed"
	if s.Fields[0].Children[0].Children[0].Name != "value" || s.Fields[0].Children[1].Children[0].Name != "value" {
		t.Fatal("DAG snapshot alias")
	}
}
func TestRecursiveStructAncestorDomainAndStorage(t *testing.T) {
	for _, depth := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			f, v := recursiveTestVector(depth)
			if e := v.ValidateRecursiveWithContext(f, 3, nil); e != nil {
				t.Fatal("NULL ancestor must hide required descendant values", e)
			}
			leaf := recursiveTestLeaf(v)
			leaf.Values.Int64s["n"].Validity = Bitmap{1}
			if e := v.ValidateRecursiveWithContext(f, 3, nil); e == nil {
				t.Fatal("visible required NULL accepted")
			}
			leaf.Values.Int64s["n"].Validity = Bitmap{5}
			// Hidden NULL row still participates in structural offset validation.
			leaf.Values.Strings["text"].Offsets = []uint32{0, 7, 1, 8}
			if e := v.ValidateRecursiveWithContext(f, 3, nil); e == nil {
				t.Fatal("hidden malformed offsets accepted")
			}
			f, v = recursiveTestVector(depth)
			leaf = recursiveTestLeaf(v)
			leaf.Values.Int64s["n"].Values = leaf.Values.Int64s["n"].Values[:1]
			if e := v.ValidateRecursiveWithContext(f, 3, nil); e == nil {
				t.Fatal("short child values accepted")
			}
		})
	}
}
func TestRecursiveStructValidationCancellation(t *testing.T) {
	f, v := recursiveTestVector(8)
	checks := 0
	e := v.ValidateRecursiveWithContext(f, 3, func() error {
		checks++
		if checks == 3 {
			return context.Canceled
		}
		return nil
	})
	if !errors.Is(e, context.Canceled) || checks != 3 {
		t.Fatal("recursive traversal cancellation", e, checks)
	}
	if e = v.ValidateRecursiveWithContext(f, 3, nil); e != nil {
		t.Fatal("validation mutated input", e)
	}
}

func TestRecursiveStructZeroRowsAndStorageCycle(t *testing.T) {
	f, v := recursiveTestVector(8)
	for node := v; ; node = node.Values.Structs["inner"] {
		node.Rows = 0
		node.Values.Rows = 0
		node.Validity = nil
		if len(node.Values.Structs) == 0 {
			node.Values.Int64s["n"] = &Int64Vector{}
			node.Values.Strings["text"] = &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}
			break
		}
	}
	if e := v.ValidateRecursiveWithContext(f, 0, nil); e != nil {
		t.Fatal("zero rows", e)
	}
	f, v = recursiveTestVector(2)
	v.Values.Structs["inner"] = v
	if e := v.ValidateRecursiveWithContext(f, 3, nil); e == nil {
		t.Fatal("runtime storage cycle accepted")
	}
}

func TestRecursiveStructNestedListsAndMetadata(t *testing.T) {
	f, v := recursiveTestVector(8)
	leaf := recursiveTestLeaf(v)
	lf := &f
	for lf.Children[0].Type == TypeStruct {
		lf = &lf.Children[0]
	}
	lf.Children = append(lf.Children, ListInt64Field("ints", false, false), ListStringField("texts", false, false))
	leaf.Fields = lf.Children
	leaf.Values.Int64Lists["ints"] = &Int64ListVector{Offsets: []uint32{0, 1, 2, 3}, Validity: Bitmap{5}, Values: &Int64Vector{Values: []int64{7, 99, 9}, Validity: Bitmap{5}}}
	leaf.Values.StringLists["texts"] = &StringListVector{Offsets: []uint32{0, 1, 2, 3}, Validity: Bitmap{5}, Values: &StringViewVector{Offsets: []uint32{0, 1, 7, 8}, Data: []byte("xHIDDENz"), Validity: Bitmap{5}, Encoding: EncodingView}}
	if e := v.ValidateRecursiveWithContext(f, 3, nil); e != nil {
		t.Fatal("hidden required list/element NULL", e)
	}
	leaf.Values.Int64Lists["ints"].Values.Validity = Bitmap{1}
	if e := v.ValidateRecursiveWithContext(f, 3, nil); e == nil {
		t.Fatal("visible required element NULL")
	}
	leaf.Values.Int64Lists["ints"].Values.Validity = Bitmap{5}
	leaf.Values.StringLists["texts"].Offsets = []uint32{0, 2, 1, 3}
	if e := v.ValidateRecursiveWithContext(f, 3, nil); e == nil {
		t.Fatal("hidden malformed list offsets")
	}
	leaf.Values.StringLists["texts"].Offsets = []uint32{0, 1, 2, 3}
	snapshot, e := NewSchemaChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	leaf.Fields = append([]Field(nil), leaf.Fields...)
	leaf.Fields[0].Nullable = true
	if e := v.ValidateRecursiveWithContext(snapshot.Fields[0], 3, nil); e == nil {
		t.Fatal("deep nullable metadata mismatch accepted")
	}
}

func TestRecursiveStructOwnedRangeAndBudget(t *testing.T) {
	var prior int64
	for _, depth := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			for _, span := range [][2]int{{0, 3}, {1, 2}, {3, 0}} {
				f, v := recursiveTestVector(depth)
				m := NewBudgetManager(0)
				owned, e := CloneRecursiveStructRangeOwned(context.Background(), m, v, f, span[0], span[1])
				if e != nil {
					t.Fatal(e)
				}
				if owned == nil || owned.Vector == nil || owned.Vector.Rows != span[1] || owned.Bytes <= 0 || m.Used() != owned.Bytes {
					t.Fatal("owned result/accounting")
				}
				if e = owned.Vector.ValidateRecursiveWithContext(f, span[1], nil); e != nil {
					t.Fatal(e)
				}
				leaf := recursiveTestLeaf(owned.Vector)
				for i := 0; i < span[1]; i++ {
					row := span[0] + i
					valid := row != 1
					if owned.Vector.IsValid(i) != valid || leaf.Values.Int64s["n"].IsValid(i) != valid || leaf.Values.Strings["text"].IsValid(i) != valid {
						t.Fatal("range validity", row)
					}
					if valid {
						want := int64(11)
						text := "a"
						if row == 2 {
							want = 33
							text = "c"
						}
						if leaf.Values.Int64s["n"].Values[i] != want || string(leaf.Values.Strings["text"].ValueBytes(i)) != text {
							t.Fatal("range values", row)
						}
					}
				}
				sourceLeaf := recursiveTestLeaf(v)
				sourceLeaf.Values.Int64s["n"].Values[0] = 777
				sourceLeaf.Values.Strings["text"].Data[0] = 'z'
				if span[0] == 0 && span[1] > 0 && (leaf.Values.Int64s["n"].Values[0] != 11 || string(leaf.Values.Strings["text"].ValueBytes(0)) != "a") {
					t.Fatal("owned payload aliases source")
				}
				bytes := owned.Bytes
				owned.Close()
				owned.Close()
				if m.Used() != 0 || owned.Vector != nil {
					t.Fatal("Close ownership")
				}
				// The same charged bound must admit, one byte less must refuse before copy.
				f, v = recursiveTestVector(depth)
				tight := NewBudgetManager(bytes)
				again, e := CloneRecursiveStructRangeOwned(context.Background(), tight, v, f, span[0], span[1])
				if e != nil {
					t.Fatal("exact bound", e)
				}
				again.Close()
				small := NewBudgetManager(bytes - 1)
				bad, e := CloneRecursiveStructRangeOwned(context.Background(), small, v, f, span[0], span[1])
				if bad != nil || !errors.Is(e, ErrBudgetExceeded) || small.Used() != 0 {
					t.Fatal("underbudget result/cleanup", bad, e, small.Used())
				}
				if span[0] == 0 {
					if bytes <= prior {
						t.Fatal("recursive nodes not charged", depth, bytes, prior)
					}
					prior = bytes
				}
			}
		})
	}
}

type recursiveTestReserveManager struct {
	Manager
	calls  int
	cancel context.CancelFunc
	fail   bool
}

func (m *recursiveTestReserveManager) Reserve(ctx context.Context, n int64) (Reservation, error) {
	m.calls++
	if m.fail {
		return nil, ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if e == nil && m.cancel != nil {
		m.cancel()
	}
	return r, e
}
func TestRecursiveStructOwnedFailureAndPostReserveCancel(t *testing.T) {
	f, v := recursiveTestVector(8)
	m := &recursiveTestReserveManager{Manager: NewBudgetManager(0), fail: true}
	out, e := CloneRecursiveStructRangeOwned(context.Background(), m, v, f, 0, 3)
	if out != nil || !errors.Is(e, ErrBudgetExceeded) || m.calls != 1 || m.Used() != 0 {
		t.Fatal("single admission failure", out, e, m.calls, m.Used())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m = &recursiveTestReserveManager{Manager: NewBudgetManager(0), cancel: cancel}
	out, e = CloneRecursiveStructRangeOwned(ctx, m, v, f, 0, 3)
	if out != nil || !errors.Is(e, context.Canceled) || m.calls != 1 || m.Used() != 0 {
		t.Fatal("post-admission cancel", out, e, m.calls, m.Used())
	}
	m = &recursiveTestReserveManager{Manager: NewBudgetManager(0)}
	leaf := recursiveTestLeaf(v)
	leaf.Values.Strings["text"].Offsets = []uint32{0, 7, 1, 8}
	out, e = CloneRecursiveStructRangeOwned(context.Background(), m, v, f, 0, 3)
	if out != nil || e == nil || m.calls != 0 || m.Used() != 0 {
		t.Fatal("invalid input must reject before admission", out, e, m.calls)
	}
}

func TestRecursiveStructIntermediateNullableDomain(t *testing.T) {
	for _, nullLevel := range []int{1, 3, 6} {
		t.Run(fmt.Sprint(nullLevel), func(t *testing.T) {
			f, v := recursiveTestVector(8)
			field := &f
			node := v
			for level := 0; level < 8; level++ {
				node.Validity = Bitmap{7}
				if level == nullLevel {
					field.Nullable = true
					node.Validity = Bitmap{5}
				}
				if level < 7 {
					field = &field.Children[0]
					node = node.Values.Structs["inner"]
				}
			}
			if e := v.ValidateRecursiveWithContext(f, 3, nil); e != nil {
				t.Fatal("intermediate NULL domain", e)
			}
			// With that nullable node visible, the required leaf NULL is no longer hidden.
			node = v
			for level := 0; level < nullLevel; level++ {
				node = node.Values.Structs["inner"]
			}
			node.Validity = Bitmap{7}
			if e := v.ValidateRecursiveWithContext(f, 3, nil); e == nil {
				t.Fatal("unhidden required NULL accepted")
			}
		})
	}
}

type recursiveTestCopyContext struct {
	context.Context
	checks  int
	manager Manager
}

func (c *recursiveTestCopyContext) Err() error {
	if c.manager.Used() > 0 {
		var pcs [32]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		for {
			f, more := frames.Next()
			if strings.HasSuffix(f.Function, ".appendRecursiveString") {
				c.checks++
				if c.checks == 2 {
					return context.Canceled
				}
				break
			}
			if !more {
				break
			}
		}
	}
	return c.Context.Err()
}
func TestRecursiveStructOwnedLongCopyCancellation(t *testing.T) {
	f, v := recursiveTestVector(8)
	leaf := recursiveTestLeaf(v)
	payload := strings.Repeat("界", 44000)
	leaf.Values.Strings["text"] = &StringViewVector{Offsets: []uint32{0, uint32(len(payload)), uint32(len(payload) + 6), uint32(len(payload) + 7)}, Data: []byte(payload + "HIDDENc"), Validity: Bitmap{5}, Encoding: EncodingView}
	m := NewBudgetManager(1 << 20)
	ctx := &recursiveTestCopyContext{Context: context.Background(), manager: m}
	out, e := CloneRecursiveStructRangeOwned(ctx, m, v, f, 0, 3)
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || m.Used() != 0 {
		t.Fatal("chunk copy cancel", out, e, ctx.checks, m.Used())
	}
	if string(leaf.Values.Strings["text"].ValueBytes(0)) != payload {
		t.Fatal("failed copy mutated source")
	}
	out, e = CloneRecursiveStructRangeOwned(context.Background(), m, v, f, 0, 3)
	if e != nil {
		t.Fatal("retry", e)
	}
	if string(recursiveTestLeaf(out.Vector).Values.Strings["text"].ValueBytes(0)) != payload {
		t.Fatal("retry exact payload")
	}
	out.Close()
	if m.Used() != 0 {
		t.Fatal("retry leak")
	}
}

func TestRecursiveStructOwnedSharedOccurrencesAndWidth(t *testing.T) {
	var previous int64
	for _, width := range []int{1, 2, 64} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			childField, shared := recursiveTestVector(1)
			fields := make([]Field, width)
			batch := NewBatch()
			batch.Rows = 3
			for i := range fields {
				f := childField
				f.Name = fmt.Sprintf("child%d", i)
				fields[i] = f
				batch.Structs[f.Name] = shared
			}
			f := Field{Name: "root", Type: TypeStruct, Nullable: true, Children: fields}
			v := &StructVector{Rows: 3, Fields: fields, Values: batch, Validity: Bitmap{5}}
			m := NewBudgetManager(0)
			out, e := CloneRecursiveStructRangeOwned(context.Background(), m, v, f, 0, 3)
			if e != nil {
				t.Fatal(e)
			}
			if out.Bytes <= previous || out.Bytes != m.Used() {
				t.Fatal("nodes/edges or shared occurrences uncharged", width, out.Bytes, previous)
			}
			previous = out.Bytes
			if width > 1 {
				a, b := out.Vector.Values.Structs["child0"], out.Vector.Values.Structs["child1"]
				if a == b || a.Values == b.Values || &a.Values.Int64s["n"].Values[0] == &b.Values.Int64s["n"].Values[0] {
					t.Fatal("owned DAG occurrences alias")
				}
				a.Values.Int64s["n"].Values[0] = 999
				if b.Values.Int64s["n"].Values[0] != 11 {
					t.Fatal("shared output mutated other occurrence")
				}
			}
			out.Close()
			if m.Used() != 0 {
				t.Fatal("wide tree release")
			}
		})
	}
}

func recursiveTestAllLeaves(depth int) (Field, *StructVector) {
	v := emptyStructGatherFixture()
	v.Fields = append(v.Fields, ListInt64Field("int_list", true, true))
	v.Values.Int64Lists["int_list"] = &Int64ListVector{Offsets: []uint32{0, 2, 3, 3}, Values: &Int64Vector{Values: []int64{9223372036854775807, 123, -9223372036854775808}, Validity: Bitmap{5}}, Validity: Bitmap{3}}
	f := Field{Name: "node", Type: TypeStruct, Nullable: true, Children: v.Fields}
	for i := 1; i < depth; i++ {
		f.Name = "inner"
		b := NewBatch()
		b.Rows = 3
		b.Structs["inner"] = v
		f = Field{Name: "node", Type: TypeStruct, Nullable: true, Children: []Field{f}}
		v = &StructVector{Rows: 3, Fields: f.Children, Values: b, Validity: Bitmap{5}, Encoding: EncodingView}
	}
	return f, v
}
func recursiveTestPhysical(b *Batch, name string) reflect.Value {
	bv := reflect.ValueOf(b).Elem()
	for i := 0; i < bv.NumField(); i++ {
		m := bv.Field(i)
		if m.Kind() != reflect.Map || m.Type().Key().Kind() != reflect.String {
			continue
		}
		x := m.MapIndex(reflect.ValueOf(name))
		if x.IsValid() && !x.IsNil() {
			return x.Elem()
		}
	}
	return reflect.Value{}
}
func TestRecursiveStructOwnedAllTwentyLeaves(t *testing.T) {
	for _, depth := range []int{2, 8} {
		for _, span := range [][2]int{{0, 3}, {0, 1}, {1, 2}, {3, 0}} {
			t.Run(fmt.Sprintf("depth%d/%d+%d", depth, span[0], span[1]), func(t *testing.T) {
				f, src := recursiveTestAllLeaves(depth)
				m := NewBudgetManager(0)
				out, e := CloneRecursiveStructRangeOwned(context.Background(), m, src, f, span[0], span[1])
				if e != nil {
					t.Fatal(e)
				}
				leaf := recursiveTestLeaf(out.Vector)
				if len(leaf.Fields) != 20 {
					t.Fatal("terminal coverage", len(leaf.Fields))
				}
				if e = out.Vector.ValidateRecursiveWithContext(f, span[1], nil); e != nil {
					t.Fatal(e)
				}
				for _, field := range leaf.Fields {
					v := recursiveTestPhysical(leaf.Values, field.Name)
					if !v.IsValid() {
						t.Fatal("missing leaf", field.Name)
					}
					for i := 0; i < span[1]; i++ {
						row := span[0] + i
						valid := row == 0
						if field.Type == TypeList {
							if field.Children[0].Type == TypeInt64 {
								l := leaf.Values.Int64Lists[field.Name]
								if l.IsValid(i) != valid {
									t.Fatal("IntList NULL")
								}
								lo, hi := int(l.Offsets[i]), int(l.Offsets[i+1])
								if valid {
									if hi-lo != 2 || l.Values.Values[lo] != 9223372036854775807 || !l.Values.IsValid(lo) || l.Values.IsValid(lo+1) || l.Values.Values[lo+1] != 0 {
										t.Fatal("IntList exact copy")
									}
								} else if lo != hi {
									t.Fatal("hidden IntList payload")
								}
							} else {
								l := leaf.Values.StringLists[field.Name]
								if l.IsValid(i) != valid {
									t.Fatal("StringList NULL")
								}
								lo, hi := int(l.Offsets[i]), int(l.Offsets[i+1])
								if valid {
									if hi-lo != 2 || string(l.Values.ValueBytes(lo)) != "a" || !l.Values.IsValid(lo) || l.Values.IsValid(lo+1) || len(l.Values.ValueBytes(lo+1)) != 0 {
										t.Fatal("StringList exact copy")
									}
								} else if lo != hi {
									t.Fatal("hidden StringList payload")
								}
							}
							continue
						}
						if field.Type == TypeString {
							s := leaf.Values.Strings[field.Name]
							if s.IsValid(i) != valid || valid && string(s.ValueBytes(i)) != "a" || !valid && len(s.ValueBytes(i)) != 0 {
								t.Fatal("String exact copy")
							}
							continue
						}
						bits := v.FieldByName("Validity").Interface().(Bitmap)
						if (bits == nil || IsSet(bits, i)) != valid {
							t.Fatal("fixed NULL", field.Type)
						}
						x := v.FieldByName("Values").Index(i)
						want := float64(0)
						if valid {
							want = 7
							if field.Type == TypeBool {
								want = 1
							}
						}
						var got float64
						switch x.Kind() {
						case reflect.Float32, reflect.Float64:
							got = x.Float()
						case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
							got = float64(x.Uint())
						default:
							got = float64(x.Int())
						}
						if got != want {
							t.Fatal("fixed value", field.Type, got, want)
						}
					}
				}
				bytes := out.Bytes
				out.Close()
				if m.Used() != 0 {
					t.Fatal("all leaf cleanup")
				}
				tight := NewBudgetManager(bytes)
				again, e := CloneRecursiveStructRangeOwned(context.Background(), tight, src, f, span[0], span[1])
				if e != nil {
					t.Fatal(e)
				}
				again.Close()
				small := NewBudgetManager(bytes - 1)
				bad, e := CloneRecursiveStructRangeOwned(context.Background(), small, src, f, span[0], span[1])
				if bad != nil || !errors.Is(e, ErrBudgetExceeded) || small.Used() != 0 {
					t.Fatal("all leaf exact budget")
				}
			})
		}
	}
}
func TestRecursiveStructOwnedStringListLongCopy(t *testing.T) {
	f, v := recursiveTestAllLeaves(8)
	leaf := recursiveTestLeaf(v)
	payload := "xx" + strings.Repeat("界", 44000) + "z"
	l := leaf.Values.StringLists["list"]
	l.Values = &StringViewVector{Offsets: []uint32{0, uint32(len(payload)), uint32(len(payload)), uint32(len(payload) + 6)}, Data: []byte(payload + "HIDDEN"), Validity: Bitmap{5}, Encoding: EncodingView}
	m := NewBudgetManager(0)
	ctx := &recursiveTestCopyContext{Context: context.Background(), manager: m}
	out, e := CloneRecursiveStructRangeOwned(ctx, m, v, f, 0, 3)
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || m.Used() != 0 {
		t.Fatal("StringList copy cancellation", out, e, ctx.checks, m.Used())
	}
	// The short scalar String consumes one copy checkpoint; the second is the
	// first List payload chunk. Use a List-only terminal to prove mid-list copy.
	lf := &f
	for lf.Children[0].Type == TypeStruct {
		lf = &lf.Children[0]
	}
	lf.Children = []Field{ListStringField("list", true, true)}
	leaf.Fields = lf.Children
	delete(leaf.Values.Strings, "string")
	ctx = &recursiveTestCopyContext{Context: context.Background(), manager: m}
	out, e = CloneRecursiveStructRangeOwned(ctx, m, v, f, 0, 3)
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || m.Used() != 0 {
		t.Fatal("mid StringList copy cancellation", out, e, ctx.checks, m.Used())
	}
	out, e = CloneRecursiveStructRangeOwned(context.Background(), m, v, f, 0, 3)
	if e != nil {
		t.Fatal(e)
	}
	got := recursiveTestLeaf(out.Vector).Values.StringLists["list"]
	if string(got.Values.ValueBytes(0)) != payload || got.Values.IsValid(1) {
		t.Fatal("StringList retry values")
	}
	l.Values.Data[0] = 'q'
	if string(got.Values.ValueBytes(0)) != payload {
		t.Fatal("StringList ownership")
	}
	out.Close()
	if m.Used() != 0 {
		t.Fatal("StringList cleanup")
	}
}

func TestRecursiveStructOwnedListSliceAndOffsetContract(t *testing.T) {
	f, v := recursiveTestAllLeaves(8)
	for n := v; ; n = n.Values.Structs["inner"] {
		n.Validity = Bitmap{7}
		if len(n.Values.Structs) == 0 {
			break
		}
	}
	m := NewBudgetManager(0)
	out, e := CloneRecursiveStructRangeOwned(context.Background(), m, v, f, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	leaf := recursiveTestLeaf(out.Vector)
	ints, texts := leaf.Values.Int64Lists["int_list"], leaf.Values.StringLists["list"]
	if !reflect.DeepEqual(ints.Offsets, []uint32{0, 1}) || ints.Values.Values[0] != -9223372036854775808 || !ints.Values.IsValid(0) || !reflect.DeepEqual(texts.Offsets, []uint32{0, 1}) || string(texts.Values.ValueBytes(0)) != "HIDDEN" {
		t.Fatal("nonzero selected element offset copy")
	}
	out.Close()
	for _, text := range []bool{false, true} {
		f, v = recursiveTestAllLeaves(8)
		leaf = recursiveTestLeaf(v)
		if text {
			leaf.Values.StringLists["list"].Offsets[0] = 1
		} else {
			leaf.Values.Int64Lists["int_list"].Offsets[0] = 1
		}
		out, e = CloneRecursiveStructRangeOwned(context.Background(), m, v, f, 0, 3)
		if out != nil || e == nil || !strings.Contains(e.Error(), "first offset") || m.Used() != 0 {
			t.Fatal("native nonzero base offset contract", text, out, e)
		}
	}
}

type recursiveCollectTestManager struct {
	Manager
	calls, failAt, cancelAt int
	cancel                  context.CancelFunc
}

func (m *recursiveCollectTestManager) Reserve(ctx context.Context, n int64) (Reservation, error) {
	m.calls++
	if m.calls == m.failAt {
		return nil, ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if e == nil && m.calls == m.cancelAt {
		m.cancel()
	}
	return r, e
}
func TestRecursiveStructCollectOwnedActualPath(t *testing.T) {
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			f, v := recursiveTestAllLeaves(depth)
			f.Name = "a"
			g := f
			g.Name = "b"
			schema, e := NewSchemaChecked(f, g, Field{Name: "flat", Type: TypeInt64})
			if e != nil {
				t.Fatal(e)
			}
			src := NewBatch()
			src.Rows = 3
			src.Structs["a"] = v
			src.Structs["b"] = v
			src.Int64s["flat"] = &Int64Vector{Values: []int64{41, 42, 43}}
			manager := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			out, lease, e := CloneCollectBatchRecursive(context.Background(), manager, src, schema)
			if e != nil {
				t.Fatal(e)
			}
			if lease == nil || lease.Bytes() != manager.Used() || out.Rows != 3 {
				t.Fatal("Collect merged lease")
			}
			calls, charge := manager.calls, lease.Bytes()
			for _, name := range []string{"a", "b"} {
				if e = out.Structs[name].ValidateRecursiveWithContext(schema.Fields[0], 3, nil); e != nil {
					t.Fatal(e)
				}
				leaf := recursiveTestLeaf(out.Structs[name])
				if len(leaf.Fields) != 20 || leaf.Values.Int8s["int8"].Values[0] != 7 || leaf.Values.Int64Lists["int_list"].Values.Values[0] != 9223372036854775807 || string(leaf.Values.StringLists["list"].Values.ValueBytes(0)) != "a" {
					t.Fatal("Collect recursive oracle")
				}
			}
			recursiveTestLeaf(v).Values.Int8s["int8"].Values[0] = 99
			src.Int64s["flat"].Values[0] = 99
			if recursiveTestLeaf(out.Structs["a"]).Values.Int8s["int8"].Values[0] != 7 || out.Int64s["flat"].Values[0] != 41 {
				t.Fatal("Collect source alias")
			}
			recursiveTestLeaf(out.Structs["a"]).Values.Int8s["int8"].Values[0] = 88
			if recursiveTestLeaf(out.Structs["b"]).Values.Int8s["int8"].Values[0] != 7 {
				t.Fatal("Collect root alias")
			}
			out.ReleaseOwned()
			lease.Release()
			lease.Release()
			if manager.Used() != 0 {
				t.Fatal("Collect release")
			}
			for fail := 1; fail <= calls; fail++ {
				m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), failAt: fail}
				bad, l, e := CloneCollectBatchRecursive(context.Background(), m, src, schema)
				if bad != nil || l != nil || !errors.Is(e, ErrBudgetExceeded) || m.Used() != 0 {
					t.Fatal("Collect Reserve rollback", fail, e, m.Used())
				}
			}
			for cancelAt := 1; cancelAt <= calls; cancelAt++ {
				ctx, cancel := context.WithCancel(context.Background())
				m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), cancelAt: cancelAt, cancel: cancel}
				bad, l, e := CloneCollectBatchRecursive(ctx, m, src, schema)
				cancel()
				if bad != nil || l != nil || !errors.Is(e, context.Canceled) || m.Used() != 0 {
					t.Fatal("Collect cancel rollback", cancelAt, e, m.Used())
				}
			}
			tight := NewBudgetManager(charge)
			again, l, e := CloneCollectBatchRecursive(context.Background(), tight, src, schema)
			if e != nil {
				t.Fatal("Collect exact limit", e)
			}
			again.ReleaseOwned()
			l.Release()
			small := NewBudgetManager(charge - 1)
			bad, l, e := CloneCollectBatchRecursive(context.Background(), small, src, schema)
			if bad != nil || l != nil || !errors.Is(e, ErrBudgetExceeded) || small.Used() != 0 {
				t.Fatal("Collect -1 limit", e)
			}
		})
	}
}
func TestRecursiveStructCollectRejectsUndeclaredCycle(t *testing.T) {
	f, v := recursiveTestVector(2)
	schema, e := NewSchemaChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	src := NewBatch()
	src.Rows = 3
	src.Structs[f.Name] = v
	src.Structs["undeclared"] = &StructVector{Rows: 3, Values: src}
	m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	out, lease, e := CloneCollectBatchRecursive(context.Background(), m, src, schema)
	if out != nil || lease != nil || e == nil || m.calls != 0 || m.Used() != 0 {
		t.Fatal("undeclared runtime cycle admitted", out, e, m.calls)
	}
}

func TestRecursiveStructCollectEmptyAndMissingDeclaredRoot(t *testing.T) {
	f, v := recursiveTestAllLeaves(8)
	sourceBudget := NewBudgetManager(0)
	empty, e := CloneRecursiveStructRangeOwned(context.Background(), sourceBudget, v, f, 3, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer empty.Close()
	schema, e := NewSchemaChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	src := NewBatch()
	src.Structs[f.Name] = empty.Vector
	m := NewBudgetManager(0)
	out, lease, e := CloneCollectBatchRecursive(context.Background(), m, src, schema)
	if e != nil {
		t.Fatal(e)
	}
	if out.Rows != 0 || out.Structs[f.Name].Rows != 0 || len(recursiveTestLeaf(out.Structs[f.Name]).Fields) != 20 {
		t.Fatal("empty declared tree lost")
	}
	out.ReleaseOwned()
	lease.Release()
	if m.Used() != 0 {
		t.Fatal("empty Collect leak")
	}
	delete(src.Structs, f.Name)
	out, lease, e = CloneCollectBatchRecursive(context.Background(), m, src, schema)
	if out != nil || lease != nil || e == nil || m.Used() != 0 {
		t.Fatal("missing declared recursive root", out, e, m.Used())
	}
}
