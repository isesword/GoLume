package physical

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func limitProbe(batch *memory.Batch) *filterProbeSource {
	p := &filterProbeSource{}
	p.next = func(_ context.Context, dst *memory.Batch) (bool, error) {
		if p.nexts > 1 {
			return false, nil
		}
		bindAll(dst, batch)
		dst.Rows = batch.Rows
		return true, nil
	}
	return p
}

func TestLimitLargePrefixBorrowsUnderOneByteBudget(t *testing.T) {
	const rows = 50000
	input := memory.NewBatch()
	input.Rows = rows
	input.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, rows)}
	for i := range input.Int64s["v"].Values {
		input.Int64s["v"].Values[i] = int64(i)
	}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), input)
	if err != nil {
		t.Fatal(err)
	}
	limited, _ := NewLimitSource(base, rows-1)
	root := memory.NewEngineBudget(1)
	q, _ := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 1})
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	opts := testReadOptions()
	opts.BatchRows = rows
	stream, err := limited.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(ctx, out); !ok || err != nil {
		t.Fatalf("Next=(%v,%v)", ok, err)
	}
	if out.Rows != rows-1 || len(out.Int64s["v"].Values) != rows-1 || out.Int64s["v"].Values[rows-2] != rows-2 {
		t.Fatal("wrong limited output")
	}
	// Logical payload is 399,992 bytes, but it must not be an uncharged copy.
	if &out.Int64s["v"].Values[0] != &input.Int64s["v"].Values[0] {
		t.Fatal("Limit materialized an uncharged numeric prefix")
	}
	if q.Used() != 0 || q.Snapshot().PeakBytes != 0 || root.Used() != 0 {
		t.Fatal("borrowed payload was charged twice")
	}
	if err := stream.Close(); err != nil || out.Rows != 0 {
		t.Fatalf("Close=%v rows=%d", err, out.Rows)
	}
	if ok, err := stream.Next(ctx, out); ok || !errors.Is(err, source.ErrBatchStreamClosed) {
		t.Fatalf("Next after Close=(%v,%v)", ok, err)
	}
	if input.Rows != rows || input.Int64s["v"].Values[rows-1] != rows-1 {
		t.Fatal("closing Limit altered its source")
	}
}

// A prefix can still need storage: Arrow-style offsets may have a nonzero
// origin, and list children may start at a misaligned validity bit.
func limitRebasedBatch() *memory.Batch {
	b := memory.NewBatch()
	b.Rows = 3
	b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{2, 3, 5, 6}, Data: []byte("xxabbc"), Encoding: memory.EncodingView}
	b.StringLists["l"] = &memory.StringListVector{
		Offsets: []uint32{1, 3, 4, 5}, Encoding: memory.EncodingView,
		Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 3, 4, 5, 6}, Data: []byte("qabbcd"), Validity: memory.Bitmap{0b11011}, Encoding: memory.EncodingView},
	}
	return b
}

func TestLimitRangeMetadataAdmissionAndRelease(t *testing.T) {
	// String offsets: 12; list offsets: 12; child offsets: 16; child validity: 8.
	const bytes = 48
	for _, query := range []bool{false, true} {
		for _, hard := range []int64{bytes - 1, bytes} {
			for _, end := range []string{"eof", "close", "cancel", "nil-context", "nil-destination"} {
				t.Run(fmt.Sprintf("query=%v/hard=%d/%s", query, hard, end), func(t *testing.T) {
					input := limitRebasedBatch()
					probe := limitProbe(input)
					limited, _ := NewLimitSource(probe, 2)
					ctx := context.Background()
					manager := memory.NewBudgetManager(hard)
					var q *memory.QueryBudget
					var root *memory.EngineBudget
					opts := testReadOptions()
					opts.MemoryManager = manager
					if query {
						root = memory.NewEngineBudget(hard)
						q, _ = root.BeginQuery(memory.BudgetOptions{HardLimitBytes: hard})
						defer q.Close()
						ctx = memory.WithQueryBudget(ctx, q)
						// A query budget must win over an unrelated unlimited manager.
						opts.MemoryManager = memory.NewBudgetManager(0)
					}
					opened, err := limited.NewBatchStream(ctx, opts)
					if err != nil {
						t.Fatal(err)
					}
					stream := opened.(*limitBatchStream)
					defer stream.Close()
					out := memory.NewBatch()
					ok, err := stream.Next(context.Background(), out)
					if hard < bytes {
						if ok || !errors.Is(err, memory.ErrBudgetExceeded) || out.Rows != 0 || stream.view != nil || stream.viewLease != nil {
							t.Fatalf("failed admission=(%v,%v), rows=%d", ok, err, out.Rows)
						}
						if query {
							var resource *memory.ResourceError
							if !errors.As(err, &resource) || resource.Operator != "Limit" || resource.RequestedBytes != bytes {
								t.Fatalf("wrong resource diagnosis: %v", err)
							}
						}
						if _, again := stream.Next(ctx, out); again != err {
							t.Fatal("budget error was not terminal")
						}
					} else {
						if !ok || err != nil || out.Rows != 2 {
							t.Fatalf("admitted prefix=(%v,%v)", ok, err)
						}
						if string(out.Strings["s"].ValueBytes(0)) != "a" || string(out.Strings["s"].ValueBytes(1)) != "bb" || !reflect.DeepEqual(out.StringLists["l"].Offsets, []uint32{0, 2, 3}) {
							t.Fatal("rebased offsets changed row values")
						}
						list := out.StringLists["l"].Values
						if !list.IsValid(0) || list.IsValid(1) || !list.IsValid(2) || &list.Data[0] != &input.StringLists["l"].Values.Data[1] {
							t.Fatal("list payload/nulls not borrowed correctly")
						}
						actual := int64(cap(out.Strings["s"].Offsets)+cap(out.StringLists["l"].Offsets)+cap(list.Offsets))*4 + int64(cap(list.Validity))*8
						if actual != bytes || stream.viewLease.Bytes() != actual {
							t.Fatalf("actual metadata=%d lease=%d", actual, stream.viewLease.Bytes())
						}
						if query {
							if q.Used() != bytes || q.Snapshot().Operators["Limit"] != bytes || root.Used() != bytes || opts.MemoryManager.Used() != 0 {
								t.Fatalf("query accounting=%+v", q.Snapshot())
							}
						} else if manager.Used() != bytes {
							t.Fatalf("legacy charge=%d", manager.Used())
						}
						switch end {
						case "close":
							err = stream.Close()
						case "cancel":
							canceled, cancel := context.WithCancel(ctx)
							cancel()
							_, err = stream.Next(canceled, memory.NewBatch())
							if !errors.Is(err, context.Canceled) {
								t.Fatal(err)
							}
						case "nil-context":
							_, err = stream.Next(nil, memory.NewBatch())
							if err == nil {
								t.Fatal("nil context accepted")
							}
						case "nil-destination":
							_, err = stream.Next(ctx, nil)
							if err == nil {
								t.Fatal("nil destination accepted")
							}
						case "eof":
							if ok, nextErr := stream.Next(ctx, memory.NewBatch()); ok || nextErr != nil {
								t.Fatalf("EOF=(%v,%v)", ok, nextErr)
							}
						}
					}
					if out.Rows != 0 || stream.view != nil || stream.viewLease != nil || stream.input != nil || manager.Used() != 0 || query && (q.Used() != 0 || root.Used() != 0) {
						t.Fatal("terminal path retained output or charged storage")
					}
					stream.Close()
					stream.Close()
					if probe.nexts != 1 || probe.closes != 1 {
						t.Fatalf("child Next=%d Close=%d", probe.nexts, probe.closes)
					}
				})
			}
		}
	}
}

func TestLimitBorrowedPrefixAllStorageFamilies(t *testing.T) {
	input := limitRebasedBatch()
	valid := memory.Bitmap{0b101}
	input.Int8s["i8"] = &memory.Int8Vector{Values: []int8{-128, 1, 127}, Validity: valid}
	input.Int16s["i16"] = &memory.Int16Vector{Values: []int16{-32768, 1, 32767}, Validity: valid}
	input.Int32s["i32"] = &memory.Int32Vector{Values: []int32{-2147483648, 1, 2147483647}, Validity: valid}
	input.Int64s["i64"] = &memory.Int64Vector{Values: []int64{-1 << 63, 1, 1<<63 - 1}, Validity: valid}
	input.Uint8s["u8"] = &memory.Uint8Vector{Values: []uint8{255, 1, 0}, Validity: valid}
	input.Uint16s["u16"] = &memory.Uint16Vector{Values: []uint16{65535, 1, 0}, Validity: valid}
	input.Uint32s["u32"] = &memory.Uint32Vector{Values: []uint32{1<<32 - 1, 1, 0}, Validity: valid}
	input.Uint64s["u64"] = &memory.Uint64Vector{Values: []uint64{1<<64 - 1, 1, 0}, Validity: valid}
	input.Float32s["f32"] = &memory.Float32Vector{Values: []float32{1.5, 2.5, 3.5}, Validity: valid}
	input.Float64s["f64"] = &memory.Float64Vector{Values: []float64{1.5, 2.5, 3.5}, Validity: valid}
	input.DictStrings["d"] = &memory.DictionaryStringVector{Indices: []uint32{1, 0, 1}, Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), Validity: valid, DictionaryDomain: "domain", Encoding: memory.EncodingDictionary}
	children := memory.NewBatch()
	children.Rows = input.Rows
	children.Int8s["value"] = input.Int8s["i8"]
	input.Structs["record"] = &memory.StructVector{Rows: 3, Values: children, Fields: []memory.Field{{Name: "value", Type: memory.TypeInt8}}, Validity: valid}
	probe := limitProbe(input)
	limited, _ := NewLimitSource(probe, 2)
	stream, err := limited.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("Next=(%v,%v)", ok, err)
	}
	for _, field := range []string{"Int8s", "Int16s", "Int32s", "Int64s", "Uint8s", "Uint16s", "Uint32s", "Uint64s", "Float32s", "Float64s"} {
		srcMap := reflect.ValueOf(input).Elem().FieldByName(field)
		dstMap := reflect.ValueOf(out).Elem().FieldByName(field)
		for _, key := range srcMap.MapKeys() {
			a, b := srcMap.MapIndex(key).Elem(), dstMap.MapIndex(key).Elem()
			x, y := a.FieldByName("Values"), b.FieldByName("Values")
			if y.Len() != 2 || x.Pointer() != y.Pointer() || !reflect.DeepEqual(y.Interface(), x.Slice(0, 2).Interface()) || a.FieldByName("Validity").Pointer() != b.FieldByName("Validity").Pointer() || a.FieldByName("Encoding").Interface() != b.FieldByName("Encoding").Interface() {
				t.Fatalf("%s payload/metadata changed or copied", field)
			}
		}
	}
	d := out.DictStrings["d"]
	if d.DictionaryDomain != "domain" || d.Encoding != memory.EncodingDictionary || &d.Indices[0] != &input.DictStrings["d"].Indices[0] || &d.Data[0] != &input.DictStrings["d"].Data[0] {
		t.Fatal("dictionary identity/payload not preserved")
	}
	record := out.Structs["record"]
	if record.Rows != 2 || record.Values.Rows != 2 || record.IsValid(1) || &record.Values.Int8s["value"].Values[0] != &children.Int8s["value"].Values[0] {
		t.Fatal("struct prefix not borrowed")
	}
	out.ClearColumns()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if input.Rows != 3 || children.Rows != 3 || len(children.Int8s["value"].Values) != 3 || valid[0] != 0b101 {
		t.Fatal("clearing output modified source")
	}
}

func TestLimitArrowOwnershipAndLifecycle(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, nested := range []bool{false, true} {
			for _, end := range []string{"eof", "close", "clear", "cancel", "child-error"} {
				if partial && end == "child-error" {
					continue
				}
				t.Run(fmt.Sprintf("partial=%v/nested=%v/%s", partial, nested, end), func(t *testing.T) {
					allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
					defer allocator.AssertSize(t, 0)
					builder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int32)
					for i := 0; i < 6; i++ {
						builder.Append(i != 3)
						builder.ValueBuilder().(*array.Int32Builder).Append(int32(i))
					}
					whole := builder.NewArray()
					builder.Release()
					arr := array.NewSlice(whole, 2, 5)
					whole.Release()
					raw := &memory.OpaqueVector{Rows: 3, Type: memory.TypeArrow, ArrowType: arr.DataType().String(), Value: arr, Owned: true, Validity: memory.Bitmap{0b101}}
					input := memory.NewBatch()
					input.Rows = 3
					if nested {
						child := memory.NewBatch()
						child.Rows = 3
						child.Opaque["raw"] = raw
						input.Structs["record"] = &memory.StructVector{Rows: 3, Values: child, Fields: []memory.Field{{Name: "raw", Type: memory.TypeArrow}}}
					} else {
						input.Opaque["raw"] = raw
					}
					probe := limitProbe(input)
					first := memory.NewBatch()
					var stream *limitBatchStream
					checkReleased := func() {
						if first.Rows != 0 || stream.view != nil || stream.borrowed != nil || !raw.Owned {
							t.Error("output survives invalidation or source ownership changed")
						}
					}
					nextErr, closeErr := errors.New("child next"), errors.New("child close")
					firstNext := probe.next
					probe.next = func(ctx context.Context, dst *memory.Batch) (bool, error) {
						if probe.nexts == 1 {
							return firstNext(ctx, dst)
						}
						checkReleased()
						if end == "child-error" {
							return false, nextErr
						}
						return false, nil
					}
					probe.close = func() error {
						checkReleased()
						input.ReleaseOwned()
						allocator.AssertSize(t, 0)
						if end == "child-error" {
							return closeErr
						}
						return nil
					}
					limit := int64(10)
					if partial {
						limit = 2
					}
					limited, _ := NewLimitSource(probe, limit)
					opened, err := limited.NewBatchStream(context.Background(), testReadOptions())
					if err != nil {
						t.Fatal(err)
					}
					stream = opened.(*limitBatchStream)
					defer stream.Close()
					if ok, err := stream.Next(context.Background(), first); !ok || err != nil {
						t.Fatalf("Next=(%v,%v)", ok, err)
					}
					view := first
					if nested {
						view = first.Structs["record"].Values
					}
					got := view.Opaque["raw"]
					if got.Owned || got == raw || !raw.Owned || got.Value.(arrow.Array).Len() != first.Rows || got.Value.(arrow.Array).IsNull(0) || !got.Value.(arrow.Array).IsNull(1) {
						t.Fatal("Arrow range values, nulls, or ownership changed")
					}
					if end == "clear" {
						first.ClearColumns()
						if !raw.Owned || arr.Len() != 3 {
							t.Fatal("caller clear released the source")
						}
					}
					if end == "close" || end == "clear" {
						err = stream.Close()
					} else {
						ctx := context.Background()
						if end == "cancel" {
							var cancel context.CancelFunc
							ctx, cancel = context.WithCancel(ctx)
							cancel()
						}
						var ok bool
						ok, err = stream.Next(ctx, memory.NewBatch())
						if ok {
							t.Fatal("unexpected output")
						}
					}
					if end == "child-error" {
						if !errors.Is(err, nextErr) || !errors.Is(err, closeErr) {
							t.Fatalf("lost child error: %v", err)
						}
						if _, again := stream.Next(context.Background(), memory.NewBatch()); again != err {
							t.Fatal("error not terminal")
						}
					} else if end == "cancel" {
						if !errors.Is(err, context.Canceled) {
							t.Fatal(err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					stream.Close()
					if probe.closes != 1 || partial && probe.nexts != 1 || first.Rows != 0 {
						t.Fatal("close/early-stop lifecycle failed")
					}
				})
			}
		}
	}
}

func TestLimitQueryOwnershipAndCanceledOpen(t *testing.T) {
	background := context.Background()
	q1, _ := memory.NewEngineBudget(1000).BeginQuery(memory.BudgetOptions{})
	defer q1.Close()
	q2, _ := memory.NewEngineBudget(1000).BeginQuery(memory.BudgetOptions{})
	defer q2.Close()
	contexts := []context.Context{background, memory.WithQueryBudget(background, q1), memory.WithQueryBudget(background, q2)}
	for i, openCtx := range contexts {
		for j, nextCtx := range contexts {
			if i == j || j == 0 {
				continue
			}
			probe := limitProbe(limitRebasedBatch())
			limited, _ := NewLimitSource(probe, 2)
			stream, err := limited.NewBatchStream(openCtx, testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := stream.Next(nextCtx, memory.NewBatch()); ok || err == nil {
				t.Fatal("query drift accepted")
			}
			stream.Close()
			if probe.nexts != 0 || probe.closes != 1 || q1.Used() != 0 || q2.Used() != 0 {
				t.Fatal("query drift advanced child or leaked")
			}
		}
	}
	canceled, cancel := context.WithCancel(background)
	cancel()
	for _, ctx := range []context.Context{nil, canceled} {
		probe := limitProbe(limitRebasedBatch())
		limited, _ := NewLimitSource(probe, 2)
		if _, err := limited.NewBatchStream(ctx, testReadOptions()); err == nil || probe.opens != 0 {
			t.Fatal("invalid open reached child")
		}
	}
}

func TestLimitZeroEmptyAndCancellationDuringChildNext(t *testing.T) {
	for _, mode := range []string{"zero", "empty", "cancel-during-next"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			probe := limitProbe(limitRebasedBatch())
			firstNext := probe.next
			probe.next = func(ctx context.Context, dst *memory.Batch) (bool, error) {
				if mode == "empty" {
					dst.Rows = 0
					return probe.nexts == 1, nil
				}
				ok, err := firstNext(ctx, dst)
				if mode == "cancel-during-next" {
					cancel()
				}
				return ok, err
			}
			n := int64(2)
			if mode == "zero" {
				n = 0
			}
			limited, _ := NewLimitSource(probe, n)
			stream, err := limited.NewBatchStream(ctx, testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			ok, err := stream.Next(ctx, out)
			if ok || out.Rows != 0 || probe.closes != 1 {
				t.Fatalf("Next=(%v,%v) closes=%d", ok, err, probe.closes)
			}
			if mode == "cancel-during-next" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if mode == "zero" && probe.nexts != 0 || mode == "empty" && probe.nexts != 2 {
				t.Fatalf("Next count=%d", probe.nexts)
			}
		})
	}
}

func TestLimitKeepsChildChargedUntilBorrowExpires(t *testing.T) {
	ctx := context.Background()
	root := memory.NewEngineBudget(24)
	q, _ := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 24})
	defer q.Close()
	ctx = memory.WithQueryBudget(ctx, q)
	input := memory.NewBatch()
	input.Rows = 3
	input.Int64s["x"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	probe := limitProbe(input)
	var lease memory.Reservation
	var previous *memory.Batch
	probe.next = func(ctx context.Context, dst *memory.Batch) (bool, error) {
		if previous != nil && previous.Rows != 0 {
			t.Fatal("previous destination survives child reuse")
		}
		if lease == nil {
			var err error
			lease, err = q.Operator("test-child", memory.MemoryWorkspace).Reserve(ctx, 24)
			if err != nil {
				return false, err
			}
		}
		for i := range input.Int64s["x"].Values {
			input.Int64s["x"].Values[i] = int64((probe.nexts-1)*3 + i)
		}
		bindAll(dst, input)
		dst.Rows = input.Rows
		return true, nil
	}
	probe.close = func() error {
		if previous != nil && previous.Rows != 0 {
			t.Error("child closed before borrowed output was cleared")
		}
		input.Int64s["x"].Values = nil
		lease.Release()
		return nil
	}
	limited, _ := NewLimitSource(probe, 5)
	stream, err := limited.NewBatchStream(ctx, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for _, want := range [][]int64{{0, 1, 2}, {3, 4}} {
		out := memory.NewBatch()
		if ok, err := stream.Next(ctx, out); !ok || err != nil {
			t.Fatalf("Next=(%v,%v)", ok, err)
		}
		if !reflect.DeepEqual(out.Int64s["x"].Values, want) || &out.Int64s["x"].Values[0] != &input.Int64s["x"].Values[0] {
			t.Fatal("borrowed child buffer corrupted or copied")
		}
		if q.Used() != 24 || q.Snapshot().PeakBytes != 24 || probe.closes != 0 {
			t.Fatal("child payload released early or charged twice")
		}
		previous = out
	}
	if ok, err := stream.Next(ctx, memory.NewBatch()); ok || err != nil {
		t.Fatalf("EOF=(%v,%v)", ok, err)
	}
	if probe.nexts != 2 || probe.closes != 1 || q.Used() != 0 || root.Used() != 0 {
		t.Fatal("early stop leaked or advanced child")
	}
}

func TestLimitRangeFailureJoinsChildCloseError(t *testing.T) {
	probe := limitProbe(limitRebasedBatch())
	closeErr := errors.New("close failure")
	probe.close = func() error { return closeErr }
	limited, _ := NewLimitSource(probe, 2)
	opts := testReadOptions()
	opts.MemoryManager = memory.NewBudgetManager(1)
	stream, err := limited.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	ok, err := stream.Next(context.Background(), out)
	if ok || !errors.Is(err, memory.ErrBudgetExceeded) || !errors.Is(err, closeErr) || out.Rows != 0 {
		t.Fatalf("Next=(%v,%v)", ok, err)
	}
	if _, again := stream.Next(context.Background(), out); again != err {
		t.Fatal("error was not terminal")
	}
	if err := stream.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close=%v", err)
	}
	if probe.closes != 1 || opts.MemoryManager.Used() != 0 {
		t.Fatal("error cleanup leaked or closed twice")
	}
}

func TestLimitMalformedPrefixClosesChild(t *testing.T) {
	for _, kind := range []string{"numeric", "string", "list", "struct", "opaque"} {
		t.Run(kind, func(t *testing.T) {
			input := memory.NewBatch()
			input.Rows = 3
			switch kind {
			case "numeric":
				input.Int64s["x"] = &memory.Int64Vector{Values: []int64{1}}
			case "string":
				input.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{0, 2, 4, 6}, Data: []byte("a")}
			case "list":
				input.StringLists["x"] = &memory.StringListVector{Offsets: []uint32{0, 1, 2, 3}}
			case "struct":
				input.Structs["x"] = &memory.StructVector{Rows: 3}
			case "opaque":
				input.Opaque["x"] = &memory.OpaqueVector{Rows: 3, Value: struct{}{}}
			}
			probe := limitProbe(input)
			limited, _ := NewLimitSource(probe, 2)
			opts := testReadOptions()
			opts.MemoryManager = memory.NewBudgetManager(1024)
			stream, err := limited.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), out); ok || err == nil {
				t.Fatalf("malformed prefix=(%v,%v)", ok, err)
			}
			if probe.closes != 1 || opts.MemoryManager.Used() != 0 || out.Rows != 0 {
				t.Fatal("malformed prefix retained resources")
			}
		})
	}
}

func TestLimitResidentSortTransferredArrowOutput(t *testing.T) {
	for _, n := range []int64{2, 3, 4} {
		for _, end := range []string{"close", "eof", "cancel"} {
			t.Run(fmt.Sprintf("limit=%d/%s", n, end), func(t *testing.T) {
				// Resident Sort gathers with the default allocator and transfers
				// owned Arrow output into Limit's input, rather than retaining it
				// in the child stream. Track those allocations, not only the source.
				allocator := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
				previous := arrowmem.DefaultAllocator
				arrowmem.DefaultAllocator = allocator
				defer func() { arrowmem.DefaultAllocator = previous }()
				defer allocator.AssertSize(t, 0)
				builder := array.NewListBuilder(allocator, arrow.PrimitiveTypes.Int32)
				for i := 0; i < 3; i++ {
					builder.Append(true)
					builder.ValueBuilder().(*array.Int32Builder).Append(int32(i))
				}
				arr := builder.NewArray()
				builder.Release()
				defer arr.Release()
				input := memory.NewBatch()
				input.Rows = 3
				input.Int64s["id"] = &memory.Int64Vector{Values: []int64{2, 0, 1}}
				input.Opaque["raw"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeArrow, ArrowType: arr.DataType().String(), Value: arr}
				schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: arr.DataType().String()})
				base, err := inmemory.New(schema, input)
				if err != nil {
					t.Fatal(err)
				}
				sorted, err := NewSortSource(base, []SortKey{{Column: "id"}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				limited, _ := NewLimitSource(sorted, n)
				stream, err := limited.NewBatchStream(context.Background(), testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
					t.Fatalf("Next=(%v,%v)", ok, err)
				}
				if !reflect.DeepEqual(out.Int64s["id"].Values, []int64{0, 1, 2}[:min(n, 3)]) {
					t.Fatal("wrong sorted prefix")
				}
				list := out.Opaque["raw"].Value.(*array.List)
				values := list.ListValues().(*array.Int32)
				for i, want := range []int32{1, 2, 0}[:min(n, 3)] {
					lo, hi := list.ValueOffsets(i)
					if hi-lo != 1 || values.Value(int(lo)) != want {
						t.Fatal("opaque sorted row changed")
					}
				}
				out.ClearColumns()
				if end != "close" {
					ctx := context.Background()
					if end == "cancel" {
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					}
					ok, err := stream.Next(ctx, out)
					if ok || end == "eof" && err != nil || end == "cancel" && !errors.Is(err, context.Canceled) {
						t.Fatalf("terminal Next=(%v,%v)", ok, err)
					}
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestLimitTransferredArrowCleanupAfterChildCloseError(t *testing.T) {
	allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	builder := array.NewInt64Builder(allocator)
	builder.AppendValues([]int64{1, 2, 3}, nil)
	arr := builder.NewArray()
	builder.Release()
	input := memory.NewBatch()
	input.Rows = 3
	input.Opaque["raw"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeArrow, Value: arr, Owned: true}
	probe := limitProbe(input)
	closeErr := errors.New("transferring child close failure")
	probe.close = func() error { return closeErr }
	limited, _ := NewLimitSource(probe, 2)
	stream, err := limited.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("Next=(%v,%v)", ok, err)
	}
	for i := 0; i < 2; i++ {
		if err := stream.Close(); !errors.Is(err, closeErr) {
			t.Fatal(err)
		}
	}
	if out.Rows != 0 || probe.closes != 1 || input.Opaque["raw"].Owned {
		t.Fatal("transferred output retained after child close failure")
	}
}
