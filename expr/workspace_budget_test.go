package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"strconv"
	"testing"
	"unsafe"
)

func workspaceTestProgram(t *testing.T) Program {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	p, _, err := CompilePredicate(schema, And(Contains(Col("s"), StringLiteral{Value: "foo"}), Or(EQ(Col("s"), StringLiteral{Value: "foobar"}), IsNull(Col("s")))))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func workspaceTestBatch(rows int) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = rows
	offsets := make([]uint32, rows+1)
	data := make([]byte, rows*6)
	validity := make(memory.Bitmap, memory.WordsFor(rows))
	for i := 0; i < rows; i++ {
		copy(data[i*6:], "foobar")
		offsets[i+1] = uint32((i + 1) * 6)
		if i%3 != 0 {
			memory.Set(validity, i)
		}
	}
	b.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: validity, Encoding: memory.EncodingView}
	return b
}

func TestProgramWorkspaceBytesExactFreshCapacities(t *testing.T) {
	p := workspaceTestProgram(t)
	metadata := int64(unsafe.Sizeof(BoolVector{}) + unsafe.Sizeof(dictionaryPredicateCache{}) + unsafe.Sizeof([]byte(nil)) + unsafe.Sizeof(""))
	for _, rows := range []int{0, 1, 63, 64, 65} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			n, err := p.WorkspaceBytes(rows)
			if err != nil {
				t.Fatal(err)
			}
			want := int64(len(p.Instructions))*(metadata+16*int64((rows+63)/64)) + 9
			if n != want {
				t.Fatalf("estimate=%d want=%d", n, want)
			}
			var w EvalWorkspace
			if _, err := p.Evaluate(workspaceTestBatch(rows), &w, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			if got := w.MemoryBytes(); got != n {
				t.Fatalf("capacity bytes=%d estimate=%d", got, n)
			}
			for _, lit := range w.literals {
				if cap(lit) != len(lit) {
					t.Fatalf("literal capacity=%d length=%d", cap(lit), len(lit))
				}
			}
		})
	}
}

func TestProgramWorkspaceBytesGrowthReuseAndDictionaryExclusion(t *testing.T) {
	p := workspaceTestProgram(t)
	var w EvalWorkspace
	maxRows := 0
	for _, rows := range []int{0, 1, 63, 64, 65, 1, 0, 129, 65} {
		if rows > maxRows {
			maxRows = rows
		}
		if _, err := p.Evaluate(workspaceTestBatch(rows), &w, testKernelSet()); err != nil {
			t.Fatal(err)
		}
		want, err := p.WorkspaceBytes(maxRows)
		if err != nil {
			t.Fatal(err)
		}
		if got := w.MemoryBytes(); got != want {
			t.Fatalf("rows=%d retained=%d want=%d", rows, got, want)
		}
	}
	base, _ := p.WorkspaceBytes(maxRows)
	w.dictionary[0].matches = make([]bool, 7)
	if got := w.MemoryBytes(); got != base+7 {
		t.Fatalf("dynamic matches: got=%d want=%d", got, base+7)
	}
	if got, _ := p.WorkspaceBytes(maxRows); got != base {
		t.Fatal("estimate included runtime dictionary")
	}
	batch := workspaceTestBatch(65)
	ks := testKernelSet()
	if allocs := testing.AllocsPerRun(100, func() {
		if _, err := p.Evaluate(batch, &w, ks); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("reused evaluation allocated: %g", allocs)
	}
}

func TestProgramWorkspaceBytesRejectsInvalidSizes(t *testing.T) {
	p := Program{Instructions: []Instruction{{Code: codeConstTrue}}, Root: 0}
	for _, rows := range []int{-1, int(^uint(0) >> 1)} {
		if _, err := p.WorkspaceBytes(rows); err == nil {
			t.Fatalf("accepted rows=%d", rows)
		}
		var w EvalWorkspace
		if _, err := p.Evaluate(&memory.Batch{Rows: rows}, &w, testKernelSet()); err == nil {
			t.Fatalf("Evaluate accepted rows=%d", rows)
		}
		if w.MemoryBytes() != 0 {
			t.Fatal("allocated before rejecting invalid rows")
		}
	}
	if strconv.IntSize == 64 {
		huge := Program{Instructions: make([]Instruction, 64)}
		if _, err := huge.WorkspaceBytes(int(^uint(0)>>1) - 63); err == nil {
			t.Fatal("accepted overflowing workspace total")
		}
		var w EvalWorkspace
		if _, err := huge.Evaluate(&memory.Batch{Rows: int(^uint(0)>>1) - 63}, &w, testKernelSet()); err == nil {
			t.Fatal("Evaluate accepted overflowing workspace total")
		}
		if w.MemoryBytes() != 0 {
			t.Fatal("Evaluate allocated before rejecting overflowing workspace total")
		}
	}
	if n, err := (Program{}).WorkspaceBytes(65); err != nil || n != 0 {
		t.Fatalf("empty estimate=%d err=%v", n, err)
	}
}

func TestDictionaryWorkspaceReservationOverlapAndRelease(t *testing.T) {
	for _, limit := range []int64{5, 6} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			manager := memory.NewBudgetManager(limit)
			var requested []int64
			peak := int64(0)
			var w EvalWorkspace
			w.SetReserveHook(func(n int64) (memory.Reservation, error) {
				requested = append(requested, n)
				r, err := manager.Reserve(context.Background(), n)
				if manager.Used() > peak {
					peak = manager.Used()
				}
				return r, err
			})
			p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), EQ(Col("s"), StringLiteral{Value: ""}))
			if err != nil {
				t.Fatal(err)
			}
			evaluate := func(n int) error {
				b := memory.NewBatch()
				b.Rows = 1
				b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: make([]uint32, n+1), DictionaryDomain: fmt.Sprint(n)}
				_, err := p.Evaluate(b, &w, testKernelSet())
				return err
			}
			if err := evaluate(2); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 2 {
				t.Fatalf("first charge=%d", manager.Used())
			}
			err = evaluate(4)
			if limit == 5 {
				if !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("expected overlap rejection, got %v", err)
				}
				if manager.Used() != 2 || cap(w.dictionary[0].matches) != 2 {
					t.Fatal("failed growth changed old allocation or reservation")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if peak != 6 || manager.Used() != 4 {
					t.Fatalf("peak=%d retained=%d", peak, manager.Used())
				}
				if err := evaluate(1); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 4 {
					t.Fatal("shrinking dictionary lost retained charge")
				}
			}
			if len(requested) != 2 || requested[0] != 2 || requested[1] != 4 {
				t.Fatalf("requests=%v", requested)
			}
			// Inactive slots still own reservations, including across metadata growth.
			w.Ensure(0, 0)
			w.Ensure(0, 2)
			if w.dictionary[0].reservation == nil {
				t.Fatal("metadata growth dropped inactive reservation")
			}
			w.Release()
			w.Release()
			if manager.Used() != 0 || w.MemoryBytes() != 0 {
				t.Fatalf("release left manager=%d workspace=%d", manager.Used(), w.MemoryBytes())
			}
		})
	}
}

func TestDictionaryWorkspaceReserveFailureRetry(t *testing.T) {
	manager := memory.NewBudgetManager(6)
	blocker, err := manager.Reserve(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Release()
	var w EvalWorkspace
	w.SetReserveHook(func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) })
	defer w.Release()
	p, _, err := CompilePredicate(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), EQ(Col("s"), StringLiteral{Value: "a"}))
	if err != nil {
		t.Fatal(err)
	}
	evaluate := func(data string, domain string) (BoolVector, error) {
		b := memory.NewBatch()
		b.Rows = 1
		offsets := make([]uint32, len(data)+1)
		for i := range offsets {
			offsets[i] = uint32(i)
		}
		b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: offsets, Data: []byte(data), DictionaryDomain: domain}
		return p.Evaluate(b, &w, testKernelSet())
	}
	check := func(data, domain string, want bool) {
		t.Helper()
		out, err := evaluate(data, domain)
		if err != nil {
			t.Fatal(err)
		}
		if memory.IsSet(out.Truth, 0) != want {
			t.Fatalf("domain %s truth=%v want=%v", domain, out.Truth, want)
		}
		if w.dictionary[0].reserveErr != nil {
			t.Fatal("reserve error remained sticky")
		}
	}
	check("ab", "old", true)
	old := w.dictionary[0].reservation
	if _, err := evaluate("bcde", "new"); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("growth error=%v", err)
	}
	if w.dictionary[0].reservation != old || manager.Used() != 3 {
		t.Fatal("failed growth changed retained ownership")
	}
	check("ab", "old", true)
	if w.dictionary[0].reservation != old || manager.Used() != 3 {
		t.Fatal("return to old dictionary changed reservation")
	}
	blocker.Release()
	check("bcde", "new", false)
	if w.dictionary[0].reservation == old || manager.Used() != 4 {
		t.Fatal("retry failed to replace and release old reservation")
	}
	// The failed reservation must not poison either a successful retry or reuse.
	check("ab", "old", true)
	if manager.Used() != 4 {
		t.Fatal("return to old dictionary failed to retain larger capacity")
	}
	w.Release()
	if manager.Used() != 0 {
		t.Fatalf("workspace leaked %d bytes", manager.Used())
	}
}
