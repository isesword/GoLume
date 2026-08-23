package kernel

import (
	"gocolumn/memory"
	"testing"
)

func TestScalarInt64GTTailAndNulls(t *testing.T) {
	vals := make([]int64, 70)
	valid := make(memory.Bitmap, memory.WordsFor(len(vals)))
	for i := range vals {
		vals[i] = int64(i)
		if i%7 != 0 {
			memory.Set(valid, i)
		}
	}
	out := make(memory.Bitmap, memory.WordsFor(len(vals)))
	n := scalarInt64GT(vals, valid, out, 40)
	want := 0
	for i, v := range vals {
		ok := i%7 != 0 && v > 40
		if ok {
			want++
		}
		if memory.IsSet(out, i) != ok {
			t.Fatalf("row=%d bit mismatch", i)
		}
	}
	if n != want {
		t.Fatalf("selected=%d want=%d", n, want)
	}
}

func TestAutoKernelCorrectness(t *testing.T) {
	vals := make([]int64, 131)
	valid := make(memory.Bitmap, memory.WordsFor(len(vals)))
	for i := range vals {
		vals[i] = int64(i*3 - 5)
		if i%5 != 1 {
			memory.Set(valid, i)
		}
	}
	want := make(memory.Bitmap, memory.WordsFor(len(vals)))
	got := make(memory.Bitmap, memory.WordsFor(len(vals)))
	wn := scalarInt64GT(vals, valid, want, 100)
	set := New(ModeAuto)
	gn := set.Int64GT(vals, valid, got, 100)
	if wn != gn {
		t.Fatalf("count got=%d want=%d caps=%+v", gn, wn, set.Caps)
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("word %d mismatch got=%x want=%x caps=%+v", i, got[i], want[i], set.Caps)
		}
	}
}
