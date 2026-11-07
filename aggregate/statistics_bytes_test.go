package aggregate

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math"
	"reflect"
	"testing"
	"unsafe"
)

func TestStatisticsBytesGrowthOwnership(t *testing.T) {
	budget := memory.NewBudgetManager(1 << 25)
	fail := false
	reserve := func(n int64) (memory.Reservation, error) {
		if fail {
			return nil, errors.New("injected")
		}
		return budget.Reserve(context.Background(), n)
	}
	types := []memory.DataType{memory.TypeString, memory.TypeBinary, memory.TypeInt64}
	layouts := []StatisticsLayout{{First: true}, {Last: true}, {Moments: true}}
	b, err := NewStatisticsBundle(types, layouts, reserve)
	if err != nil {
		t.Fatal(err)
	}
	for g := 0; g < 65; g++ {
		if err = b.EnsureGroups(g + 1); err != nil {
			t.Fatal(err)
		}
		input := []byte{byte(g), 255}
		for c := 0; c < 2; c++ {
			if err = b.AddBytes(g, c, input, true, SourceOrdinal{0, 1}); err != nil {
				t.Fatal(err)
			}
		}
		input[0] = 200
		if b.RetainedBytes() != budget.Used() {
			t.Fatal("retained accounting", b.RetainedBytes(), budget.Used())
		}
	}
	for g := 0; g < 65; g++ {
		for c := 0; c < 2; c++ {
			r := b.BytesResult(g, c)
			if len(r.First) != 2 || r.First[0] != byte(g) || r.Last[0] != byte(g) {
				t.Fatal("growth or input alias", g, r)
			}
		}
	}
	before, cap, used := b.BytesResult(0, 0), b.Capacity(), budget.Used()
	fail = true
	if b.EnsureGroups(cap+1) == nil || b.Capacity() != cap || budget.Used() != used || !reflect.DeepEqual(before, b.BytesResult(0, 0)) {
		t.Fatal("failed growth mutated state")
	}
	fail = false
	if err = b.EnsureGroups(cap + 1); err != nil {
		t.Fatal(err)
	}
	used = budget.Used()
	if err = b.ClearGroup(0); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != used-8 || b.BytesResult(0, 0).Seen {
		t.Fatal("reset leaked")
	}
	if err = b.AddBytes(0, 0, nil, true, SourceOrdinal{}); err != nil {
		t.Fatal(err)
	}
	if !b.BytesResult(0, 0).FirstValid {
		t.Fatal("empty is null")
	}
	if err = b.AddBytes(0, 0, nil, false, SourceOrdinal{1, 0}); err != nil {
		t.Fatal(err)
	}
	if b.BytesResult(0, 0).LastValid {
		t.Fatal("boundary null lost")
	}
	b.Close()
	b.Close()
	if budget.Used() != 0 {
		t.Fatal("close leak", budget.Used())
	}
}

func TestStatisticsBytesPrepareAllOrAbort(t *testing.T) {
	budget := memory.NewBudgetManager(1 << 20)
	calls, failAt := 0, 0
	sentinel := errors.New("injected")
	reserve := func(n int64) (memory.Reservation, error) {
		calls++
		if calls == failAt {
			return nil, sentinel
		}
		return budget.Reserve(context.Background(), n)
	}
	b, err := NewStatisticsBundle([]memory.DataType{memory.TypeString, memory.TypeBinary}, []StatisticsLayout{{First: true}, {Last: true}}, reserve)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err = b.EnsureGroups(1); err != nil {
		t.Fatal(err)
	}
	// Account the caller-owned pending array explicitly, as execution must do.
	scratch, err := reserve(int64(2 * unsafe.Sizeof(BytesEndpointMutation{})))
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.Release()
	pending := make([]BytesEndpointMutation, 2)
	base := budget.Used()
	for rejected := 1; rejected <= 4; rejected++ {
		failAt = calls + rejected
		failed := false
		for c := 0; c < 2; c++ {
			pending[c], err = b.PrepareBytes(0, c, []byte("row"), true, SourceOrdinal{})
			if err != nil {
				failed = true
				break
			}
		}
		if !failed {
			t.Fatal("expected failure")
		}
		for c := range pending {
			pending[c].Abort()
			pending[c].Abort()
		}
		if budget.Used() != base || b.BytesResult(0, 0).Seen || b.BytesResult(0, 1).Seen {
			t.Fatal("prepare failure not atomic")
		}
	}
	failAt = 0
	input := []byte("row")
	for c := 0; c < 2; c++ {
		pending[c], err = b.PrepareBytes(0, c, input, true, SourceOrdinal{})
		if err != nil {
			t.Fatal(err)
		}
	}
	input[0] = 'X'
	if b.BytesResult(0, 0).Seen || budget.Used() != base+12 {
		t.Fatal("prepare mutated target or wrong overlap")
	}
	for c := range pending {
		pending[c].Commit()
		pending[c].Commit()
		pending[c].Abort()
	}
	if string(b.BytesResult(0, 0).First) != "row" || budget.Used() != base+12 {
		t.Fatal("commit alias/leak")
	}
	incoming := BytesEndpointResult{Type: memory.TypeString, Seen: true, First: []byte("row"), Last: []byte("later"), FirstValid: true, LastValid: true, LastOrdinal: SourceOrdinal{2, 0}}
	p, err := b.PrepareMergeBytes(0, 0, incoming)
	if err != nil {
		t.Fatal(err)
	}
	if string(b.BytesResult(0, 0).Last) != "row" {
		t.Fatal("merge preparation mutated")
	}
	p.Abort()
	if budget.Used() != base+12 {
		t.Fatal("merge abort leak")
	}
	p, err = b.PrepareMergeBytes(0, 0, incoming)
	if err != nil {
		t.Fatal(err)
	}
	p.Commit()
	if string(b.BytesResult(0, 0).Last) != "later" {
		t.Fatal("merge failed")
	}
}

func TestStatisticsBytesDescriptorsAndMisuse(t *testing.T) {
	reserve := func(n int64) (memory.Reservation, error) {
		return memory.NewBudgetManager(1<<20).Reserve(context.Background(), n)
	}
	for _, typ := range []memory.DataType{memory.TypeString, memory.TypeBinary} {
		for _, l := range []StatisticsLayout{{First: true}, {Last: true}, {First: true, Last: true}} {
			n, err := StatisticsBundleBytes(8, []memory.DataType{typ}, []StatisticsLayout{l})
			if err != nil || n != int64(unsafe.Sizeof(statisticsLane{})+8*unsafe.Sizeof(BytesEndpointState{})) {
				t.Fatal(n, err)
			}
			if _, err = StatisticsBundleBytes(math.MaxInt, []memory.DataType{typ}, []StatisticsLayout{l}); err == nil {
				t.Fatal("overflow")
			}
		}
	}
	b, err := NewStatisticsBundle([]memory.DataType{memory.TypeString, memory.TypeInt64}, []StatisticsLayout{{First: true}, {Moments: true}}, reserve)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err = b.EnsureGroups(1); err != nil {
		t.Fatal(err)
	}
	if b.AddBits(0, 0, 0, true, SourceOrdinal{}) == nil || b.Merge(0, 0, b.Result(0, 0)) == nil {
		t.Fatal("fixed API accepted byte lane")
	}
	if _, err = b.PrepareBytes(0, 1, nil, true, SourceOrdinal{}); err == nil {
		t.Fatal("byte API accepted numeric")
	}
	if _, err = b.PrepareBytes(-1, 0, nil, true, SourceOrdinal{}); err == nil {
		t.Fatal("bad group")
	}
	if _, err = b.PrepareMergeBytes(0, 0, BytesEndpointResult{Type: memory.TypeBinary}); err == nil {
		t.Fatal("bad type")
	}
	if b.Result(0, 0) != (StatisticsResult{Type: memory.TypeString, Layout: StatisticsLayout{First: true}}) {
		t.Fatal("fixed result changed")
	}
	b.Close()
	if _, err = b.PrepareBytes(0, 0, nil, true, SourceOrdinal{}); err == nil {
		t.Fatal("closed prepared")
	}
}
