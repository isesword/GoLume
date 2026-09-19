package benchtrack

import (
	"testing"
	"time"
)

func rec(name, commit string, ts time.Time, ns float64) Record {
	return Record{
		SchemaVersion: SchemaVersion,
		Timestamp:     ts,
		Commit:        commit,
		GOOS:          "darwin",
		GOARCH:        "arm64",
		Name:          name,
		Iterations:    100,
		NsPerOp:       ns,
	}
}

func runOf(commit string, ts time.Time, entries ...Record) []Record {
	out := make([]Record, len(entries))
	for i, e := range entries {
		e.Commit = commit
		e.Timestamp = ts
		out[i] = e
	}
	return out
}

var (
	t1 = time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)
	t2 = time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)
	t3 = time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
)

func TestCompareThresholdBoundary(t *testing.T) {
	base := []Record{rec("BenchmarkA-8", "base", t1, 100)}
	// Exactly at the 5% threshold: not a regression.
	atThreshold := []Record{rec("BenchmarkA-8", "cur", t2, 105)}
	rep := Compare(base, atThreshold, Thresholds{NsPerOpPct: 5})
	if rep.HasRegressions() {
		t.Fatalf("change exactly equal to threshold must not regress: %+v", rep.Regressions)
	}
	if rep.Matched != 1 {
		t.Fatalf("expected 1 matched benchmark, got %d", rep.Matched)
	}

	// Just above the threshold: regression.
	above := []Record{rec("BenchmarkA-8", "cur", t2, 105.01)}
	rep = Compare(base, above, Thresholds{NsPerOpPct: 5})
	if !rep.HasRegressions() {
		t.Fatal("expected regression above threshold")
	}
	d := rep.Regressions[0].Deltas[0]
	if d.Metric != "ns/op" || d.Baseline != 100 || d.Current != 105.01 {
		t.Fatalf("unexpected delta %+v", d)
	}

	// Improvement: never a regression.
	better := []Record{rec("BenchmarkA-8", "cur", t2, 90)}
	if Compare(base, better, Thresholds{NsPerOpPct: 5}).HasRegressions() {
		t.Fatal("improvement must not be a regression")
	}
}

func TestCompareMemoryMetrics(t *testing.T) {
	b := rec("BenchmarkA-8", "base", t1, 100)
	b.BytesPerOp = floatPtr(100)
	b.AllocsPerOp = floatPtr(10)
	c := rec("BenchmarkA-8", "cur", t2, 100)
	c.BytesPerOp = floatPtr(120)
	c.AllocsPerOp = floatPtr(10)

	rep := Compare([]Record{b}, []Record{c}, Thresholds{NsPerOpPct: 5, BytesPerOpPct: 5, AllocsPerOpPct: 5})
	if !rep.HasRegressions() {
		t.Fatal("expected B/op regression")
	}
	if len(rep.Regressions[0].Deltas) != 1 || rep.Regressions[0].Deltas[0].Metric != "B/op" {
		t.Fatalf("expected only B/op delta, got %+v", rep.Regressions[0].Deltas)
	}

	// Memory metric absent on one side: skipped, not a regression.
	c.BytesPerOp = nil
	rep = Compare([]Record{b}, []Record{c}, Thresholds{NsPerOpPct: 5, BytesPerOpPct: 0, AllocsPerOpPct: 5})
	if rep.HasRegressions() {
		t.Fatalf("metric missing on one side must be skipped, got %+v", rep.Regressions)
	}
}

func TestCompareOnlyOnOneSide(t *testing.T) {
	base := []Record{rec("BenchmarkOld-8", "base", t1, 100)}
	cur := []Record{rec("BenchmarkNew-8", "cur", t2, 100)}
	rep := Compare(base, cur, Thresholds{NsPerOpPct: 5})
	if rep.HasRegressions() {
		t.Fatal("benchmarks on only one side must not regress")
	}
	if rep.Matched != 0 {
		t.Fatalf("expected 0 matched, got %d", rep.Matched)
	}
	if len(rep.OnlyInBaseline) != 1 || rep.OnlyInBaseline[0] != "BenchmarkOld-8" {
		t.Fatalf("unexpected OnlyInBaseline %+v", rep.OnlyInBaseline)
	}
	if len(rep.OnlyInCurrent) != 1 || rep.OnlyInCurrent[0] != "BenchmarkNew-8" {
		t.Fatalf("unexpected OnlyInCurrent %+v", rep.OnlyInCurrent)
	}
	if rep.BaselineCommit != "base" || rep.CurrentCommit != "cur" {
		t.Fatalf("unexpected commits in report %+v", rep)
	}
}

func TestResolveBaselineDefaultPicksDifferentCommit(t *testing.T) {
	history := concat(
		runOf("aaa", t1, rec("BenchmarkA-8", "", time.Time{}, 100)),
		runOf("bbb", t2, rec("BenchmarkA-8", "", time.Time{}, 110)),
		runOf("bbb", t3, rec("BenchmarkA-8", "", time.Time{}, 120)),
	)
	base, err := ResolveBaseline(history, "")
	if err != nil {
		t.Fatalf("ResolveBaseline: %v", err)
	}
	if base[0].Commit != "aaa" {
		t.Fatalf("expected newest different-commit run aaa, got %q", base[0].Commit)
	}
	if base[0].NsPerOp != 100 {
		t.Fatalf("expected baseline ns/op 100, got %v", base[0].NsPerOp)
	}
}

func TestResolveBaselineNoDifferentCommit(t *testing.T) {
	history := concat(
		runOf("aaa", t1, rec("BenchmarkA-8", "", time.Time{}, 100)),
		runOf("aaa", t2, rec("BenchmarkA-8", "", time.Time{}, 110)),
	)
	if _, err := ResolveBaseline(history, ""); err == nil {
		t.Fatal("expected error when no run has a different commit")
	}
}

func TestResolveBaselineByCommitPrefix(t *testing.T) {
	history := concat(
		runOf("aaaaaaa1", t1, rec("BenchmarkA-8", "", time.Time{}, 100)),
		runOf("bbbbbbb2", t2, rec("BenchmarkA-8", "", time.Time{}, 110)),
	)
	base, err := ResolveBaseline(history, "aaaa")
	if err != nil {
		t.Fatalf("ResolveBaseline: %v", err)
	}
	if base[0].Commit != "aaaaaaa1" {
		t.Fatalf("expected commit aaaaaaa1, got %q", base[0].Commit)
	}
	if _, err := ResolveBaseline(history, "cccc"); err == nil {
		t.Fatal("expected error for unknown commit")
	}
}

func TestResolveBaselineByLineNumber(t *testing.T) {
	history := concat(
		runOf("aaa", t1, rec("BenchmarkA-8", "", time.Time{}, 100), rec("BenchmarkB-8", "", time.Time{}, 200)),
		runOf("bbb", t2, rec("BenchmarkA-8", "", time.Time{}, 110)),
	)
	// Line 2 belongs to the first run (commit aaa), so the whole run is
	// selected as baseline.
	base, err := ResolveBaseline(history, "2")
	if err != nil {
		t.Fatalf("ResolveBaseline: %v", err)
	}
	if len(base) != 2 || base[0].Commit != "aaa" {
		t.Fatalf("expected run aaa with 2 records, got %+v", base)
	}
	if _, err := ResolveBaseline(history, "99"); err == nil {
		t.Fatal("expected error for out-of-range line number")
	}
}

func TestLatestRunEmpty(t *testing.T) {
	if _, err := LatestRun(nil); err == nil {
		t.Fatal("expected error for empty history")
	}
}

func TestSummarize(t *testing.T) {
	history := concat(
		runOf("aaa", t1, rec("BenchmarkA-8", "", time.Time{}, 100), rec("BenchmarkB-8", "", time.Time{}, 50)),
		runOf("bbb", t2, rec("BenchmarkA-8", "", time.Time{}, 80)),
		runOf("ccc", t3, rec("BenchmarkA-8", "", time.Time{}, 120)),
	)
	trends := Summarize(history)
	if len(trends) != 2 {
		t.Fatalf("expected 2 trends, got %d", len(trends))
	}
	a := trends[0]
	if a.Name != "BenchmarkA-8" || a.Runs != 3 {
		t.Fatalf("unexpected trend %+v", a)
	}
	if a.FirstNsPerOp != 100 || a.LatestNsPerOp != 120 || a.MinNsPerOp != 80 || a.MaxNsPerOp != 120 {
		t.Fatalf("unexpected trend values %+v", a)
	}
	if a.ChangePct != 20 {
		t.Fatalf("expected +20%% change, got %v", a.ChangePct)
	}
	b := trends[1]
	if b.Name != "BenchmarkB-8" || b.Runs != 1 || b.ChangePct != 0 {
		t.Fatalf("unexpected trend %+v", b)
	}
}

func concat(runs ...[]Record) []Record {
	var out []Record
	for _, r := range runs {
		out = append(out, r...)
	}
	return out
}
