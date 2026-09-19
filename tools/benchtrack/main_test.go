package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const baselineBench = `goos: darwin
goarch: arm64
pkg: gocolumn/kernel
BenchmarkAdd-8    	1000	    1000 ns/op	     512 B/op	       3 allocs/op
BenchmarkMul-8    	1000	    2000 ns/op	    1024 B/op	       4 allocs/op
PASS
ok  	gocolumn/kernel	1.234s
`

const regressedBench = `goos: darwin
goarch: arm64
pkg: gocolumn/kernel
BenchmarkAdd-8    	1000	    1200 ns/op	     512 B/op	       3 allocs/op
BenchmarkMul-8    	1000	    2000 ns/op	    1024 B/op	       4 allocs/op
PASS
ok  	gocolumn/kernel	1.456s
`

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestEndToEndRecordCompareReport(t *testing.T) {
	dir := t.TempDir()
	history := filepath.Join(dir, "history.jsonl")
	good := writeFile(t, dir, "good.txt", baselineBench)
	bad := writeFile(t, dir, "bad.txt", regressedBench)
	fakeCommit := func() string { return "00000000fallback" }

	var out, errOut bytes.Buffer

	// Record the baseline run.
	code := run([]string{"record", "-input", good, "-history", history, "-commit", "aaaaaaa1"}, &out, &errOut, fakeCommit)
	if code != 0 {
		t.Fatalf("record baseline: exit %d, stderr %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "recorded 2 benchmark(s)") {
		t.Fatalf("unexpected record output %q", out.String())
	}

	// Record the regressed run.
	out.Reset()
	code = run([]string{"record", "-input", bad, "-history", history, "-commit", "bbbbbbb2"}, &out, &errOut, fakeCommit)
	if code != 0 {
		t.Fatalf("record current: exit %d, stderr %s", code, errOut.String())
	}

	// Default compare (newest different commit, 5% threshold): BenchmarkAdd
	// regressed +20% -> exit 1.
	out.Reset()
	code = run([]string{"compare", "-history", history}, &out, &errOut, fakeCommit)
	if code != 1 {
		t.Fatalf("expected exit 1 for regression, got %d (stderr %s)", code, errOut.String())
	}
	if !strings.Contains(out.String(), "REGRESSION BenchmarkAdd-8: ns/op 1000.00 -> 1200.00 (+20.0%, threshold 5.0%)") {
		t.Fatalf("unexpected compare output %q", out.String())
	}
	if strings.Contains(out.String(), "BenchmarkMul-8") {
		t.Fatalf("unchanged benchmark must not appear in report %q", out.String())
	}

	// A threshold above the regression passes with exit 0.
	out.Reset()
	code = run([]string{"compare", "-history", history, "-threshold", "25"}, &out, &errOut, fakeCommit)
	if code != 0 {
		t.Fatalf("expected exit 0 with 25%% threshold, got %d", code)
	}
	if !strings.Contains(out.String(), "no regressions") {
		t.Fatalf("unexpected compare output %q", out.String())
	}

	// Explicit baseline by commit prefix.
	out.Reset()
	code = run([]string{"compare", "-history", history, "-baseline", "aaaa", "-threshold", "25"}, &out, &errOut, fakeCommit)
	if code != 0 {
		t.Fatalf("compare with commit prefix: exit %d, stderr %s", code, errOut.String())
	}

	// Baseline by history line number (line 1 is in the baseline run).
	out.Reset()
	code = run([]string{"compare", "-history", history, "-baseline", "1", "-threshold", "25"}, &out, &errOut, fakeCommit)
	if code != 0 {
		t.Fatalf("compare with line number: exit %d, stderr %s", code, errOut.String())
	}

	// Report prints trends for both benchmarks.
	out.Reset()
	code = run([]string{"report", "-history", history}, &out, &errOut, fakeCommit)
	if code != 0 {
		t.Fatalf("report: exit %d, stderr %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "BenchmarkAdd-8") || !strings.Contains(out.String(), "BenchmarkMul-8") {
		t.Fatalf("unexpected report output %q", out.String())
	}
	if !strings.Contains(out.String(), "+20.0%") {
		t.Fatalf("report should show +20%% change for BenchmarkAdd-8, got %q", out.String())
	}
}

func TestRecordCSVFormat(t *testing.T) {
	dir := t.TempDir()
	history := filepath.Join(dir, "history.jsonl")
	csvFile := writeFile(t, dir, "bench.csv", "name,ns/op,B/op,allocs/op\nBenchmarkCSV-8,42.5,128,2\n")

	var out, errOut bytes.Buffer
	code := run([]string{"record", "-input", csvFile, "-history", history, "-commit", "ccccccc3"}, &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("record csv: exit %d, stderr %s", code, errOut.String())
	}
	data, err := os.ReadFile(history)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	if !strings.Contains(string(data), `"name":"BenchmarkCSV-8"`) || !strings.Contains(string(data), `"ns_per_op":42.5`) {
		t.Fatalf("unexpected history content %s", data)
	}
	if !strings.Contains(string(data), `"commit":"ccccccc3"`) {
		t.Fatalf("history should carry the commit %s", data)
	}
}

func TestRecordEmptyInput(t *testing.T) {
	dir := t.TempDir()
	empty := writeFile(t, dir, "empty.txt", "PASS\n")
	var out, errOut bytes.Buffer
	code := run([]string{"record", "-input", empty, "-history", filepath.Join(dir, "h.jsonl")}, &out, &errOut, nil)
	if code != 2 {
		t.Fatalf("expected exit 2 for empty input, got %d", code)
	}
}

func TestUnknownSubcommand(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"frobnicate"}, &out, &errOut, nil); code != 2 {
		t.Fatalf("expected exit 2 for unknown subcommand, got %d", code)
	}
	if code := run(nil, &out, &errOut, nil); code != 2 {
		t.Fatalf("expected exit 2 for missing subcommand, got %d", code)
	}
}

func TestCompareMissingHistory(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"compare", "-history", filepath.Join(t.TempDir(), "nope.jsonl")}, &out, &errOut, nil)
	if code != 2 {
		t.Fatalf("expected exit 2 for missing history, got %d", code)
	}
}
