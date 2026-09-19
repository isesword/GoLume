package benchtrack

import (
	"reflect"
	"strings"
	"testing"
)

func floatPtr(v float64) *float64 { return &v }

func TestParseGoTestOutputVariants(t *testing.T) {
	input := `goos: darwin
goarch: arm64
pkg: gocolumn/kernel
cpu: Apple M3 Pro
BenchmarkAdd-8         	1000000	      1234 ns/op	     678 B/op	       9 allocs/op
BenchmarkNoMem-8       	  2000	    567890 ns/op
BenchmarkWithThroughput-8	 100	     12345 ns/op	   7.77 MB/s	     100 B/op	       1 allocs/op
BenchmarkGroup/sub_case-8	   10	12345678 ns/op	       1 B/op	       0 allocs/op
BenchmarkDigit2-8      	   500	     100.5 ns/op
PASS
ok  	gocolumn/kernel	12.345s
`
	got, err := ParseGoTestOutput(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseGoTestOutput: %v", err)
	}
	want := []ParsedBenchmark{
		{Name: "BenchmarkAdd-8", Iterations: 1000000, NsPerOp: 1234, BytesPerOp: floatPtr(678), AllocsPerOp: floatPtr(9)},
		{Name: "BenchmarkNoMem-8", Iterations: 2000, NsPerOp: 567890},
		{Name: "BenchmarkWithThroughput-8", Iterations: 100, NsPerOp: 12345, BytesPerOp: floatPtr(100), AllocsPerOp: floatPtr(1)},
		{Name: "BenchmarkGroup/sub_case-8", Iterations: 10, NsPerOp: 12345678, BytesPerOp: floatPtr(1), AllocsPerOp: floatPtr(0)},
		{Name: "BenchmarkDigit2-8", Iterations: 500, NsPerOp: 100.5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestParseGoTestOutputEmpty(t *testing.T) {
	got, err := ParseGoTestOutput(strings.NewReader("PASS\nok pkg 1s\n"))
	if err != nil {
		t.Fatalf("ParseGoTestOutput: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no benchmarks, got %+v", got)
	}
}

func TestParseGoTestOutputMalformed(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"missing metrics", "BenchmarkX-8\t100\n"},
		{"odd metric fields", "BenchmarkX-8\t100\t1234 ns/op\t678\n"},
		{"bad metric value", "BenchmarkX-8\t100\tabc ns/op\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseGoTestOutput(strings.NewReader(tc.input))
			if err == nil {
				t.Fatalf("expected error for %q", tc.input)
			}
			if !strings.Contains(err.Error(), "line 1") {
				t.Fatalf("error should cite line number, got %v", err)
			}
		})
	}
}

func TestParseBenchstatCSV(t *testing.T) {
	input := "name,iterations,ns/op,B/op,allocs/op\n" +
		"BenchmarkA-8,1000,123.4,512,3\n" +
		"BenchmarkB-8,,99.9,,\n"
	got, err := ParseBenchstatCSV(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseBenchstatCSV: %v", err)
	}
	want := []ParsedBenchmark{
		{Name: "BenchmarkA-8", Iterations: 1000, NsPerOp: 123.4, BytesPerOp: floatPtr(512), AllocsPerOp: floatPtr(3)},
		{Name: "BenchmarkB-8", NsPerOp: 99.9},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestParseBenchstatCSVHeaderVariants(t *testing.T) {
	got, err := ParseBenchstatCSV(strings.NewReader("Benchmark,NS_per_op,Bytes/Op\nBenchmarkX-8,1.5,64\n"))
	if err != nil {
		t.Fatalf("ParseBenchstatCSV: %v", err)
	}
	if len(got) != 1 || got[0].Name != "BenchmarkX-8" || got[0].NsPerOp != 1.5 {
		t.Fatalf("unexpected result %+v", got)
	}
	if got[0].BytesPerOp == nil || *got[0].BytesPerOp != 64 {
		t.Fatalf("expected B/op 64, got %+v", got[0].BytesPerOp)
	}
}

func TestParseBenchstatCSVErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"missing ns/op column", "name,seconds\nBenchmarkX,1\n"},
		{"missing name column", "ns/op\n12\n"},
		{"bad number", "name,ns/op\nBenchmarkX,oops\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseBenchstatCSV(strings.NewReader(tc.input)); err == nil {
				t.Fatalf("expected error for %q", tc.input)
			}
		})
	}
}

func TestIsBenchmarkLine(t *testing.T) {
	if !IsBenchmarkLine("BenchmarkAdd-8  \t100  1234 ns/op") {
		t.Fatal("expected benchmark line to be detected")
	}
	if IsBenchmarkLine("name,ns/op") || IsBenchmarkLine("goos: darwin") {
		t.Fatal("non-benchmark lines must not be detected")
	}
}
