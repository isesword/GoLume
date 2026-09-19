package benchtrack

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ParsedBenchmark is a benchmark measurement extracted from tool output,
// before run metadata (timestamp, commit, platform) is attached.
type ParsedBenchmark struct {
	Name        string
	Iterations  int64
	NsPerOp     float64
	BytesPerOp  *float64
	AllocsPerOp *float64
}

// NewRecords converts parsed benchmarks into history records sharing one
// run identity. ts is normalized to UTC; empty commit, goos, or goarch
// values are stored as-is (an empty commit means detection failed).
func NewRecords(parsed []ParsedBenchmark, ts time.Time, commit, goos, goarch string) []Record {
	records := make([]Record, 0, len(parsed))
	for _, p := range parsed {
		records = append(records, Record{
			SchemaVersion: SchemaVersion,
			Timestamp:     ts.UTC(),
			Commit:        commit,
			GOOS:          goos,
			GOARCH:        goarch,
			Name:          p.Name,
			Iterations:    p.Iterations,
			NsPerOp:       p.NsPerOp,
			BytesPerOp:    p.BytesPerOp,
			AllocsPerOp:   p.AllocsPerOp,
		})
	}
	return records
}

// benchmarkLine matches result lines such as
// "BenchmarkFoo/bar-8   100   12345 ns/op   678 B/op   9 allocs/op".
// The name group is greedy so the GOMAXPROCS suffix is the last
// dash-number before the iteration count.
var benchmarkLine = regexp.MustCompile(`^(Benchmark.+)-([0-9]+)[ \t]+([0-9]+)[ \t]+(.+)$`)

// IsBenchmarkLine reports whether line looks like a "go test -bench"
// result line (used by the CLI for format auto-detection).
func IsBenchmarkLine(line string) bool {
	return benchmarkLine.MatchString(strings.TrimSpace(line))
}

// ParseGoTestOutput parses "go test -bench" text output. Non-result
// lines (goos:, pkg:, PASS, ok, ...) are ignored. A line that starts a
// benchmark result but is malformed is an error citing its line number.
// Metric units other than ns/op, B/op, and allocs/op (for example MB/s)
// are parsed but discarded.
func ParseGoTestOutput(r io.Reader) ([]ParsedBenchmark, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var out []ParsedBenchmark
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		m := benchmarkLine.FindStringSubmatch(line)
		if m == nil {
			return nil, fmt.Errorf("benchtrack: line %d: malformed benchmark line %q", lineNo, line)
		}
		iter, err := strconv.ParseInt(m[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("benchtrack: line %d: bad iteration count: %w", lineNo, err)
		}
		pb := ParsedBenchmark{Name: m[1] + "-" + m[2], Iterations: iter}
		fields := strings.Fields(m[4])
		if len(fields) == 0 || len(fields)%2 != 0 {
			return nil, fmt.Errorf("benchtrack: line %d: malformed metric fields %q", lineNo, m[4])
		}
		for i := 0; i < len(fields); i += 2 {
			value, err := strconv.ParseFloat(fields[i], 64)
			if err != nil {
				return nil, fmt.Errorf("benchtrack: line %d: bad metric value %q: %w", lineNo, fields[i], err)
			}
			switch fields[i+1] {
			case "ns/op":
				pb.NsPerOp = value
			case "B/op":
				v := value
				pb.BytesPerOp = &v
			case "allocs/op":
				v := value
				pb.AllocsPerOp = &v
			}
		}
		out = append(out, pb)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("benchtrack: read benchmark output: %w", err)
	}
	return out, nil
}

// ParseBenchstatCSV parses CSV with a header row. The header must
// contain a benchmark name column ("name" or "benchmark") and an
// "ns/op" column; optional columns are "iterations"/"n", "B/op" or
// "bytes/op", and "allocs/op". Column matching is case-insensitive.
func ParseBenchstatCSV(r io.Reader) ([]ParsedBenchmark, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("benchtrack: read CSV header: %w", err)
	}
	col := func(names ...string) int {
		for i, h := range header {
			h = strings.ToLower(strings.TrimSpace(h))
			for _, want := range names {
				if h == want {
					return i
				}
			}
		}
		return -1
	}
	nameCol := col("name", "benchmark")
	nsCol := col("ns/op", "ns_per_op")
	iterCol := col("iterations", "n")
	bytesCol := col("b/op", "bytes/op", "bytes_per_op")
	allocsCol := col("allocs/op", "allocs_per_op")
	if nameCol < 0 || nsCol < 0 {
		return nil, fmt.Errorf("benchtrack: CSV header %v must contain a name and an ns/op column", header)
	}

	var out []ParsedBenchmark
	lineNo := 1
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		lineNo++
		if err != nil {
			return nil, fmt.Errorf("benchtrack: CSV line %d: %w", lineNo, err)
		}
		if strings.TrimSpace(row[nameCol]) == "" {
			continue
		}
		pb := ParsedBenchmark{Name: strings.TrimSpace(row[nameCol])}
		if pb.NsPerOp, err = parseCSVFloat(row, nsCol, lineNo); err != nil {
			return nil, err
		}
		if iterCol >= 0 && strings.TrimSpace(row[iterCol]) != "" {
			v, err := strconv.ParseInt(strings.TrimSpace(row[iterCol]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("benchtrack: CSV line %d: bad iterations %q: %w", lineNo, row[iterCol], err)
			}
			pb.Iterations = v
		}
		if bytesCol >= 0 && strings.TrimSpace(row[bytesCol]) != "" {
			v, err := parseCSVFloat(row, bytesCol, lineNo)
			if err != nil {
				return nil, err
			}
			pb.BytesPerOp = &v
		}
		if allocsCol >= 0 && strings.TrimSpace(row[allocsCol]) != "" {
			v, err := parseCSVFloat(row, allocsCol, lineNo)
			if err != nil {
				return nil, err
			}
			pb.AllocsPerOp = &v
		}
		out = append(out, pb)
	}
	return out, nil
}

func parseCSVFloat(row []string, col, lineNo int) (float64, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(row[col]), 64)
	if err != nil {
		return 0, fmt.Errorf("benchtrack: CSV line %d: bad number %q: %w", lineNo, row[col], err)
	}
	return v, nil
}
