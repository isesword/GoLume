// Command benchtrack records Go benchmark results into a JSONL history
// file and reports performance regressions between recorded runs.
//
// Usage:
//
//	benchtrack record  [-input file|-] [-format auto|gotest|csv] [-history path] [-commit sha]
//	benchtrack compare [-history path] [-baseline commit|line] [-threshold pct] [-bytes-threshold pct] [-allocs-threshold pct]
//	benchtrack report  [-history path]
//
// compare exits with code 1 when any benchmark regresses beyond its
// threshold, and with code 2 on usage or I/O errors.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"gocolumn/internal/benchtrack"
)

const defaultHistory = "validation/bench-history.jsonl"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, detectCommit))
}

// detectCommit returns the current git HEAD commit, or "" when the
// working directory is not a git repository or git is unavailable.
func detectCommit() string {
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func run(args []string, stdout, stderr io.Writer, getCommit func() string) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: benchtrack <record|compare|report> [flags]")
		return 2
	}
	var err error
	switch args[0] {
	case "record":
		err = cmdRecord(args[1:], stdout, getCommit)
	case "compare":
		err = cmdCompare(args[1:], stdout)
	case "report":
		err = cmdReport(args[1:], stdout)
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q\n", args[0])
		return 2
	}
	if err != nil {
		var reg regressionExit
		if errors.As(err, &reg) {
			return 1
		}
		fmt.Fprintln(stderr, "benchtrack:", err)
		return 2
	}
	return 0
}

// regressionExit is returned by cmdCompare when regressions are found;
// it maps to exit code 1 rather than a usage error.
type regressionExit struct{}

// Error implements the error interface.
func (regressionExit) Error() string { return "regressions detected" }

func cmdRecord(args []string, stdout io.Writer, getCommit func() string) error {
	fs := flag.NewFlagSet("record", flag.ContinueOnError)
	input := fs.String("input", "-", "benchmark output file, or - for stdin")
	format := fs.String("format", "auto", "input format: auto, gotest, or csv")
	history := fs.String("history", defaultHistory, "JSONL history file")
	commit := fs.String("commit", "", "git commit to record; default is git rev-parse HEAD")
	if err := fs.Parse(args); err != nil {
		return err
	}

	data, err := readInput(*input)
	if err != nil {
		return err
	}
	parsed, err := parseInput(data, *format)
	if err != nil {
		return err
	}
	if len(parsed) == 0 {
		return fmt.Errorf("no benchmark results found in %s", inputName(*input))
	}

	c := *commit
	if c == "" {
		c = getCommit()
	}
	records := benchtrack.NewRecords(parsed, time.Now(), c, runtime.GOOS, runtime.GOARCH)
	if err := benchtrack.AppendFile(*history, records); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "recorded %d benchmark(s) (commit %s) -> %s\n", len(records), shortCommit(c), *history)
	return nil
}

func cmdCompare(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	history := fs.String("history", defaultHistory, "JSONL history file")
	baseline := fs.String("baseline", "", "baseline: commit (exact or prefix), 1-based history line number, or empty for the newest run with a different commit")
	threshold := fs.Float64("threshold", 5, "ns/op regression threshold in percent; strictly greater is a regression")
	bytesThreshold := fs.Float64("bytes-threshold", 5, "B/op regression threshold in percent")
	allocsThreshold := fs.Float64("allocs-threshold", 5, "allocs/op regression threshold in percent")
	if err := fs.Parse(args); err != nil {
		return err
	}

	hist, err := benchtrack.LoadFile(*history)
	if err != nil {
		return err
	}
	current, err := benchtrack.LatestRun(hist)
	if err != nil {
		return err
	}
	base, err := benchtrack.ResolveBaseline(hist, *baseline)
	if err != nil {
		return err
	}

	rep := benchtrack.Compare(base, current, benchtrack.Thresholds{
		NsPerOpPct:     *threshold,
		BytesPerOpPct:  *bytesThreshold,
		AllocsPerOpPct: *allocsThreshold,
	})
	printReport(stdout, rep)
	if rep.HasRegressions() {
		return regressionExit{}
	}
	return nil
}

func cmdReport(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	history := fs.String("history", defaultHistory, "JSONL history file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	hist, err := benchtrack.LoadFile(*history)
	if err != nil {
		return err
	}
	trends := benchtrack.Summarize(hist)
	if len(trends) == 0 {
		fmt.Fprintln(stdout, "no benchmark history")
		return nil
	}
	fmt.Fprintf(stdout, "%-40s %6s %14s %14s %14s %14s %10s\n", "BENCHMARK", "RUNS", "FIRST ns/op", "LATEST ns/op", "MIN ns/op", "MAX ns/op", "CHANGE")
	for _, t := range trends {
		fmt.Fprintf(stdout, "%-40s %6d %14.2f %14.2f %14.2f %14.2f %+9.1f%%\n",
			t.Name, t.Runs, t.FirstNsPerOp, t.LatestNsPerOp, t.MinNsPerOp, t.MaxNsPerOp, t.ChangePct)
	}
	return nil
}

func printReport(w io.Writer, rep benchtrack.Report) {
	fmt.Fprintf(w, "baseline commit %s vs current commit %s (%d benchmark(s) matched)\n",
		shortCommit(rep.BaselineCommit), shortCommit(rep.CurrentCommit), rep.Matched)
	for _, reg := range rep.Regressions {
		for _, d := range reg.Deltas {
			fmt.Fprintf(w, "REGRESSION %s: %s %.2f -> %.2f (%+.1f%%, threshold %.1f%%)\n",
				reg.Name, d.Metric, d.Baseline, d.Current, d.ChangePct, d.ThresholdPct)
		}
	}
	if len(rep.OnlyInBaseline) > 0 {
		fmt.Fprintf(w, "only in baseline: %s\n", strings.Join(rep.OnlyInBaseline, ", "))
	}
	if len(rep.OnlyInCurrent) > 0 {
		fmt.Fprintf(w, "only in current: %s\n", strings.Join(rep.OnlyInCurrent, ", "))
	}
	if !rep.HasRegressions() {
		fmt.Fprintln(w, "no regressions")
	}
}

func readInput(path string) ([]byte, error) {
	if path == "-" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		return data, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func parseInput(data []byte, format string) ([]benchtrack.ParsedBenchmark, error) {
	text := string(data)
	switch format {
	case "gotest":
		return benchtrack.ParseGoTestOutput(strings.NewReader(text))
	case "csv":
		return benchtrack.ParseBenchstatCSV(strings.NewReader(text))
	case "auto":
		for _, line := range strings.Split(text, "\n") {
			if benchtrack.IsBenchmarkLine(line) {
				return benchtrack.ParseGoTestOutput(strings.NewReader(text))
			}
		}
		return benchtrack.ParseBenchstatCSV(strings.NewReader(text))
	default:
		return nil, fmt.Errorf("unknown format %q (want auto, gotest, or csv)", format)
	}
}

func inputName(path string) string {
	if path == "-" {
		return "stdin"
	}
	return path
}

func shortCommit(c string) string {
	if c == "" {
		return "(unknown)"
	}
	if len(c) > 8 {
		return c[:8]
	}
	return c
}
