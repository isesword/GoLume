package benchtrack

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Thresholds holds regression thresholds as percent increases. A metric
// is a regression only when its increase is strictly greater than the
// threshold; an increase exactly equal to the threshold is not.
type Thresholds struct {
	NsPerOpPct     float64
	BytesPerOpPct  float64
	AllocsPerOpPct float64
}

// MetricDelta describes the change of one metric between the baseline
// and the current run.
type MetricDelta struct {
	Metric       string // "ns/op", "B/op", or "allocs/op"
	Baseline     float64
	Current      float64
	ChangePct    float64
	ThresholdPct float64
	Regressed    bool
}

// Regression reports one benchmark whose metrics exceeded thresholds.
type Regression struct {
	Name   string
	Deltas []MetricDelta // only metrics that regressed
}

// Report is the structured result of comparing a current run against a
// baseline run.
type Report struct {
	BaselineCommit string
	CurrentCommit  string
	Matched        int // benchmarks present in both runs
	Regressions    []Regression
	OnlyInBaseline []string // benchmarks missing from the current run
	OnlyInCurrent  []string // benchmarks missing from the baseline run
}

// HasRegressions reports whether the report contains any regression.
func (r Report) HasRegressions() bool {
	return len(r.Regressions) > 0
}

// pctChange returns the percent increase from base to cur. A zero base
// yields +Inf when cur is positive and 0 when both are zero.
func pctChange(base, cur float64) float64 {
	if base == 0 {
		if cur > 0 {
			return math.Inf(1)
		}
		return 0
	}
	return (cur - base) / base * 100
}

func byName(records []Record) map[string]Record {
	m := make(map[string]Record, len(records))
	for _, rec := range records {
		m[rec.Name] = rec
	}
	return m
}

// Compare matches benchmarks by name between baseline and current and
// evaluates each shared metric against th. Benchmarks present on only
// one side are listed in the report but never counted as regressions.
// Memory metrics are evaluated only when both sides recorded them.
func Compare(baseline, current []Record, th Thresholds) Report {
	base := byName(baseline)
	cur := byName(current)
	rep := Report{}
	if len(baseline) > 0 {
		rep.BaselineCommit = baseline[0].Commit
	}
	if len(current) > 0 {
		rep.CurrentCommit = current[0].Commit
	}

	names := make([]string, 0, len(base)+len(cur))
	seen := make(map[string]bool)
	for name := range base {
		seen[name] = true
		names = append(names, name)
	}
	for name := range cur {
		if !seen[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	for _, name := range names {
		b, inBase := base[name]
		c, inCur := cur[name]
		switch {
		case !inBase:
			rep.OnlyInCurrent = append(rep.OnlyInCurrent, name)
		case !inCur:
			rep.OnlyInBaseline = append(rep.OnlyInBaseline, name)
		default:
			rep.Matched++
			var deltas []MetricDelta
			if d := deltaMetric("ns/op", b.NsPerOp, c.NsPerOp, th.NsPerOpPct); d.Regressed {
				deltas = append(deltas, d)
			}
			if b.BytesPerOp != nil && c.BytesPerOp != nil {
				if d := deltaMetric("B/op", *b.BytesPerOp, *c.BytesPerOp, th.BytesPerOpPct); d.Regressed {
					deltas = append(deltas, d)
				}
			}
			if b.AllocsPerOp != nil && c.AllocsPerOp != nil {
				if d := deltaMetric("allocs/op", *b.AllocsPerOp, *c.AllocsPerOp, th.AllocsPerOpPct); d.Regressed {
					deltas = append(deltas, d)
				}
			}
			if len(deltas) > 0 {
				rep.Regressions = append(rep.Regressions, Regression{Name: name, Deltas: deltas})
			}
		}
	}
	return rep
}

func deltaMetric(metric string, base, cur, thresholdPct float64) MetricDelta {
	d := MetricDelta{
		Metric:       metric,
		Baseline:     base,
		Current:      cur,
		ChangePct:    pctChange(base, cur),
		ThresholdPct: thresholdPct,
	}
	d.Regressed = d.ChangePct > thresholdPct
	return d
}

// runKey identifies one recorded run: all records appended by a single
// record invocation share a timestamp and commit.
type runKey struct {
	ts     time.Time
	commit string
}

// groupRuns groups history into runs keyed by (timestamp, commit) and
// returns the groups ordered by ascending timestamp (first-seen order
// breaks ties). Records within a run keep history order.
func groupRuns(history []Record) [][]Record {
	index := make(map[runKey]int)
	var runs [][]Record
	for _, rec := range history {
		k := runKey{ts: rec.Timestamp, commit: rec.Commit}
		i, ok := index[k]
		if !ok {
			i = len(runs)
			index[k] = i
			runs = append(runs, nil)
		}
		runs[i] = append(runs[i], rec)
	}
	sort.SliceStable(runs, func(i, j int) bool {
		return runs[i][0].Timestamp.Before(runs[j][0].Timestamp)
	})
	return runs
}

// LatestRun returns the most recent run in history. It is an error if
// history is empty.
func LatestRun(history []Record) ([]Record, error) {
	runs := groupRuns(history)
	if len(runs) == 0 {
		return nil, fmt.Errorf("benchtrack: history is empty")
	}
	return runs[len(runs)-1], nil
}

// ResolveBaseline selects the baseline run from history according to
// selector:
//
//   - "": the newest run whose commit differs from the newest run's
//     commit; an error if no such run exists.
//   - a decimal integer within range: the run containing that 1-based
//     history line number.
//   - otherwise: the newest run whose commit equals the selector, or for
//     which the selector is a unique commit prefix.
func ResolveBaseline(history []Record, selector string) ([]Record, error) {
	runs := groupRuns(history)
	if len(runs) == 0 {
		return nil, fmt.Errorf("benchtrack: history is empty")
	}

	if selector == "" {
		current := runs[len(runs)-1]
		for i := len(runs) - 2; i >= 0; i-- {
			if runs[i][0].Commit != current[0].Commit {
				return runs[i], nil
			}
		}
		return nil, fmt.Errorf("benchtrack: no baseline run with a different commit than %q; use -baseline to select one", current[0].Commit)
	}

	if n, err := strconv.Atoi(selector); err == nil && n >= 1 && n <= len(history) {
		key := runKey{ts: history[n-1].Timestamp, commit: history[n-1].Commit}
		for _, run := range runs {
			if run[0].Timestamp.Equal(key.ts) && run[0].Commit == key.commit {
				return run, nil
			}
		}
		return nil, fmt.Errorf("benchtrack: internal error: line %d does not map to a run", n)
	}

	var matches [][]Record
	for i := len(runs) - 1; i >= 0; i-- {
		commit := runs[i][0].Commit
		if commit == selector {
			return runs[i], nil
		}
		if strings.HasPrefix(commit, selector) {
			matches = append(matches, runs[i])
		}
	}
	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("benchtrack: no run with commit %q in history", selector)
	case 1:
		return matches[0], nil
	default:
		return nil, fmt.Errorf("benchtrack: commit prefix %q matches %d runs; use a longer prefix", selector, len(matches))
	}
}

// Trend summarizes the history of one benchmark.
type Trend struct {
	Name          string
	Runs          int
	FirstNsPerOp  float64
	LatestNsPerOp float64
	MinNsPerOp    float64
	MaxNsPerOp    float64
	ChangePct     float64 // latest versus first
}

// Summarize groups history by benchmark name and returns per-benchmark
// trends ordered by name. Within a benchmark, runs are ordered by
// timestamp (history order breaks ties).
func Summarize(history []Record) []Trend {
	type point struct {
		ts time.Time
		ns float64
	}
	byBench := make(map[string][]point)
	for _, rec := range history {
		byBench[rec.Name] = append(byBench[rec.Name], point{ts: rec.Timestamp, ns: rec.NsPerOp})
	}
	names := make([]string, 0, len(byBench))
	for name := range byBench {
		names = append(names, name)
	}
	sort.Strings(names)

	trends := make([]Trend, 0, len(names))
	for _, name := range names {
		points := byBench[name]
		sort.SliceStable(points, func(i, j int) bool { return points[i].ts.Before(points[j].ts) })
		t := Trend{Name: name, Runs: len(points)}
		t.FirstNsPerOp = points[0].ns
		t.LatestNsPerOp = points[len(points)-1].ns
		t.MinNsPerOp = points[0].ns
		t.MaxNsPerOp = points[0].ns
		for _, p := range points[1:] {
			t.MinNsPerOp = math.Min(t.MinNsPerOp, p.ns)
			t.MaxNsPerOp = math.Max(t.MaxNsPerOp, p.ns)
		}
		t.ChangePct = pctChange(t.FirstNsPerOp, t.LatestNsPerOp)
		trends = append(trends, t)
	}
	return trends
}
