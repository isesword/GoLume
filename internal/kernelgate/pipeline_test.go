package kernelgate

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gocolumn/internal/benchtrack"
)

// fakeRunner serves canned command output to the pipeline.
type fakeRunner struct {
	benchRaw  []byte
	benchErr  error
	csv       []byte
	liveIDs   []string
	genIDs    []string
	patterns  []string
	benchRuns int
}

func (f *fakeRunner) RunBenchmark(_ context.Context, pattern string, _ int, _ string) ([]byte, error) {
	f.benchRuns++
	f.patterns = append(f.patterns, pattern)
	return f.benchRaw, f.benchErr
}

func (f *fakeRunner) BenchstatCSV(_ context.Context, _ []byte) ([]byte, error) {
	return f.csv, nil
}

func (f *fakeRunner) LiveKernelIDs(_ context.Context) ([]string, error) {
	return f.liveIDs, nil
}

func (f *fakeRunner) GeneratedKernelIDs(_ context.Context) ([]string, error) {
	return f.genIDs, nil
}

// fixtureRoot builds a repository-shaped temp directory with a gates
// file, a registry, and the evidence files they reference.
func fixtureRoot(t *testing.T) (string, *fakeRunner) {
	t.Helper()
	root := t.TempDir()
	validation := filepath.Join(root, ValidationDir)
	if err := os.MkdirAll(validation, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(validation, "simd-auto-gates.json"), []byte(testGatesJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	// Evidence files referenced by the fixture registry and gates file.
	for _, name := range []string{
		"simd-kernelid-candidates-refactor-arm64.txt",
		"simd-kernelid-candidates-refactor-arm64.csv",
		"simd-expression-e2e-refactor-arm64.txt",
	} {
		if err := os.WriteFile(filepath.Join(validation, name), []byte("fixture"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	reg := &Registry{Schema: RegistrySchema, Generated: "2026-09-19"}
	reg.Kernels = []*Kernel{
		{
			ID: "compare-int32-gt", Family: "comparison", Type: "int32", Operation: "gt",
			Source: SourceGenerated,
			Arches: map[string]ArchState{
				"darwin/arm64": {State: StateApproved, Evidence: []string{"simd-kernelid-candidates-refactor-arm64.txt", "simd-kernelid-candidates-refactor-arm64.csv"}},
			},
		},
		{
			ID: "hash64-batch", Family: "hash", Type: "uint64", Operation: "batch",
			Source: SourceHandwritten,
			Arches: map[string]ArchState{
				"darwin/arm64": {State: StateApproved, Evidence: []string{"simd-kernelid-candidates-refactor-arm64.txt", "simd-kernelid-candidates-refactor-arm64.csv"}},
			},
		},
		{
			ID: "string-match-equal", Family: "string-match", Type: "string", Operation: "equal",
			Source: SourceHandwritten, Arches: map[string]ArchState{},
		},
	}
	if err := reg.Save(filepath.Join(validation, "kernel-registry.json")); err != nil {
		t.Fatal(err)
	}
	runner := &fakeRunner{
		csv:     []byte("benchstat csv\n"),
		liveIDs: []string{"compare-int32-gt", "hash64-batch", "string-match-equal"},
		genIDs:  []string{"compare-int32-gt"},
	}
	return root, runner
}

func testPipeline(root string, runner Runner) *Pipeline {
	return &Pipeline{
		Root:   root,
		Runner: runner,
		Now:    func() time.Time { return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC) },
		Commit: func() string { return "deadbeef" },
	}
}

func TestGateApprove(t *testing.T) {
	root, runner := fixtureRoot(t)
	runner.benchRaw = rawBenchOutput("string-match-equal", spread(90000, 10), spread(20000, 10))
	p := testPipeline(root, runner)
	res, err := p.Gate(context.Background(), "string-match-equal", GateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.NewState != StateApproved || res.PrevState != StateCandidate {
		t.Fatalf("states %s -> %s, want candidate -> approved", res.PrevState, res.NewState)
	}
	if runner.patterns[0] != DefaultBenchPattern("string-match-equal") {
		t.Errorf("pattern %q", runner.patterns[0])
	}
	// Evidence files exist.
	for _, f := range []string{res.SamplesFile, res.CSVFile} {
		if _, err := os.Stat(filepath.Join(root, f)); err != nil {
			t.Errorf("evidence %s: %v", f, err)
		}
	}
	wantSamples := "validation/simd-gate-string-match-equal-" + runtime.GOARCH + ".txt"
	if res.SamplesFile != wantSamples {
		t.Errorf("samples file %s, want %s", res.SamplesFile, wantSamples)
	}
	// Registry updated.
	reg, err := LoadRegistry(filepath.Join(root, RegistryPath))
	if err != nil {
		t.Fatal(err)
	}
	entry := reg.Find("string-match-equal")
	state := entry.Arches[LocalArch()]
	if state.State != StateApproved || len(state.Evidence) != 2 || state.Updated != "2026-09-19" {
		t.Errorf("registry arch state = %+v", state)
	}
	// Gates file updated and re-readable.
	gates, err := LoadGates(filepath.Join(root, GatesPath))
	if err != nil {
		t.Fatal(err)
	}
	ok, err := gates.ApprovedArchKernel(LocalArch(), "string-match-equal")
	if err != nil || !ok {
		t.Errorf("gates approved = %t, %v", ok, err)
	}
	// History recorded.
	records, err := benchtrack.LoadFile(filepath.Join(root, HistoryPath))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 20 {
		t.Fatalf("history records = %d, want 20", len(records))
	}
	if records[0].Commit != "deadbeef" || records[0].GOOS+" "+records[0].GOARCH == " " {
		t.Errorf("record metadata = %+v", records[0])
	}
}

func TestGateReject(t *testing.T) {
	root, runner := fixtureRoot(t)
	runner.benchRaw = rawBenchOutput("string-match-equal", spread(90000, 10), spread(89000, 10))
	p := testPipeline(root, runner)
	res, err := p.Gate(context.Background(), "string-match-equal", GateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.NewState != StateRejected {
		t.Fatalf("state %s, want rejected", res.NewState)
	}
	// Registry marks the rejection with a reason and keeps evidence.
	reg, err := LoadRegistry(filepath.Join(root, RegistryPath))
	if err != nil {
		t.Fatal(err)
	}
	state := reg.Find("string-match-equal").Arches[LocalArch()]
	if state.State != StateRejected || state.Reason == "" || len(state.Evidence) == 0 {
		t.Errorf("registry arch state = %+v", state)
	}
	// Rejection must not touch the gates approved list.
	gates, err := LoadGates(filepath.Join(root, GatesPath))
	if err != nil {
		t.Fatal(err)
	}
	ok, err := gates.ApprovedArchKernel(LocalArch(), "string-match-equal")
	if err != nil || ok {
		t.Errorf("gates approved = %t, %v", ok, err)
	}
	// Evidence is still archived for the rejection.
	if _, err := os.Stat(filepath.Join(root, res.SamplesFile)); err != nil {
		t.Errorf("rejection evidence missing: %v", err)
	}
}

func TestGateErrors(t *testing.T) {
	root, runner := fixtureRoot(t)
	var raw strings.Builder
	raw.Write(rawBenchOutput("string-match-equal", spread(90000, 10), spread(20000, 10)))
	raw.Write(rawBenchOutput("compare-int32-gt", spread(70000, 10), spread(20000, 10)))
	runner.benchRaw = []byte(raw.String())
	p := testPipeline(root, runner)
	if _, err := p.Gate(context.Background(), "no-such-kernel", GateOptions{}); err == nil {
		t.Error("unknown kernel: expected error")
	}
	// Approved kernels refuse re-gating without -force.
	if _, err := p.Gate(context.Background(), "compare-int32-gt", GateOptions{}); err == nil ||
		!strings.Contains(err.Error(), "-force") {
		t.Errorf("re-gate without force: %v", err)
	}
	if _, err := p.Gate(context.Background(), "compare-int32-gt", GateOptions{Force: true}); err != nil {
		t.Errorf("re-gate with force: %v", err)
	}
}

func TestInit(t *testing.T) {
	root, runner := fixtureRoot(t)
	if err := os.Remove(filepath.Join(root, RegistryPath)); err != nil {
		t.Fatal(err)
	}
	p := testPipeline(root, runner)
	if _, err := p.Init(context.Background(), InitOptions{}); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(filepath.Join(root, RegistryPath))
	if err != nil {
		t.Fatal(err)
	}
	if len(reg.Kernels) != 3 {
		t.Fatalf("kernels = %d, want 3", len(reg.Kernels))
	}
	if reg.Find("compare-int32-gt").Source != SourceGenerated {
		t.Error("generated kernel not marked generated")
	}
	if reg.Find("string-match-equal").Source != SourceHandwritten {
		t.Error("handwritten kernel not marked handwritten")
	}
	if reg.Find("compare-int32-gt").StateOn("darwin/arm64") != StateApproved {
		t.Error("gates approval not carried into init registry")
	}
	if reg.Find("string-match-equal").StateOn("darwin/arm64") != StateCandidate {
		t.Error("ungated kernel should start as candidate")
	}
	// Refuse to overwrite without -force.
	if _, err := p.Init(context.Background(), InitOptions{}); err == nil {
		t.Error("init over existing registry: expected error")
	}
	if _, err := p.Init(context.Background(), InitOptions{Force: true}); err != nil {
		t.Errorf("init with force: %v", err)
	}
}

func TestCheckConsistent(t *testing.T) {
	root, runner := fixtureRoot(t)
	p := testPipeline(root, runner)
	problems, err := p.Check(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 0 {
		t.Errorf("problems = %v", problems)
	}
}

func TestCheckDetectsDrift(t *testing.T) {
	root, runner := fixtureRoot(t)
	p := testPipeline(root, runner)
	cases := []struct {
		name    string
		breakIt func()
		want    string
	}{
		{"kernel added in code", func() {
			runner.liveIDs = append(runner.liveIDs, "compact-float64")
		}, "missing from the registry"},
		{"kernel removed from code", func() {
			runner.liveIDs = runner.liveIDs[:2]
		}, "does not exist in code"},
		{"source drift", func() {
			runner.genIDs = nil
		}, "disagrees with gensimd"},
		{"evidence deleted", func() {
			_ = os.Remove(filepath.Join(root, ValidationDir, "simd-kernelid-candidates-refactor-arm64.txt"))
		}, "evidence file"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			subRoot, subRunner := fixtureRoot(t)
			runner = subRunner
			root = subRoot
			*p = *testPipeline(subRoot, subRunner)
			tc.breakIt()
			problems, err := p.Check(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(problems, "\n")
			if !strings.Contains(joined, tc.want) {
				t.Errorf("problems = %v, want substring %q", problems, tc.want)
			}
		})
	}
}

func TestStatus(t *testing.T) {
	root, _ := fixtureRoot(t)
	reg, err := LoadRegistry(filepath.Join(root, RegistryPath))
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	if err := Status(&out, reg); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{"KERNEL ID", "compare-int32-gt", "darwin/arm64:approved", "candidate (no arch entries)"} {
		if !strings.Contains(text, want) {
			t.Errorf("status output missing %q:\n%s", want, text)
		}
	}
}
