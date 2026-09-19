package kernelgate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"gocolumn/internal/benchtrack"
)

// Default paths, relative to the repository root.
const (
	// RegistryPath is the kernel registry file.
	RegistryPath = "validation/kernel-registry.json"
	// GatesPath is the SIMD auto-gate approvals file.
	GatesPath = "validation/simd-auto-gates.json"
	// HistoryPath is the append-only benchmark history ledger.
	HistoryPath = "validation/bench-history.jsonl"
	// ValidationDir holds all archived evidence files.
	ValidationDir = "validation"
)

// DefaultBenchPattern renders the benchmark pattern that isolates one
// kernel's scalar and SIMD sub-benchmarks in BenchmarkKernelID.
func DefaultBenchPattern(kernelID string) string {
	return "^BenchmarkKernelID/" + kernelID + "/"
}

// Pipeline wires the registry, gates file, and benchmark runner
// together. The zero-value clock and commit detector are replaced in
// tests.
type Pipeline struct {
	// Root is the repository root; all default paths resolve under it.
	Root string
	// Runner executes benchmarks and listing commands.
	Runner Runner
	// Now supplies timestamps for history records and registry updates.
	Now func() time.Time
	// Commit supplies the git commit recorded into history records.
	Commit func() string
}

func (p *Pipeline) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Pipeline) commit(ctx context.Context) string {
	if p.Commit != nil {
		return p.Commit()
	}
	return DetectCommit(ctx, p.Root)
}

func (p *Pipeline) registryPath() string { return filepath.Join(p.Root, RegistryPath) }
func (p *Pipeline) gatesPath() string    { return filepath.Join(p.Root, GatesPath) }
func (p *Pipeline) historyPath() string  { return filepath.Join(p.Root, HistoryPath) }

// loadPair loads registry and gates file together.
func (p *Pipeline) loadPair() (*Registry, *Gates, error) {
	reg, err := LoadRegistry(p.registryPath())
	if err != nil {
		return nil, nil, err
	}
	gates, err := LoadGates(p.gatesPath())
	if err != nil {
		return nil, nil, err
	}
	return reg, gates, nil
}

// GateOptions tunes one gate run.
type GateOptions struct {
	// BenchPattern overrides the benchmark pattern; empty uses
	// DefaultBenchPattern(kernelID).
	BenchPattern string
	// Force permits re-gating kernels that are not candidates.
	Force bool
}

// GateResult reports the outcome of gating one kernel.
type GateResult struct {
	KernelID  string
	Arch      string
	Decision  Decision
	PrevState State
	NewState  State
	// SamplesFile and CSVFile are the archived evidence paths, relative
	// to the repository root.
	SamplesFile string
	CSVFile     string
}

// Gate runs the full approval pipeline for one kernel: benchmark,
// benchstat decision, evidence archival, gates-file and registry
// update, and bench-history recording. The kernel must be present in
// the registry; its current state must be candidate unless
// opts.Force is set.
func (p *Pipeline) Gate(ctx context.Context, kernelID string, opts GateOptions) (GateResult, error) {
	res := GateResult{KernelID: kernelID, Arch: LocalArch()}
	reg, gates, err := p.loadPair()
	if err != nil {
		return res, err
	}
	entry := reg.Find(kernelID)
	if entry == nil {
		return res, fmt.Errorf("kernelgate: kernel %q is not in the registry", kernelID)
	}
	res.PrevState = entry.StateOn(res.Arch)
	if res.PrevState != StateCandidate && !opts.Force {
		return res, fmt.Errorf("kernelgate: kernel %q is %s on %s; use -force to re-gate", kernelID, res.PrevState, res.Arch)
	}
	pattern := opts.BenchPattern
	if pattern == "" {
		pattern = DefaultBenchPattern(kernelID)
	}
	raw, err := p.Runner.RunBenchmark(ctx, pattern, gates.Criteria.MinimumSamples, gates.Criteria.Benchtime)
	if err != nil {
		return res, err
	}
	res.Decision, err = Judge(raw, kernelID, gates.Criteria)
	if err != nil {
		return res, err
	}
	csv, err := p.Runner.BenchstatCSV(ctx, raw)
	if err != nil {
		return res, err
	}
	// Archive raw evidence before mutating any state file, so a crash
	// never leaves an approval without its evidence on disk.
	base := fmt.Sprintf("simd-gate-%s-%s", kernelID, runtime.GOARCH)
	samplesName, csvName := base+".txt", base+".csv"
	if err := os.WriteFile(filepath.Join(p.Root, ValidationDir, samplesName), raw, 0o644); err != nil {
		return res, fmt.Errorf("kernelgate: archive samples: %w", err)
	}
	if err := os.WriteFile(filepath.Join(p.Root, ValidationDir, csvName), csv, 0o644); err != nil {
		return res, fmt.Errorf("kernelgate: archive benchstat csv: %w", err)
	}
	res.SamplesFile = ValidationDir + "/" + samplesName
	res.CSVFile = ValidationDir + "/" + csvName

	if err := p.recordHistory(ctx, raw); err != nil {
		return res, err
	}

	res.NewState = StateRejected
	if res.Decision.Pass {
		res.NewState = StateApproved
		approved, err := gates.Approved(res.Arch)
		if err != nil {
			return res, err
		}
		if err := gates.SetApproved(res.Arch, append(approved, kernelID)); err != nil {
			return res, err
		}
		if err := gates.Save(p.gatesPath()); err != nil {
			return res, err
		}
	}
	if err := Transition(res.PrevState, res.NewState); err != nil {
		return res, err
	}
	if entry.Arches == nil {
		entry.Arches = map[string]ArchState{}
	}
	archState := ArchState{
		State:    res.NewState,
		Evidence: []string{samplesName, csvName},
		Updated:  p.now().UTC().Format("2006-01-02"),
	}
	if !res.Decision.Pass {
		archState.Reason = res.Decision.Note
	}
	entry.Arches[res.Arch] = archState
	if err := reg.Save(p.registryPath()); err != nil {
		return res, err
	}
	return res, nil
}

// recordHistory parses the raw benchmark output and appends every
// measurement to the bench-history ledger.
func (p *Pipeline) recordHistory(ctx context.Context, raw []byte) error {
	parsed, err := benchtrack.ParseGoTestOutput(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("kernelgate: parse benchmark output for history: %w", err)
	}
	records := benchtrack.NewRecords(parsed, p.now(), p.commit(ctx), runtime.GOOS, runtime.GOARCH)
	if err := benchtrack.AppendFile(p.historyPath(), records); err != nil {
		return err
	}
	return nil
}

// InitOptions tunes registry bootstrapping.
type InitOptions struct {
	// Force permits overwriting an existing registry file.
	Force bool
}

// Init builds a fresh registry from three live sources: the kernel IDs
// that exist in the kernel package, the gensimd generated-kernel list
// (for the source field), and the gates file (approved IDs and their
// evidence files, plus previously deleted candidates, become approved
// and rejected entries). Every other kernel starts as a candidate.
func (p *Pipeline) Init(ctx context.Context, opts InitOptions) (*Registry, error) {
	path := p.registryPath()
	if _, err := os.Stat(path); err == nil && !opts.Force {
		return nil, fmt.Errorf("kernelgate: registry %s already exists; use -force to rebuild", path)
	}
	live, err := p.Runner.LiveKernelIDs(ctx)
	if err != nil {
		return nil, err
	}
	generated, err := p.Runner.GeneratedKernelIDs(ctx)
	if err != nil {
		return nil, err
	}
	gates, err := LoadGates(p.gatesPath())
	if err != nil {
		return nil, err
	}
	genSet := make(map[string]bool, len(generated))
	for _, id := range generated {
		genSet[id] = true
	}
	reg := &Registry{Schema: RegistrySchema, Generated: p.now().UTC().Format("2006-01-02")}
	for _, id := range live {
		family, typ, op, err := ParseKernelID(id)
		if err != nil {
			return nil, err
		}
		entry := &Kernel{ID: id, Family: family, Type: typ, Operation: op, Source: SourceHandwritten, Arches: map[string]ArchState{}}
		if genSet[id] {
			entry.Source = SourceGenerated
		}
		reg.Kernels = append(reg.Kernels, entry)
	}
	for arch := range gates.Architectures {
		approved, err := gates.Approved(arch)
		if err != nil {
			return nil, err
		}
		samples, err := gates.ArchField(arch, "kernel_gate.samples_file")
		if err != nil {
			return nil, err
		}
		csv, err := gates.ArchField(arch, "kernel_gate.benchstat_csv")
		if err != nil {
			return nil, err
		}
		hashSamples, err := gates.ArchField(arch, "hash64_gate.samples_file")
		if err != nil {
			return nil, err
		}
		hashCSV, err := gates.ArchField(arch, "hash64_gate.benchstat_csv")
		if err != nil {
			return nil, err
		}
		for _, id := range approved {
			entry := reg.Find(id)
			if entry == nil {
				return nil, fmt.Errorf("kernelgate: gates file approves unknown kernel %q on %s", id, arch)
			}
			evidence := []string{filepath.Base(samples), filepath.Base(csv)}
			if id == "hash64-batch" && hashSamples != "" {
				evidence = []string{filepath.Base(hashSamples), filepath.Base(hashCSV)}
			}
			entry.Arches[arch] = ArchState{State: StateApproved, Evidence: evidence, Updated: reg.Generated}
		}
		rejected, err := deletedCandidateIDs(gates, arch)
		if err != nil {
			return nil, err
		}
		for _, dr := range rejected {
			entry := reg.Find(dr.id)
			if entry == nil {
				continue // deleted candidates may no longer exist in code
			}
			entry.Arches[arch] = ArchState{State: StateRejected, Reason: dr.reason, Evidence: dr.evidence, Updated: reg.Generated}
		}
	}
	if err := reg.Validate(); err != nil {
		return nil, err
	}
	if err := reg.Save(path); err != nil {
		return nil, err
	}
	return reg, nil
}

// deletedCandidate reads one entry of the gates file's
// deleted_candidates list.
type deletedCandidate struct {
	id       string
	reason   string
	evidence []string
}

// deletedCandidateIDs extracts kernel_id entries from the gates file's
// deleted_candidates list for arch. Family-level entries (no kernel_id)
// carry no registry target and are skipped.
func deletedCandidateIDs(g *Gates, arch string) ([]deletedCandidate, error) {
	raw, ok := g.Architectures[arch]
	if !ok {
		return nil, nil
	}
	var entry struct {
		Deleted []struct {
			KernelID    string `json:"kernel_id"`
			Reason      string `json:"reason"`
			SamplesFile string `json:"samples_file"`
		} `json:"deleted_candidates"`
	}
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, fmt.Errorf("kernelgate: gates arch %s deleted_candidates: %w", arch, err)
	}
	var out []deletedCandidate
	for _, d := range entry.Deleted {
		if d.KernelID == "" {
			continue
		}
		dc := deletedCandidate{id: d.KernelID, reason: d.Reason}
		if d.SamplesFile != "" {
			dc.evidence = []string{filepath.Base(d.SamplesFile)}
		}
		out = append(out, dc)
	}
	return out, nil
}

// sortedIDs returns the registry's kernel IDs in sorted order.
func sortedIDs(reg *Registry) []string {
	ids := make([]string, 0, len(reg.Kernels))
	for _, k := range reg.Kernels {
		ids = append(ids, k.ID)
	}
	sort.Strings(ids)
	return ids
}
