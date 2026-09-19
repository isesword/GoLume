// Package kernelgate is the library behind the kernelgate command: it
// maintains the kernel registry (validation/kernel-registry.json), keeps
// it consistent with the SIMD auto-gate approvals
// (validation/simd-auto-gates.json) and the kernels that actually exist
// in the kernel package, and runs the one-command approval gate for a
// candidate kernel: benchmark, benchstat decision, evidence archival,
// and bench-history recording.
package kernelgate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
)

// RegistrySchema is the current schema version of the kernel registry
// file. Load rejects any other version so a format change is an explicit
// migration instead of a silent misread.
const RegistrySchema = 1

// State is the lifecycle state of one kernel on one architecture.
type State string

const (
	// StateCandidate marks a kernel with a SIMD implementation that has
	// not yet passed (or failed) the approval gate on an architecture.
	StateCandidate State = "candidate"
	// StateApproved marks a kernel whose SIMD backend passed the gate and
	// is admitted to automatic dispatch on an architecture.
	StateApproved State = "approved"
	// StateRejected marks a kernel whose SIMD backend failed the gate on
	// an architecture; the scalar backend stays in production use.
	StateRejected State = "rejected"
)

// States returns every valid lifecycle state.
func States() []State { return []State{StateCandidate, StateApproved, StateRejected} }

// Transition reports whether moving a kernel from state `from` to state
// `to` is legal, and returns an error describing the violation when it
// is not. Identity transitions are legal so re-running a gate is
// idempotent. Approved kernels can only move back to candidate (a fresh
// gate run); they can never be rejected without re-gating.
func Transition(from, to State) error {
	if !validState(from) {
		return fmt.Errorf("kernelgate: unknown source state %q", from)
	}
	if !validState(to) {
		return fmt.Errorf("kernelgate: unknown target state %q", to)
	}
	if from == to {
		return nil
	}
	switch {
	case from == StateCandidate:
		// A candidate may be approved or rejected by a gate run.
		return nil
	case from == StateApproved && to == StateCandidate:
		// Re-gating an approved kernel demotes it to candidate first.
		return nil
	case from == StateRejected && to == StateCandidate:
		// A rejected kernel may be re-entered as a candidate after the
		// implementation changes.
		return nil
	default:
		return fmt.Errorf("kernelgate: illegal state transition %s -> %s", from, to)
	}
}

func validState(s State) bool {
	switch s {
	case StateCandidate, StateApproved, StateRejected:
		return true
	}
	return false
}

// Source identifies where a kernel's scalar reference implementation
// comes from.
type Source string

const (
	// SourceGenerated marks kernels whose scalar (and possibly SIMD)
	// implementation is emitted by internal/cmd/gensimd into
	// kernel/zz_generated_*.go.
	SourceGenerated Source = "generated"
	// SourceHandwritten marks kernels implemented in hand-maintained
	// kernel package files.
	SourceHandwritten Source = "handwritten"
)

// archName matches a GOOS/GOARCH pair such as "darwin/arm64".
var archName = regexp.MustCompile(`^[a-z0-9]+/[a-z0-9_]+$`)

// knownArches is the whitelist of architectures the registry accepts.
// An architecture outside this list is almost certainly a typo; add it
// here deliberately when real benchmark evidence for it appears.
var knownArches = map[string]bool{
	"darwin/arm64":  true,
	"darwin/amd64":  true,
	"linux/arm64":   true,
	"linux/amd64":   true,
	"windows/amd64": true,
}

// ArchState records the gate state of one kernel on one GOOS/GOARCH
// pair. Evidence holds validation/ file names (not paths) of the raw
// benchmark output and benchstat CSV that justify the current state.
type ArchState struct {
	State    State    `json:"state"`
	Evidence []string `json:"evidence,omitempty"`
	Reason   string   `json:"reason,omitempty"`
	Updated  string   `json:"updated,omitempty"`
}

// Kernel is one registry entry. ID is the stable operation-level
// identifier (kernel.KernelID.String(), for example
// "compare-int32-gt"); Family, Type, and Operation are derived from the
// ID and re-checked by Validate so the registry cannot drift from the
// identifier grammar.
type Kernel struct {
	ID        string               `json:"id"`
	Family    string               `json:"family"`
	Type      string               `json:"type,omitempty"`
	Operation string               `json:"operation,omitempty"`
	Source    Source               `json:"source"`
	Arches    map[string]ArchState `json:"arches"`
}

// StateOn returns the kernel's state on arch, defaulting to
// StateCandidate when the architecture has no entry yet.
func (k *Kernel) StateOn(arch string) State {
	if a, ok := k.Arches[arch]; ok {
		return a.State
	}
	return StateCandidate
}

// Registry is the single source of truth for every kernel's gate state.
type Registry struct {
	Schema    int       `json:"schema"`
	Generated string    `json:"generated"`
	Kernels   []*Kernel `json:"kernels"`
}

// Find returns the entry for id, or nil when the registry does not
// know the kernel.
func (r *Registry) Find(id string) *Kernel {
	for _, k := range r.Kernels {
		if k.ID == id {
			return k
		}
	}
	return nil
}

// LoadRegistry reads and validates the registry file at path.
func LoadRegistry(path string) (*Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("kernelgate: read registry: %w", err)
	}
	var reg Registry
	if err := json.Unmarshal(data, &reg); err != nil {
		return nil, fmt.Errorf("kernelgate: parse registry %s: %w", path, err)
	}
	if err := reg.Validate(); err != nil {
		return nil, fmt.Errorf("kernelgate: invalid registry %s: %w", path, err)
	}
	return &reg, nil
}

// Save writes the registry to path with stable ordering: kernels sorted
// by ID, two-space indentation, trailing newline.
func (r *Registry) Save(path string) error {
	sort.Slice(r.Kernels, func(i, j int) bool { return r.Kernels[i].ID < r.Kernels[j].ID })
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("kernelgate: encode registry: %w", err)
	}
	data = append(data, '\n')
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("kernelgate: create registry directory %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("kernelgate: write registry %s: %w", path, err)
	}
	return nil
}

// Validate checks the registry for structural errors: schema version,
// duplicate or malformed IDs, metadata that disagrees with the ID
// grammar, unknown architectures, invalid states, and approved entries
// without evidence.
func (r *Registry) Validate() error {
	if r.Schema != RegistrySchema {
		return fmt.Errorf("unsupported schema %d (want %d)", r.Schema, RegistrySchema)
	}
	seen := make(map[string]bool, len(r.Kernels))
	for _, k := range r.Kernels {
		if k.ID == "" {
			return fmt.Errorf("kernel with empty id")
		}
		if seen[k.ID] {
			return fmt.Errorf("duplicate kernel id %q", k.ID)
		}
		seen[k.ID] = true
		family, typ, op, err := ParseKernelID(k.ID)
		if err != nil {
			return fmt.Errorf("kernel %q: %w", k.ID, err)
		}
		if k.Family != family {
			return fmt.Errorf("kernel %q: family %q does not match id-derived family %q", k.ID, k.Family, family)
		}
		if k.Type != typ {
			return fmt.Errorf("kernel %q: type %q does not match id-derived type %q", k.ID, k.Type, typ)
		}
		if k.Operation != op {
			return fmt.Errorf("kernel %q: operation %q does not match id-derived operation %q", k.ID, k.Operation, op)
		}
		switch k.Source {
		case SourceGenerated, SourceHandwritten:
		default:
			return fmt.Errorf("kernel %q: unknown source %q", k.ID, k.Source)
		}
		for arch, a := range k.Arches {
			if !archName.MatchString(arch) || !knownArches[arch] {
				return fmt.Errorf("kernel %q: unknown architecture %q", k.ID, arch)
			}
			if !validState(a.State) {
				return fmt.Errorf("kernel %q arch %s: unknown state %q", k.ID, arch, a.State)
			}
			if a.State == StateApproved && len(a.Evidence) == 0 {
				return fmt.Errorf("kernel %q arch %s: approved without evidence files", k.ID, arch)
			}
			for _, ev := range a.Evidence {
				if ev == "" || filepath.Base(ev) != ev {
					return fmt.Errorf("kernel %q arch %s: evidence %q must be a validation/ file name", k.ID, arch, ev)
				}
			}
		}
	}
	return nil
}
