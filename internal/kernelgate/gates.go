package kernelgate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// GatesSchema is the schema version of validation/simd-auto-gates.json
// that this package reads and writes.
const GatesSchema = 2

// Criteria holds the approval thresholds from the gates file. A kernel
// passes the isolated gate when its SIMD backend is at least
// MinimumImprovementPercent faster than scalar with a Mann-Whitney
// p-value at or below MaximumPValue, measured over at least
// MinimumSamples runs of Benchtime each.
type Criteria struct {
	MinimumSamples                   int     `json:"minimum_samples"`
	Benchtime                        string  `json:"benchtime"`
	MinimumImprovementPercent        float64 `json:"minimum_improvement_percent"`
	MaximumPValue                    float64 `json:"maximum_p_value"`
	MaximumEndToEndRegressionPercent float64 `json:"maximum_end_to_end_regression_percent"`
}

// Validate checks that the criteria are internally sane.
func (c Criteria) Validate() error {
	if c.MinimumSamples < 2 {
		return fmt.Errorf("kernelgate: criteria minimum_samples %d must be at least 2", c.MinimumSamples)
	}
	if c.Benchtime == "" {
		return fmt.Errorf("kernelgate: criteria benchtime must not be empty")
	}
	if c.MinimumImprovementPercent <= 0 {
		return fmt.Errorf("kernelgate: criteria minimum_improvement_percent %v must be positive", c.MinimumImprovementPercent)
	}
	if c.MaximumPValue <= 0 || c.MaximumPValue >= 1 {
		return fmt.Errorf("kernelgate: criteria maximum_p_value %v must lie in (0, 1)", c.MaximumPValue)
	}
	return nil
}

// Gates is a faithful in-memory view of validation/simd-auto-gates.json.
// Fields this package does not understand are preserved verbatim so a
// gate run never silently drops hand-maintained evidence.
type Gates struct {
	Schema    int      `json:"schema"`
	Generated string   `json:"generated"`
	Criteria  Criteria `json:"criteria"`
	// Architectures maps GOOS/GOARCH to the raw per-architecture entry.
	// Entries are edited through Approved and SetApproved, which decode
	// and re-encode the entry while keeping every other key.
	Architectures map[string]json.RawMessage `json:"architectures"`
}

// LoadGates reads the gates file at path.
func LoadGates(path string) (*Gates, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("kernelgate: read gates file: %w", err)
	}
	var g Gates
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&g); err != nil {
		return nil, fmt.Errorf("kernelgate: parse gates file %s: %w", path, err)
	}
	if g.Schema != GatesSchema {
		return nil, fmt.Errorf("kernelgate: gates file %s has schema %d, want %d", path, g.Schema, GatesSchema)
	}
	if err := g.Criteria.Validate(); err != nil {
		return nil, fmt.Errorf("kernelgate: gates file %s: %w", path, err)
	}
	return &g, nil
}

// Save writes the gates file with two-space indentation and a trailing
// newline. Keys inside per-architecture entries are emitted in sorted
// order (encoding/json map ordering); only entries touched through
// SetApproved are re-encoded, so untouched architectures keep their
// original bytes.
func (g *Gates) Save(path string) error {
	data, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		return fmt.Errorf("kernelgate: encode gates file: %w", err)
	}
	data = append(data, '\n')
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("kernelgate: create gates directory %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("kernelgate: write gates file %s: %w", path, err)
	}
	return nil
}

// Approved returns the sorted approved kernel ID list for arch, or nil
// when the architecture has no entry.
func (g *Gates) Approved(arch string) ([]string, error) {
	entry, err := g.archEntry(arch)
	if err != nil || entry == nil {
		return nil, err
	}
	var ids []string
	if raw, ok := entry["approved_kernel_ids"]; ok {
		if err := json.Unmarshal(raw, &ids); err != nil {
			return nil, fmt.Errorf("kernelgate: gates arch %s: decode approved_kernel_ids: %w", arch, err)
		}
	}
	return ids, nil
}

// SetApproved replaces the approved kernel ID list for arch (sorted and
// deduplicated), creating the architecture entry when missing. All
// other keys of the entry are preserved.
func (g *Gates) SetApproved(arch string, ids []string) error {
	entry, err := g.archEntry(arch)
	if err != nil {
		return err
	}
	if entry == nil {
		entry = map[string]json.RawMessage{}
		if g.Architectures == nil {
			g.Architectures = map[string]json.RawMessage{}
		}
	}
	uniq := make(map[string]bool, len(ids))
	sorted := make([]string, 0, len(ids))
	for _, id := range ids {
		if !uniq[id] {
			uniq[id] = true
			sorted = append(sorted, id)
		}
	}
	sort.Strings(sorted)
	raw, err := json.Marshal(sorted)
	if err != nil {
		return fmt.Errorf("kernelgate: encode approved ids: %w", err)
	}
	entry["approved_kernel_ids"] = raw
	merged, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("kernelgate: encode gates arch %s: %w", arch, err)
	}
	g.Architectures[arch] = merged
	return nil
}

// ApprovedArchKernel reports whether id is approved for arch.
func (g *Gates) ApprovedArchKernel(arch, id string) (bool, error) {
	ids, err := g.Approved(arch)
	if err != nil {
		return false, err
	}
	for _, a := range ids {
		if a == id {
			return true, nil
		}
	}
	return false, nil
}

// ArchField extracts a dotted field ("kernel_gate.samples_file") from
// the architecture entry, or "" when absent.
func (g *Gates) ArchField(arch, dotted string) (string, error) {
	entry, err := g.archEntry(arch)
	if err != nil || entry == nil {
		return "", err
	}
	var cur json.RawMessage
	parts := splitDot(dotted)
	cur, ok := entry[parts[0]]
	if !ok {
		return "", nil
	}
	for _, p := range parts[1:] {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(cur, &m); err != nil {
			return "", fmt.Errorf("kernelgate: gates arch %s field %q: %w", arch, dotted, err)
		}
		cur, ok = m[p]
		if !ok {
			return "", nil
		}
	}
	var s string
	if err := json.Unmarshal(cur, &s); err != nil {
		return "", fmt.Errorf("kernelgate: gates arch %s field %q is not a string: %w", arch, dotted, err)
	}
	return s, nil
}

func (g *Gates) archEntry(arch string) (map[string]json.RawMessage, error) {
	raw, ok := g.Architectures[arch]
	if !ok {
		return nil, nil
	}
	var entry map[string]json.RawMessage
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, fmt.Errorf("kernelgate: gates arch %s: %w", arch, err)
	}
	return entry, nil
}

func splitDot(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}
