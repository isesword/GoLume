package kernelgate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTransition(t *testing.T) {
	cases := []struct {
		from, to State
		wantErr  bool
	}{
		{StateCandidate, StateApproved, false},
		{StateCandidate, StateRejected, false},
		{StateCandidate, StateCandidate, false},
		{StateApproved, StateCandidate, false},
		{StateRejected, StateCandidate, false},
		{StateApproved, StateApproved, false},
		{StateRejected, StateRejected, false},
		{StateApproved, StateRejected, true},
		{StateRejected, StateApproved, true},
		{State("bogus"), StateCandidate, true},
		{StateCandidate, State("bogus"), true},
	}
	for _, tc := range cases {
		err := Transition(tc.from, tc.to)
		if (err != nil) != tc.wantErr {
			t.Errorf("Transition(%s, %s) err=%v, wantErr=%t", tc.from, tc.to, err, tc.wantErr)
		}
	}
}

func validRegistry() *Registry {
	return &Registry{
		Schema:    RegistrySchema,
		Generated: "2026-09-19",
		Kernels: []*Kernel{
			{
				ID: "compare-int32-gt", Family: "comparison", Type: "int32", Operation: "gt",
				Source: SourceGenerated,
				Arches: map[string]ArchState{
					"darwin/arm64": {State: StateApproved, Evidence: []string{"simd-kernelid-candidates-refactor-arm64.txt"}},
				},
			},
			{
				ID: "string-match-equal", Family: "string-match", Type: "string", Operation: "equal",
				Source: SourceHandwritten,
				Arches: map[string]ArchState{},
			},
		},
	}
}

func TestRegistrySaveLoadRoundtrip(t *testing.T) {
	reg := validRegistry()
	path := filepath.Join(t.TempDir(), "kernel-registry.json")
	if err := reg.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Kernels) != len(reg.Kernels) {
		t.Fatalf("loaded %d kernels, want %d", len(loaded.Kernels), len(reg.Kernels))
	}
	for i, k := range loaded.Kernels {
		if k.ID != reg.Kernels[i].ID || k.Family != reg.Kernels[i].Family || k.Source != reg.Kernels[i].Source {
			t.Errorf("kernel %d mismatch: %+v vs %+v", i, k, reg.Kernels[i])
		}
	}
	if loaded.Find("compare-int32-gt").StateOn("darwin/arm64") != StateApproved {
		t.Error("approved state lost in roundtrip")
	}
	if loaded.Find("missing") != nil {
		t.Error("Find returned an entry for an unknown id")
	}
}

func TestRegistryValidate(t *testing.T) {
	mutate := func(f func(*Registry)) *Registry {
		reg := validRegistry()
		f(reg)
		return reg
	}
	cases := []struct {
		name string
		reg  *Registry
		want string
	}{
		{"valid", validRegistry(), ""},
		{"bad schema", mutate(func(r *Registry) { r.Schema = 99 }), "schema"},
		{"duplicate id", mutate(func(r *Registry) { r.Kernels = append(r.Kernels, r.Kernels[0]) }), "duplicate"},
		{"empty id", mutate(func(r *Registry) { r.Kernels[0].ID = "" }), "empty id"},
		{"malformed id", mutate(func(r *Registry) { r.Kernels[0].ID = "bogus-id" }), "unrecognized"},
		{"family mismatch", mutate(func(r *Registry) { r.Kernels[0].Family = "hash" }), "family"},
		{"type mismatch", mutate(func(r *Registry) { r.Kernels[0].Type = "int64" }), "type"},
		{"operation mismatch", mutate(func(r *Registry) { r.Kernels[0].Operation = "lt" }), "operation"},
		{"unknown source", mutate(func(r *Registry) { r.Kernels[0].Source = "vendored" }), "source"},
		{"unknown arch", mutate(func(r *Registry) {
			r.Kernels[0].Arches = map[string]ArchState{"plan9/mips": {State: StateCandidate}}
		}), "architecture"},
		{"invalid state", mutate(func(r *Registry) {
			r.Kernels[0].Arches = map[string]ArchState{"linux/amd64": {State: "limbo"}}
		}), "state"},
		{"approved without evidence", mutate(func(r *Registry) {
			r.Kernels[0].Arches = map[string]ArchState{"darwin/arm64": {State: StateApproved}}
		}), "evidence"},
		{"evidence with path", mutate(func(r *Registry) {
			r.Kernels[0].Arches = map[string]ArchState{"darwin/arm64": {State: StateApproved, Evidence: []string{"validation/x.txt"}}}
		}), "evidence"},
	}
	for _, tc := range cases {
		err := tc.reg.Validate()
		if tc.want == "" {
			if err != nil {
				t.Errorf("%s: unexpected error %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %v, want substring %q", tc.name, err, tc.want)
		}
	}
}

func TestLoadRegistryCorrupt(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.json")
	if _, err := LoadRegistry(missing); err == nil {
		t.Error("missing file: expected error")
	}
	corrupt := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRegistry(corrupt); err == nil || !strings.Contains(err.Error(), "parse") {
		t.Errorf("corrupt file: error %v, want parse error", err)
	}
	invalid := filepath.Join(dir, "invalid.json")
	if err := os.WriteFile(invalid, []byte(`{"schema": 1, "kernels": [{"id": ""}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRegistry(invalid); err == nil || !strings.Contains(err.Error(), "invalid registry") {
		t.Errorf("invalid content: error %v, want validation error", err)
	}
}

func TestParseKernelID(t *testing.T) {
	cases := []struct {
		id              string
		family, typ, op string
		wantErr         bool
	}{
		{"compare-int32-gt", "comparison", "int32", "gt", false},
		{"compare-uint64-eq", "comparison", "uint64", "eq", false},
		{"compare-float32-le", "comparison", "float32", "le", false},
		{"arithmetic-float64-div", "arithmetic", "float64", "div", false},
		{"arithmetic-uint32-add", "arithmetic", "uint32", "add", false},
		{"minmax-int32-masked", "reduction", "int32", "minmax-masked", false},
		{"minmax-int64-masked", "reduction", "int64", "minmax-masked", false},
		{"minmax-uint32-masked", "reduction", "uint32", "minmax-masked", false},
		{"minmax-uint64-masked", "reduction", "uint64", "minmax-masked", false},
		{"minmax-float32-masked", "reduction", "float32", "minmax-masked", false},
		{"minmax-float64-masked", "reduction", "float64", "minmax-masked", false},
		{"minmax-", "", "", "", true},
		{"minmax-int32", "", "", "", true},
		{"minmax--masked", "", "", "", true},
		{"minmax-int32-unmasked", "", "", "", true},
		{"minmax-int8-masked", "", "", "", true},
		{"sum-int32-masked", "reduction", "int32", "sum-masked", false},
		{"sum-uint32-masked", "reduction", "uint32", "sum-masked", false},
		{"sum-int64-masked", "reduction", "int64", "sum-masked", false},
		{"sum-uint64-masked", "reduction", "uint64", "sum-masked", false},
		{"sum-", "", "", "", true},
		{"sum-int32", "", "", "", true},
		{"sum--masked", "", "", "", true},
		{"sum-int32-unmasked", "", "", "", true},
		{"sum-float32-masked", "", "", "", true},
		{"sum-int8-masked", "reduction", "int8", "sum-masked", false},
		{"sum-int16-masked", "reduction", "int16", "sum-masked", false},
		{"sum-uint8-masked", "reduction", "uint8", "sum-masked", false},
		{"sum-uint16-masked", "reduction", "uint16", "sum-masked", false},
		{"boolean-and", "boolean", "", "and", false},
		{"hash64-batch", "hash", "uint64", "batch", false},
		{"hash128-batch", "hash", "uint64", "batch128", false},
		{"int64-scale", "scale", "int64", "scale", false},
		{"string-byte-length", "string-length", "string", "byte-length", false},
		{"string-utf8-length", "string-length", "string", "utf8-length", false},
		{"string-match-prefix", "string-match", "string", "prefix", false},
		{"compact-int16", "compact", "int16", "", false},
		{"", "", "", "", true},
		{"compare-", "", "", "", true},
		{"compare-int32", "", "", "", true},
		{"nonsense", "", "", "", true},
		{"boolean-", "", "", "", true},
	}
	for _, tc := range cases {
		family, typ, op, err := ParseKernelID(tc.id)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseKernelID(%q) err=%v, wantErr=%t", tc.id, err, tc.wantErr)
			continue
		}
		if tc.wantErr {
			continue
		}
		if family != tc.family || typ != tc.typ || op != tc.op {
			t.Errorf("ParseKernelID(%q) = (%q, %q, %q), want (%q, %q, %q)",
				tc.id, family, typ, op, tc.family, tc.typ, tc.op)
		}
		if !ValidFamily(family) {
			t.Errorf("ParseKernelID(%q): family %q not in known families", tc.id, family)
		}
	}
}
