package kernelgate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testGatesJSON = `{
  "schema": 2,
  "generated": "2026-08-29",
  "criteria": {
    "minimum_samples": 10,
    "benchtime": "1s",
    "minimum_improvement_percent": 10,
    "maximum_p_value": 0.05,
    "maximum_end_to_end_regression_percent": 3
  },
  "architectures": {
    "darwin/arm64": {
      "status": "approved",
      "approved_kernel_ids": ["compare-int32-gt", "hash64-batch"],
      "kernel_gate": {
        "samples_file": "validation/simd-kernelid-candidates-refactor-arm64.txt",
        "benchstat_csv": "validation/simd-kernelid-candidates-refactor-arm64.csv"
      },
      "deleted_candidates": [
        {"kernel_id": "string-utf8-length", "reason": "regressed", "samples_file": "validation/simd-expression-e2e-refactor-arm64.txt"},
        {"family": "boolean", "reason": "word-level scalar was faster"}
      ]
    },
    "linux/amd64": {
      "approved_kernel_ids": [],
      "status": "scalar"
    }
  }
}
`

func writeGatesFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "simd-auto-gates.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadGates(t *testing.T) {
	g, err := LoadGates(writeGatesFile(t, testGatesJSON))
	if err != nil {
		t.Fatal(err)
	}
	if g.Schema != GatesSchema {
		t.Errorf("schema = %d, want %d", g.Schema, GatesSchema)
	}
	if g.Criteria.MinimumSamples != 10 || g.Criteria.Benchtime != "1s" {
		t.Errorf("unexpected criteria %+v", g.Criteria)
	}
	approved, err := g.Approved("darwin/arm64")
	if err != nil {
		t.Fatal(err)
	}
	if len(approved) != 2 || approved[0] != "compare-int32-gt" {
		t.Errorf("approved = %v", approved)
	}
	ok, err := g.ApprovedArchKernel("darwin/arm64", "hash64-batch")
	if err != nil || !ok {
		t.Errorf("ApprovedArchKernel = %t, %v", ok, err)
	}
	ok, err = g.ApprovedArchKernel("linux/amd64", "hash64-batch")
	if err != nil || ok {
		t.Errorf("ApprovedArchKernel linux/amd64 = %t, %v", ok, err)
	}
	samples, err := g.ArchField("darwin/arm64", "kernel_gate.samples_file")
	if err != nil {
		t.Fatal(err)
	}
	if samples != "validation/simd-kernelid-candidates-refactor-arm64.txt" {
		t.Errorf("ArchField = %q", samples)
	}
	missing, err := g.ArchField("linux/amd64", "kernel_gate.samples_file")
	if err != nil || missing != "" {
		t.Errorf("ArchField missing = %q, %v", missing, err)
	}
}

func TestLoadGatesErrors(t *testing.T) {
	cases := []struct {
		name, content, want string
	}{
		{"corrupt", "{nope", "parse"},
		{"bad schema", `{"schema": 1, "criteria": {}}`, "schema"},
		{"bad criteria", `{"schema": 2, "criteria": {"minimum_samples": 1}}`, "minimum_samples"},
	}
	for _, tc := range cases {
		_, err := LoadGates(writeGatesFile(t, tc.content))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %v, want substring %q", tc.name, err, tc.want)
		}
	}
	if _, err := LoadGates(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Error("missing file: expected error")
	}
}

func TestGatesSetApprovedRoundtrip(t *testing.T) {
	path := writeGatesFile(t, testGatesJSON)
	g, err := LoadGates(path)
	if err != nil {
		t.Fatal(err)
	}
	// Unsorted input with a duplicate and an existing id.
	if err := g.SetApproved("darwin/arm64", []string{"string-match-equal", "compare-int32-gt", "hash64-batch", "string-match-equal"}); err != nil {
		t.Fatal(err)
	}
	// A new architecture entry must be creatable.
	if err := g.SetApproved("linux/arm64", []string{"compare-int32-gt"}); err != nil {
		t.Fatal(err)
	}
	if err := g.Save(path); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadGates(path)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := reloaded.Approved("darwin/arm64")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"compare-int32-gt", "hash64-batch", "string-match-equal"}
	if strings.Join(approved, ",") != strings.Join(want, ",") {
		t.Errorf("approved = %v, want %v", approved, want)
	}
	approved, err = reloaded.Approved("linux/arm64")
	if err != nil {
		t.Fatal(err)
	}
	if len(approved) != 1 || approved[0] != "compare-int32-gt" {
		t.Errorf("linux/arm64 approved = %v", approved)
	}
	// Untouched keys of edited and untouched entries must survive.
	samples, err := reloaded.ArchField("darwin/arm64", "kernel_gate.samples_file")
	if err != nil || samples == "" {
		t.Errorf("kernel_gate.samples_file lost: %q, %v", samples, err)
	}
	var archEntry map[string]json.RawMessage
	if err := json.Unmarshal(reloaded.Architectures["linux/amd64"], &archEntry); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := json.Unmarshal(archEntry["status"], &status); err != nil || status != "scalar" {
		t.Errorf("linux/amd64 status lost: %q, %v", status, err)
	}
}

func TestDeletedCandidateIDs(t *testing.T) {
	g, err := LoadGates(writeGatesFile(t, testGatesJSON))
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := deletedCandidateIDs(g, "darwin/arm64")
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 1 || deleted[0].id != "string-utf8-length" {
		t.Fatalf("deleted = %+v", deleted)
	}
	if deleted[0].reason != "regressed" || len(deleted[0].evidence) != 1 || deleted[0].evidence[0] != "simd-expression-e2e-refactor-arm64.txt" {
		t.Errorf("deleted entry = %+v", deleted[0])
	}
	deleted, err = deletedCandidateIDs(g, "plan9/mips")
	if err != nil || deleted != nil {
		t.Errorf("missing arch deleted = %+v, %v", deleted, err)
	}
}

func TestCriteriaValidate(t *testing.T) {
	valid := Criteria{MinimumSamples: 10, Benchtime: "1s", MinimumImprovementPercent: 10, MaximumPValue: 0.05}
	if err := valid.Validate(); err != nil {
		t.Errorf("valid criteria rejected: %v", err)
	}
	cases := []Criteria{
		{MinimumSamples: 0, Benchtime: "1s", MinimumImprovementPercent: 10, MaximumPValue: 0.05},
		{MinimumSamples: 10, Benchtime: "", MinimumImprovementPercent: 10, MaximumPValue: 0.05},
		{MinimumSamples: 10, Benchtime: "1s", MinimumImprovementPercent: -1, MaximumPValue: 0.05},
		{MinimumSamples: 10, Benchtime: "1s", MinimumImprovementPercent: 10, MaximumPValue: 0},
		{MinimumSamples: 10, Benchtime: "1s", MinimumImprovementPercent: 10, MaximumPValue: 1.5},
	}
	for i, c := range cases {
		if err := c.Validate(); err == nil {
			t.Errorf("case %d: invalid criteria accepted", i)
		}
	}
}
