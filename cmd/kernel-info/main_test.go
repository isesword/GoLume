package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"gocolumn/kernel"
)

func TestRunJSONReportsSelectedCapabilities(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-mode", "scalar", "-json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run returned %d: %s", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var got report
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, stdout.String())
	}
	if got.Arch == "" {
		t.Fatal("missing arch")
	}
	if got.Mode != kernel.ModeScalar.String() {
		t.Fatalf("mode = %q, want %q", got.Mode, kernel.ModeScalar)
	}
	ids := kernel.KernelIDs()
	if len(got.Kernels) != len(ids) {
		t.Fatalf("reported %d kernels, want %d", len(got.Kernels), len(ids))
	}
	for i, entry := range got.Kernels {
		if entry.ID != ids[i].String() {
			t.Fatalf("kernel[%d] id = %q, want %q", i, entry.ID, ids[i])
		}
		if entry.SelectedBackend != "scalar" || entry.VectorBits != 0 || entry.HardwareSIMD || entry.Emulated {
			t.Fatalf("kernel[%d] = %+v, want scalar capability", i, entry)
		}
		if entry.AvailableBackend == "" {
			t.Fatalf("kernel[%d] missing available backend", i)
		}
	}
}

func TestRunTextDoesNotInferCapabilityState(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-mode", "scalar"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run returned %d: %s", code, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "arch=") || !strings.Contains(output, "backend=scalar") {
		t.Fatalf("unexpected text output: %s", output)
	}
	if strings.Contains(output, "state=") {
		t.Fatalf("text output inferred unreported state: %s", output)
	}
}

func TestRunRejectsUnknownMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-mode", "unknown", "-json"}, &stdout, &stderr); code != 2 {
		t.Fatalf("run returned %d, want 2", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), `unknown mode "unknown"`) {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
}

func TestRunJSONReportsExactCapabilitiesInEveryMode(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		t.Run(mode.String(), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run([]string{"-mode", mode.String(), "-json"}, &stdout, &stderr); code != 0 {
				t.Fatalf("run returned %d: %s", code, stderr.String())
			}
			var got report
			if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
				t.Fatalf("decode JSON output: %v", err)
			}
			selected, automatic, available := kernel.New(mode), kernel.New(kernel.ModeAuto), kernel.New(kernel.ModeSIMD)
			check := func(entry capabilityReport, id string, want, auto, forced kernel.KernelCapability) {
				t.Helper()
				if entry.ID != id || entry.SelectedBackend != want.Backend || entry.VectorBits != want.VectorBits ||
					entry.HardwareSIMD != want.HardwareSIMD || entry.Emulated != want.Emulated ||
					entry.AvailableBackend != forced.Backend || entry.AutoAdmitted != (auto.Backend != "scalar") {
					t.Fatalf("entry=%+v, want id=%s selected=%+v auto=%+v available=%+v", entry, id, want, auto, forced)
				}
			}
			ids := kernel.KernelIDs()
			if len(got.Kernels) != len(ids) {
				t.Fatalf("reported %d kernels, want %d", len(got.Kernels), len(ids))
			}
			seen := make(map[string]bool)
			for i, id := range ids {
				want, _ := selected.Capability(id)
				auto, _ := automatic.Capability(id)
				forced, _ := available.Capability(id)
				check(got.Kernels[i], id.String(), want, auto, forced)
				seen[got.Kernels[i].ID] = true
			}
			if len(got.ColumnComparisons) != 50 {
				t.Fatalf("reported %d column comparisons, want 50", len(got.ColumnComparisons))
			}
			i := 0
			for _, typ := range columnComparisonTypes {
				for op := kernel.CompareGT; op <= kernel.CompareEQ; op++ {
					id := fmt.Sprintf("compare-columns-%s-%s", typ, op)
					want, ok := selected.ColumnComparisonCapability(typ, op)
					auto, autoOK := automatic.ColumnComparisonCapability(typ, op)
					forced, forcedOK := available.ColumnComparisonCapability(typ, op)
					if !ok || !autoOK || !forcedOK {
						t.Fatalf("missing capability for %s", id)
					}
					entry := got.ColumnComparisons[i]
					check(entry, id, want, auto, forced)
					if seen[entry.ID] {
						t.Fatalf("duplicate literal/column report ID %q", entry.ID)
					}
					seen[entry.ID] = true
					i++
				}
			}
		})
	}
}

func TestRunTextIncludesSeparateColumnCapabilities(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-mode", "auto"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run returned %d: %s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 1+len(kernel.KernelIDs())+50 {
		t.Fatalf("reported %d lines, want arch header, KernelIDs, and 50 column comparisons", len(lines))
	}
	for _, line := range lines[1:] {
		if !strings.Contains(line, " available=") || !strings.Contains(line, " auto=") {
			t.Fatalf("missing dispatch metadata: %s", line)
		}
	}
	if !strings.Contains(stdout.String(), "compare-float32-gt ") ||
		!strings.Contains(stdout.String(), "compare-columns-float32-gt ") {
		t.Fatalf("literal and column comparisons not separately reported: %s", stdout.String())
	}
}

func TestRunCapabilityChecks(t *testing.T) {
	for _, tc := range []struct {
		args   []string
		code   int
		output string
	}{
		{[]string{"-mode", "scalar", "-check-scalar", "-max-native-width"}, 0, "0\n"},
		{[]string{"-mode", "scalar", "-check-native-width", "128"}, 1, ""},
		{[]string{"-check-native-width", "64"}, 2, ""},
		{[]string{"-check-native-width", "-1"}, 2, ""},
		{[]string{"-check-scalar", "-check-native-width", "128"}, 2, ""},
		{[]string{"-json", "-max-native-width"}, 2, ""},
	} {
		var out, err bytes.Buffer
		if code := run(tc.args, &out, &err); code != tc.code || out.String() != tc.output {
			t.Fatalf("args=%v: code=%d output=%q stderr=%s", tc.args, code, out.String(), err.String())
		}
	}
}

func TestValidateCapabilities(t *testing.T) {
	scalar := capabilityReport{ID: "scalar", SelectedBackend: "scalar"}
	native := capabilityReport{ID: "native", SelectedBackend: "portable-simd", VectorBits: 256, HardwareSIMD: true}
	for _, tc := range []struct {
		name   string
		scalar bool
		width  int
		groups [][]capabilityReport
		want   int
		fail   bool
	}{
		{"scalar", true, 0, [][]capabilityReport{{scalar}, {scalar}}, 0, false},
		{"native", false, 256, [][]capabilityReport{{scalar}, {native}}, 256, false},
		{"mismatch", false, 512, [][]capabilityReport{{native}}, 0, true},
		{"scalar rejects columns", true, 0, [][]capabilityReport{{scalar}, {native}}, 0, true},
		{"empty group", false, 0, [][]capabilityReport{{scalar}, {}}, 0, true},
		{"emulation is not native", false, 128, [][]capabilityReport{{{ID: "emulated", SelectedBackend: "portable-simd", VectorBits: 128, Emulated: true}}}, 0, true},
		{"invalid native", false, 0, [][]capabilityReport{{{ID: "bad", SelectedBackend: "scalar", HardwareSIMD: true}}}, 0, true},
		{"invalid width", false, 0, [][]capabilityReport{{{ID: "bad", SelectedBackend: "native", HardwareSIMD: true, VectorBits: 64}}}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateCapabilities(tc.scalar, tc.width, tc.groups...)
			if (err != nil) != tc.fail || got != tc.want {
				t.Fatalf("got=%d err=%v", got, err)
			}
		})
	}
}
