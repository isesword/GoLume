package main

import (
	"bytes"
	"encoding/json"
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
