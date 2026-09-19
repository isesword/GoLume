package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testSpec = `{
	"version": 1,
	"rows": 500,
	"columns": [
		{"name": "id", "type": "int64", "cardinality": 100},
		{"name": "s", "type": "string", "cardinality": 20},
		{"name": "b", "type": "bool"}
	]
}`

func writeSpec(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	return path
}

func TestRunWritesDeterministicFixture(t *testing.T) {
	spec := writeSpec(t, testSpec)
	out := filepath.Join(t.TempDir(), "fixture.parquet")
	var stdout, stderr bytes.Buffer
	code := run([]string{"-seed", "42", "-spec", spec, "-out", out}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run exit %d: %s", code, stderr.String())
	}
	var report summary
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("summary is not JSON: %v (%q)", err, stdout.String())
	}
	if report.Rows != 500 || report.Bytes <= 0 || len(report.SHA256) != 64 {
		t.Fatalf("unexpected summary %+v", report)
	}
	info, err := os.Stat(out)
	if err != nil {
		t.Fatalf("stat output: %v", err)
	}
	if info.Size() != report.Bytes {
		t.Fatalf("summary bytes %d, file size %d", report.Bytes, info.Size())
	}
}

func TestRunRefusesOverwriteWithoutFlag(t *testing.T) {
	spec := writeSpec(t, testSpec)
	out := filepath.Join(t.TempDir(), "fixture.parquet")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-spec", spec, "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("first run exit %d: %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	code := run([]string{"-spec", spec, "-out", out}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("second run exit %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "already exists") {
		t.Fatalf("stderr %q does not explain the existing target", stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-spec", spec, "-out", out, "-overwrite"}, &stdout, &stderr); code != 0 {
		t.Fatalf("overwrite run exit %d: %s", code, stderr.String())
	}
}

func TestRunRowsOverride(t *testing.T) {
	spec := writeSpec(t, testSpec)
	out := filepath.Join(t.TempDir(), "fixture.parquet")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-spec", spec, "-out", out, "-rows", "10000"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run exit %d: %s", code, stderr.String())
	}
	var report summary
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("summary is not JSON: %v", err)
	}
	if report.Rows != 10000 {
		t.Fatalf("summary rows %d, want 10000", report.Rows)
	}
}

func TestRunRejectsBadArguments(t *testing.T) {
	spec := writeSpec(t, testSpec)
	out := filepath.Join(t.TempDir(), "fixture.parquet")
	cases := []struct {
		name string
		args []string
		want int
	}{
		{"missing spec", []string{"-out", out}, 2},
		{"missing out", []string{"-spec", spec}, 2},
		{"unknown flag", []string{"-spec", spec, "-out", out, "-nope"}, 2},
		{"missing spec file", []string{"-spec", filepath.Join(t.TempDir(), "absent.json"), "-out", out}, 2},
		{"invalid spec", []string{"-spec", writeSpec(t, `{"version": 9, "rows": 1, "columns": []}`), "-out", out}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(tc.args, &stdout, &stderr); code != tc.want {
				t.Fatalf("run exit %d, want %d (stderr %q)", code, tc.want, stderr.String())
			}
		})
	}
}
