package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// These tests only run fake tools. They verify the gate's commands and failure
// paths without recursively invoking go test or requiring SIMD hardware.
func runValidation(t *testing.T, phase string, overrides ...string) (string, string, error) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash unavailable")
	}
	_, file, _, _ := runtime.Caller(0)
	dir := t.TempDir()
	script := `#!/usr/bin/env bash
set -euo pipefail
printf '%s|%s|%s|%s|%s|%s|%s\n' "$GOEXPERIMENT" "$GODEBUG" "$GOFLAGS" "${CGO_ENABLED:-default}" "${GOOS:-}" "${GOARCH:-}" "$*" >> "$FAKE_TRACE"
if [[ "$*" == 'env GOHOSTARCH' ]]; then echo "$FAKE_ARCH"; exit; fi
if [[ "$*" == 'env GOHOSTOS' ]]; then echo linux; exit; fi
if [[ "${FAKE_FAIL:-}" == "$*" ]]; then exit 19; fi
if [[ "$*" == *'./cmd/kernel-info'* ]]; then
  bits="$FAKE_BITS"
  if [[ "$*" == *'-check-scalar'* ]]; then bits=0; fi
  if [[ "$*" == *'-check-native-width '* ]]; then
    bits="$(echo "$*" | sed -E 's/.*-check-native-width ([0-9]+).*/\1/')"
  fi
  echo "$bits"
fi
`
	for name, content := range map[string]string{
		"go":    script,
		"uname": "#!/usr/bin/env bash\necho \"$FAKE_MACHINE\"\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0755); err != nil {
			t.Fatal(err)
		}
	}
	trace := filepath.Join(dir, "trace")
	cmd := exec.Command("bash", filepath.Join(filepath.Dir(file), "validate.sh"), phase)
	env := map[string]string{}
	for _, pair := range os.Environ() {
		k, v, _ := strings.Cut(pair, "=")
		env[k] = v
	}
	for _, pair := range append([]string{
		"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"FAKE_TRACE=" + trace, "FAKE_ARCH=amd64", "FAKE_MACHINE=x86_64", "FAKE_BITS=512", "FAKE_FAIL=",
		"GOEXPERIMENT=invalid", "GODEBUG=simd=0", "GOFLAGS=-run=NoTests", "GOOS=windows", "GOARCH=arm64", "CGO_ENABLED=0",
		"VALIDATE_EXPECT_ARCH=", "VALIDATE_EXPECT_OS=", "VALIDATE_REQUIRE_NATIVE=0",
	}, overrides...) {
		k, v, _ := strings.Cut(pair, "=")
		env[k] = v
	}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	output, err := cmd.CombinedOutput()
	calls, _ := os.ReadFile(trace)
	return string(calls), string(output), err
}

func TestValidationRuntimeMatrix(t *testing.T) {
	for _, tc := range []struct{ name, arch, machine, bits string }{
		{"amd64-512", "amd64", "x86_64", "512"},
		{"amd64-256", "amd64", "x86_64", "256"},
		{"arm64-128", "arm64", "aarch64", "128"},
		{"scalar-host", "amd64", "x86_64", "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, out, err := runValidation(t, "runtime", "FAKE_ARCH="+tc.arch, "FAKE_MACHINE="+tc.machine, "FAKE_BITS="+tc.bits)
			if err != nil {
				t.Fatalf("%v: %s", err, out)
			}
			if strings.Contains(calls, "NoTests") || strings.Contains(calls, "invalid") {
				t.Fatal("inherited Go settings leaked")
			}
			for _, width := range []string{"128", "256", "512"} {
				has := strings.Contains(calls, "simd=+"+width+"|-buildvcs=false|default|linux|"+tc.arch+"|test -count=1 ./...")
				want := tc.bits != "0" && width <= tc.bits
				if has != want {
					t.Fatalf("width %s tested=%t, want %t\n%s", width, has, want, calls)
				}
			}
			for _, flag := range []string{"simd=0", "cpu.all=off", "cpu.avx2=off", "simd=+128,cpu.avx2=off"} {
				has := strings.Contains(calls, "simd|"+flag+"|-buildvcs=false|default|linux|"+tc.arch+"|test -count=1 ./...")
				want := flag == "simd=0" || (tc.arch == "amd64" && (flag != "simd=+128,cpu.avx2=off" || tc.bits != "0"))
				if has != want {
					t.Fatalf("fallback %s tested=%t, want %t", flag, has, want)
				}
			}
		})
	}
}

func TestValidationRaceCoversBothBuilds(t *testing.T) {
	calls, out, err := runValidation(t, "race")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for _, line := range []string{"||-buildvcs=false|1|linux|amd64|test -race -count=1 ./...", "simd||-buildvcs=false|1|linux|amd64|test -race -count=1 ./..."} {
		if !strings.Contains(calls, line) {
			t.Fatalf("missing %s\n%s", line, calls)
		}
	}
}

func TestValidationCrossCompilesBothBuildsAndArchitectures(t *testing.T) {
	calls, out, err := runValidation(t, "cross")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for _, arch := range []string{"amd64", "arm64"} {
		for _, exp := range []string{"", "simd"} {
			want := exp + "||-buildvcs=false|0|linux|" + arch + "|build ./..."
			if !strings.Contains(calls, want) {
				t.Fatalf("missing %s", want)
			}
		}
	}
}

func TestValidationFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, phase string
		env         []string
		match       string
	}{
		{"bad phase", "unknown", nil, "usage:"},
		{"wrong arch", "tests", []string{"VALIDATE_EXPECT_ARCH=arm64"}, "expected arm64"},
		{"wrong os", "tests", []string{"VALIDATE_EXPECT_OS=darwin"}, "expected darwin"},
		{"emulated host", "tests", []string{"FAKE_MACHINE=aarch64"}, "native execution required"},
		{"missing native", "runtime", []string{"FAKE_BITS=0", "VALIDATE_REQUIRE_NATIVE=1"}, "native SIMD required"},
		{"bad probe", "runtime", []string{"FAKE_BITS=oops"}, "invalid native width"},
		{"failed probe", "runtime", []string{"FAKE_FAIL=run ./cmd/kernel-info -mode simd -max-native-width"}, ""},
		{"failed tests", "tests", []string{"FAKE_FAIL=test -count=1 ./..."}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, out, err := runValidation(t, tc.phase, tc.env...)
			if err == nil || !strings.Contains(out, tc.match) {
				t.Fatalf("err=%v out=%s", err, out)
			}
			if tc.phase == "runtime" && strings.Contains(calls, "test -count=1") {
				t.Fatal("continued after failed probe")
			}
			if tc.name == "failed tests" && strings.Contains(calls, "simd||") {
				t.Fatal("continued after failed tests")
			}
		})
	}
}

func TestCIUsesSharedValidationPhases(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	b, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(b)
	localScript, err := os.ReadFile(filepath.Join(filepath.Dir(file), "validate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(localScript), "for phase in checks tests runtime race cross demo; do") {
		t.Fatal("the local aggregate no longer contains the CI phase list in order")
	}
	for _, phase := range []string{"checks", "tests", "runtime", "race", "cross", "demo"} {
		if strings.Count(workflow, "run: bash scripts/validate.sh "+phase+"\n") != 1 {
			t.Fatalf("phase %s missing or duplicated", phase)
		}
	}
	if strings.Contains(workflow, "run: go ") || strings.Contains(workflow, "run: GOEXPERIMENT=") {
		t.Fatal("CI duplicates gate commands")
	}
	for _, required := range []string{
		"name: ${{ matrix.check_name }}",
		"check_name: validate (ubuntu-latest)",
		"check_name: validate (macos-latest)",
		"check_name: validate (ubuntu-24.04-arm, arm64)",
		"branches: [main, master]",
		"  pull_request:",
		"  workflow_dispatch:",
		"os: ubuntu-24.04-arm",
		"os: macos-15",
		"VALIDATE_REQUIRE_NATIVE: '1'",
		"VALIDATE_EXPECT_ARCH: ${{ matrix.arch }}",
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("missing workflow contract %q", required)
		}
	}
}

func TestStructureSearchErrorsFailClosed(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash unavailable")
	}
	_, file, _, _ := runtime.Caller(0)
	for _, enumerationFails := range []bool{false, true} {
		dir := t.TempDir()
		rg := `#!/usr/bin/env bash
if [[ "$1" == --files ]]; then
 printf '%s\n' engine/engine.go cmd/kernel-info/main.go kernel/set.go
 exit 0
fi
exit 2
`
		if enumerationFails {
			rg = "#!/usr/bin/env bash\nexit 2\n"
		}
		for name, content := range map[string]string{"go": "#!/usr/bin/env bash\nexit 0\n", "rg": rg} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0755); err != nil {
				t.Fatal(err)
			}
		}
		cmd := exec.Command("bash", filepath.Join(filepath.Dir(file), "check_simd_structure.sh"))
		cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"))
		out, err := cmd.CombinedOutput()
		if err == nil || strings.Contains(string(out), "checks passed") {
			t.Fatalf("search error ignored: %v: %s", err, out)
		}
	}
}
