package kernelgate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// Runner abstracts every external command the pipeline needs so tests
// can inject fakes instead of running real benchmarks.
type Runner interface {
	// RunBenchmark runs "go test -bench" for pattern and returns the
	// raw benchmark output. samples and benchtime come from the gate
	// criteria.
	RunBenchmark(ctx context.Context, pattern string, samples int, benchtime string) ([]byte, error)
	// BenchstatCSV renders the benchstat CSV for raw benchmark output,
	// matching the validation/ CSV convention.
	BenchstatCSV(ctx context.Context, raw []byte) ([]byte, error)
	// LiveKernelIDs returns the kernel IDs that actually exist in the
	// kernel package (via cmd/kernel-info).
	LiveKernelIDs(ctx context.Context) ([]string, error)
	// GeneratedKernelIDs returns the kernel IDs whose scalar
	// implementation is emitted by internal/cmd/gensimd.
	GeneratedKernelIDs(ctx context.Context) ([]string, error)
}

// ExecRunner implements Runner by executing go commands in Root.
type ExecRunner struct {
	// Root is the repository root; commands run with it as working
	// directory.
	Root string
}

// command runs name with args in r.Root and returns stdout. stderr is
// captured for error messages.
func (r ExecRunner) command(ctx context.Context, env []string, stdin []byte, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = r.Root
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdin = bytes.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("kernelgate: %s %s: %w\n%s", name, strings.Join(args, " "), err, stderr.String())
	}
	return stdout.Bytes(), nil
}

var simdEnv = []string{"GOEXPERIMENT=simd"}

// RunBenchmark implements Runner.
func (r ExecRunner) RunBenchmark(ctx context.Context, pattern string, samples int, benchtime string) ([]byte, error) {
	return r.command(ctx, simdEnv, nil, "go", "test", "./kernel",
		"-run", "^$", "-bench", pattern,
		"-benchtime", benchtime, "-count", strconv.Itoa(samples), "-benchmem")
}

// BenchstatCSV implements Runner. The -col /mode pivot matches the
// convention of the existing validation/simd-*-arm64.csv files.
// benchstat requires file inputs, so the raw output is staged in a
// temporary file.
func (r ExecRunner) BenchstatCSV(ctx context.Context, raw []byte) ([]byte, error) {
	tmp, err := os.CreateTemp("", "kernelgate-bench-*.txt")
	if err != nil {
		return nil, fmt.Errorf("kernelgate: stage benchstat input: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("kernelgate: stage benchstat input: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("kernelgate: stage benchstat input: %w", err)
	}
	return r.command(ctx, nil, nil, "go", "tool", "benchstat", "-alpha", "0.05", "-col", "/mode", "-format", "csv", tmp.Name())
}

// LiveKernelIDs implements Runner.
func (r ExecRunner) LiveKernelIDs(ctx context.Context) ([]string, error) {
	out, err := r.command(ctx, simdEnv, nil, "go", "run", "./cmd/kernel-info", "-json")
	if err != nil {
		return nil, err
	}
	var report struct {
		Kernels []struct {
			ID string `json:"id"`
		} `json:"kernels"`
	}
	if err := json.Unmarshal(out, &report); err != nil {
		return nil, fmt.Errorf("kernelgate: parse kernel-info output: %w", err)
	}
	ids := make([]string, 0, len(report.Kernels))
	for _, k := range report.Kernels {
		ids = append(ids, k.ID)
	}
	return ids, nil
}

// GeneratedKernelIDs implements Runner.
func (r ExecRunner) GeneratedKernelIDs(ctx context.Context) ([]string, error) {
	out, err := r.command(ctx, nil, nil, "go", "run", "./internal/cmd/gensimd", "-list")
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			ids = append(ids, line)
		}
	}
	return ids, nil
}

// LocalArch returns the GOOS/GOARCH pair used as the architecture key
// in the registry and gates file.
func LocalArch() string { return runtime.GOOS + "/" + runtime.GOARCH }

// DetectCommit returns the current git HEAD commit in root, or "" when
// detection fails.
func DetectCommit(ctx context.Context, root string) string {
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
