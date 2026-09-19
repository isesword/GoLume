// Command kernelgate is the kernel registry and approval-gate front
// end. Subcommands:
//
//	kernelgate init   [-force]                 rebuild the registry from code and the gates file
//	kernelgate status                          print the per-kernel state table
//	kernelgate check                           verify registry/gates/code consistency (CI)
//	kernelgate gate -kernel <id> [-kernel ...] run the approval gate for candidate kernels
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gocolumn/internal/kernelgate"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// stringList collects a repeated flag value.
type stringList []string

// String renders the collected values for flag usage output.
func (s *stringList) String() string { return fmt.Sprint([]string(*s)) }

// Set appends one flag occurrence to the list.
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: kernelgate <init|status|check|gate> [flags]")
		return 2
	}
	ctx := context.Background()
	var err error
	switch args[0] {
	case "init":
		err = cmdInit(args[1:], stdout)
	case "status":
		err = cmdStatus(args[1:], stdout)
	case "check":
		err = cmdCheck(args[1:], stdout)
	case "gate":
		err = cmdGate(ctx, args[1:], stdout)
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q\n", args[0])
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

// repoRoot walks up from the working directory until it finds go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from working directory")
		}
		dir = parent
	}
}

// pipeline builds the production pipeline for the repository root.
func pipeline(root string) *kernelgate.Pipeline {
	return &kernelgate.Pipeline{Root: root, Runner: kernelgate.ExecRunner{Root: root}}
}

func cmdInit(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite an existing registry file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	reg, err := pipeline(root).Init(context.Background(), kernelgate.InitOptions{Force: *force})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "wrote %s with %d kernels\n", kernelgate.RegistryPath, len(reg.Kernels))
	return nil
}

func cmdStatus(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	reg, err := kernelgate.LoadRegistry(filepath.Join(root, kernelgate.RegistryPath))
	if err != nil {
		return err
	}
	return kernelgate.Status(stdout, reg)
}

func cmdCheck(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	problems, err := pipeline(root).Check(context.Background())
	if err != nil {
		return err
	}
	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Fprintln(stdout, "INCONSISTENT: "+p)
		}
		return fmt.Errorf("%d consistency problem(s)", len(problems))
	}
	fmt.Fprintln(stdout, "kernel registry, gates file, and code are consistent")
	return nil
}

func cmdGate(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("gate", flag.ContinueOnError)
	var kernels stringList
	fs.Var(&kernels, "kernel", "kernel ID to gate; repeat for multiple kernels")
	bench := fs.String("bench", "", "benchmark pattern override (default isolates BenchmarkKernelID/<id>)")
	force := fs.Bool("force", false, "re-gate kernels that are not candidates")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(kernels) == 0 {
		return fmt.Errorf("gate requires at least one -kernel flag")
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	p := pipeline(root)
	failed := false
	for _, id := range kernels {
		res, err := p.Gate(ctx, id, kernelgate.GateOptions{BenchPattern: *bench, Force: *force})
		if err != nil {
			fmt.Fprintf(stdout, "%s: ERROR %v\n", id, err)
			failed = true
			continue
		}
		d := res.Decision
		verdict := "REJECTED"
		if d.Pass {
			verdict = "APPROVED"
		}
		fmt.Fprintf(stdout, "%s: %s on %s (improvement %.2f%%, p=%.4f, n=%d+%d)\n",
			id, verdict, res.Arch, d.ImprovementPercent, d.PValue, d.ScalarSamples, d.SIMDSamples)
		if d.Note != "" {
			fmt.Fprintf(stdout, "  note: %s\n", d.Note)
		}
		fmt.Fprintf(stdout, "  evidence: %s %s\n", res.SamplesFile, res.CSVFile)
		if !d.Pass {
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("one or more kernels did not pass the gate")
	}
	return nil
}
