package kernelgate

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
)

// Check verifies three-way consistency between the kernel registry, the
// gates file, and the kernels that exist in code, and returns every
// problem found. An empty result means CI may proceed.
func (p *Pipeline) Check(ctx context.Context) ([]string, error) {
	var problems []string
	reg, gates, err := p.loadPair()
	if err != nil {
		return nil, err
	}
	live, err := p.Runner.LiveKernelIDs(ctx)
	if err != nil {
		return nil, err
	}
	generated, err := p.Runner.GeneratedKernelIDs(ctx)
	if err != nil {
		return nil, err
	}

	// Registry versus live code: the two sets must be identical.
	regIDs := make(map[string]bool, len(reg.Kernels))
	for _, k := range reg.Kernels {
		regIDs[k.ID] = true
	}
	for _, id := range live {
		if !regIDs[id] {
			problems = append(problems, fmt.Sprintf("kernel %q exists in code but is missing from the registry", id))
		}
	}
	liveSet := make(map[string]bool, len(live))
	for _, id := range live {
		liveSet[id] = true
	}
	for _, id := range sortedIDs(reg) {
		if !liveSet[id] {
			problems = append(problems, fmt.Sprintf("kernel %q is in the registry but does not exist in code", id))
		}
	}

	// Registry source versus the gensimd generated list.
	genSet := make(map[string]bool, len(generated))
	for _, id := range generated {
		genSet[id] = true
		if !regIDs[id] {
			problems = append(problems, fmt.Sprintf("gensimd generates %q but it is missing from the registry", id))
		}
	}
	for _, k := range reg.Kernels {
		if (k.Source == SourceGenerated) != genSet[k.ID] {
			problems = append(problems, fmt.Sprintf("kernel %q: registry source %q disagrees with gensimd -list", k.ID, k.Source))
		}
	}

	// Registry state versus gates approvals, per architecture, plus
	// evidence file existence.
	for _, k := range reg.Kernels {
		arches := make([]string, 0, len(k.Arches))
		for arch := range k.Arches {
			arches = append(arches, arch)
		}
		sort.Strings(arches)
		for _, arch := range arches {
			a := k.Arches[arch]
			approved, err := gates.ApprovedArchKernel(arch, k.ID)
			if err != nil {
				return nil, err
			}
			if (a.State == StateApproved) != approved {
				problems = append(problems, fmt.Sprintf("kernel %q arch %s: registry state %s disagrees with gates approved list", k.ID, arch, a.State))
			}
			for _, ev := range a.Evidence {
				if _, err := os.Stat(filepath.Join(p.Root, ValidationDir, ev)); err != nil {
					problems = append(problems, fmt.Sprintf("kernel %q arch %s: evidence file %s: %v", k.ID, arch, ev, err))
				}
			}
		}
	}

	// Gates approvals must all be known, approved kernels.
	for arch := range gates.Architectures {
		approved, err := gates.Approved(arch)
		if err != nil {
			return nil, err
		}
		for _, id := range approved {
			entry := reg.Find(id)
			if entry == nil {
				problems = append(problems, fmt.Sprintf("gates arch %s approves %q but it is missing from the registry", arch, id))
				continue
			}
			if entry.StateOn(arch) != StateApproved {
				problems = append(problems, fmt.Sprintf("gates arch %s approves %q but the registry marks it %s", arch, id, entry.StateOn(arch)))
			}
		}
	}
	return problems, nil
}

// Status renders a per-kernel state table: one row per kernel, with its
// family, implementation source, and per-architecture gate state.
func Status(w io.Writer, reg *Registry) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "KERNEL ID\tFAMILY\tTYPE\tSOURCE\tSTATE")
	for _, k := range reg.Kernels {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", k.ID, k.Family, dash(k.Type), k.Source, stateSummary(k))
	}
	return tw.Flush()
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// stateSummary renders a kernel's per-architecture states compactly,
// for example "darwin/arm64:approved".
func stateSummary(k *Kernel) string {
	if len(k.Arches) == 0 {
		return "candidate (no arch entries)"
	}
	arches := make([]string, 0, len(k.Arches))
	for arch := range k.Arches {
		arches = append(arches, arch)
	}
	sort.Strings(arches)
	parts := make([]string, 0, len(arches))
	for _, arch := range arches {
		parts = append(parts, arch+":"+string(k.Arches[arch].State))
	}
	return strings.Join(parts, " ")
}
