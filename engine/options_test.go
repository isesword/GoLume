package engine_test

import (
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"os"
	"runtime"
	"testing"
)

func TestDefaultPipelineWorkersAreEvidenceBoundedAndExplicitlyOverridable(t *testing.T) {
	want := runtime.GOMAXPROCS(0)
	if want > 4 {
		want = 4
	}
	opts := engine.DefaultOptions()
	if opts.Pipeline.Workers != want {
		t.Fatalf("default pipeline workers=%d want=%d", opts.Pipeline.Workers, want)
	}

	opts.Pipeline = pipeline.Options{Workers: 8}
	eng := engine.New(opts)
	defer eng.Close()
	if got := eng.PipelineOptions().Workers; got != 8 {
		t.Fatalf("explicit pipeline workers=%d want=8", got)
	}
}

func TestEngineSpillConfigurationIsExplicitAndLazy(t *testing.T) {
	base := t.TempDir()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	opts.Spill = engine.SpillOptions{Directory: base, MaxBytes: 2 << 20}
	eng := engine.New(opts)
	defer eng.Close()
	if !eng.SpillConfigured() || eng.SpillLimit() != 2<<20 {
		t.Fatalf("configured=%v limit=%d", eng.SpillConfigured(), eng.SpillLimit())
	}
	if entries, err := os.ReadDir(base); err != nil || len(entries) != 0 {
		t.Fatalf("engine created spill eagerly entries=%v err=%v", entries, err)
	}
	query, err := eng.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	manager, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(manager.(*memory.QuerySpillStore).Directory()); err != nil {
		t.Fatal(err)
	}
	if err := query.Close(); err != nil {
		t.Fatal(err)
	}
}
