package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func kernelSetTestSource(t *testing.T, ids []int32) *inmemory.Source {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt32})
	batch := memory.NewBatch()
	batch.Int32s["id"] = &memory.Int32Vector{Values: ids}
	batch.Reset(len(ids))
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestPreparedLazyPipelinesRetainEngineKernelSet(t *testing.T) {
	eng := engine.New(engine.Options{KernelMode: kernel.ModeSIMD})
	defer eng.Close()
	session := NewSessionWithEngine(eng)

	frame, err := session.FromSource(kernelSetTestSource(t, []int32{1, 2, 3})).Limit(2).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.kernels != eng.KernelSet() {
		t.Fatal("PreparedDataFrame did not retain the engine Set handle")
	}

	joined := session.FromSource(kernelSetTestSource(t, []int32{1, 2, 3})).Join(
		session.FromSource(kernelSetTestSource(t, []int32{2, 3, 4})),
		JoinOptions{Type: InnerJoin, LeftOn: []string{"id"}, RightOn: []string{"id"}},
	)
	preparedJoin, err := joined.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer preparedJoin.Close()
	if preparedJoin.kernels != eng.KernelSet() {
		t.Fatal("prepared join pipeline did not retain the engine Set handle")
	}
}
