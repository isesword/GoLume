package main

import (
	"context"
	"flag"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	parquetsource "gocolumn/source/parquet"
	"log"
	"sort"
)

func main() {
	path := flag.String("parquet", "testdata/bench-1m.parquet", "input parquet file")
	workers := flag.Int("workers", 4, "max workers")
	materialize := flag.Bool("materialize", false, "materialize grouped rows")
	force := flag.String("strategy", "", "force: direct-fused|bitmap-direct|radix")
	repeat := flag.Int("repeat", 1, "execute the prepared query N times")
	concurrency := flag.Int("prepare-concurrency", 1, "max simultaneous executions of this prepared query")
	flag.Parse()
	src, err := parquetsource.Open(*path, parquetsource.Columns{PredicateIndex: 0, GroupIndex: 1, ValueIndex: 2})
	if err != nil {
		log.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = *workers
	opts.KernelMode = kernel.ModeAuto
	eng := engine.New(opts)
	defer eng.Close()
	q := expr.Query{Filter: expr.GT(expr.Col("pred"), expr.I64(32767)), GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("count")}, Hints: expr.Hints{Selectivity: .451, Cardinality: 1 << 18, ForceStrategy: *force}}
	prepared, err := eng.Prepare(context.Background(), src, q, engine.PrepareOptions{MaxConcurrency: *concurrency})
	if err != nil {
		log.Fatal(err)
	}
	defer prepared.Close()
	fmt.Printf("kernel=%+v\n", eng.KernelCapabilities())
	fmt.Printf("plan=%+v\n", prepared.Plan())
	var res engine.Result
	for i := 0; i < *repeat; i++ {
		res, err = prepared.Execute(context.Background(), engine.ExecuteOptions{Materialize: *materialize})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("run=%d strategy=%s workers=%d groups=%d selected=%d checksum=%d elapsed=%s retained_memory=%d\n", i+1, res.Strategy.Kind, res.Strategy.Workers, res.GroupCount, res.SelectedRows, res.Checksum, res.Elapsed, eng.Memory().Used())
	}
	if *materialize {
		sort.Slice(res.Groups, func(i, j int) bool { return res.Groups[i].Key < res.Groups[j].Key })
		limit := 5
		if len(res.Groups) < limit {
			limit = len(res.Groups)
		}
		for _, g := range res.Groups[:limit] {
			fmt.Printf("key=%d sum=%d count=%d\n", g.Key, g.Sum, g.Count)
		}
	}
}
