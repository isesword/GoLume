# Phase 2.1 implementation notes

## 1. 本阶段目的

Phase 1 已证明 int32 + Parquet + SIMD/radix 可以工作。Phase 2.1 解决两个问题：

1. 把 benchmark 式“一次查询一次建所有 scratch”改成可重复执行、可并发、可计费的 Prepared execution；
2. 验证性能路线不依赖单一 int32 key，扩到 dictionary string、plain string 和 16-byte composite key，并建立 join/operator 接口。

## 2. PreparedQuery 生命周期

```text
Engine.Prepare
  -> compile expression program
  -> read source stats / partitions
  -> choose physical strategy once
  -> validate dictionary domains
  -> create bounded workspace-pool descriptor (lazy)

PreparedQuery.Execute
  -> acquire workspace
  -> reuse readers/batches/bitmaps/hash/radix scratch
  -> execute
  -> return workspace to same prepared query

PreparedQuery.Close
  -> stop new executions
  -> wait active Execute calls
  -> close retained readers
  -> release retained memory reservations
```

不使用 `sync.Pool`，原因是它不可枚举/不可强制释放，也无法和 query memory budget 建立严格 ownership。

`MaxConcurrency=N` 的含义不是 worker 数，而是**同一个 prepared query 同时允许多少个 Execute**；最多会形成 N 份完整 query workspace，因此用户可以用它控制 latency/内存并发权衡。

## 3. Memory accounting

当前 memory budget 负责 engine-owned scratch：

- selection bitmap；
- expression registers；
- aggregate hash table；
- string arena；
- selected key/value compact buffers；
- radix histogram/scatter/local tables。

Source/Arrow-Go reader 自身内部 allocation 暂不由 `memory.Manager` 控制，这是后续 Arrow allocator / RecordBatch adapter 要解决的边界。

## 4. Predicate program / SQL three-valued logic

每个 boolean register 使用两个 bitmap：

```text
Truth    : 为 TRUE 的行
Validity : TRUE/FALSE 已知的行；0 代表 UNKNOWN(NULL)
```

逻辑公式：

```text
AND:
T = AT & BT
F = (AV & ~AT) | (BV & ~BT)
V = T | F

OR:
T = AT | BT
F = (AV & ~AT) & (BV & ~BT)
V = T | F

NOT:
T = AV & ~AT
V = AV
```

WHERE 最终只消费 root `Truth`，UNKNOWN 自然被过滤。

简单 `int64 > literal` 仍可被识别为 `SimpleGT`，planner 可以选 `direct-fused`，避免 materialize bitmap；复杂表达式自动走 program/bitmap。

## 5. Key physical representations

### int32 / int64 / uint32

保持专用 table，不在 row hot loop 走 `interface{}`。

### Dictionary string

```text
rows:       []uint32 code
validity:   bitmap
dictionary: offsets + bytes
domain:     stable ID
```

只有 `DictionaryDomain` 在全部 partitions 中稳定时，code 才能作为 semantic group key。Source 可以实现 `DictionaryProvider`，materialization 时将 code 解回 string；没有 provider 时 Result 保留 code + domain。

### Plain string view

```text
offsets []uint32
bytes   []byte
```

Hash table 不能存 batch buffer pointer/ref，因为 reader 下一次可能覆盖数据。因此新 group 第一次出现时把 key bytes 复制到 Prepared workspace arena；后续 equality 在 arena 上完成。

### Two-column fixed key

两列 int32/int64/uint32/stable-dictionary-string 编译成：

```go
type Key128 struct { A, B uint64 }
```

它不是字符串拼接，也不创建 Go object。Hash/equality/radix 全部有独立 16-byte 热路径。

## 6. Physical strategies

```text
direct-fused
  predicate + aggregate in one row loop

bitmap-direct
  expression/SIMD -> selection bitmap -> one hash table

radix
  parallel decode/filter
    -> compact selected key/value
    -> hash/histogram
    -> scatter
    -> exclusive partition ownership
    -> lock-free local aggregate
```

`planner.Heuristic` 当前考虑：rows、selectivity、cardinality、worker、key width、predicate complexity、SIMD width、是否可 radix。它仍是 bootstrap，不是最终 optimizer。

## 7. ARM / x86 strategy consequence

M4 Pro 实测：简单 predicate 下 128-bit SIMD bitmap 比 fused scalar 慢；x86 AVX-512 对 packed filter/bitmap 则有显著收益。因此核心原则是：

> SIMD 是 physical implementation choice，不是 query semantic，也不是全局 on/off 策略。

`kernel.Set` 对外不暴露 `simd` 类型，未来可以增加 arch-specific fast path 或 calibration，不改 planner/executor API。

## 8. Join 当前状态

`join.I32Table` 已实现 reusable build/probe：

- duplicate build keys 通过 chain 保留；
- inner / left / semi / anti；
- NULL build/probe key 不匹配；
- emit callback 可避免强制 materialize 大结果。

当前 join 是 operator foundation，还没有接成 logical Query/physical DAG；dictionary/composite join 应复用 `KeyCodec`/radix ownership 扩展边界，而不是把所有 key 都塞进一个 generic map。

## 9. 当前明确限制

- group key 暂不支持 nullable（NULL-group key encoding 留到 Phase 2.2）；
- SUM input 暂要求 non-null，后续需要 aggregate-state validity；
- plain string radix 尚未接入 engine；当前走 direct/bitmap table；
- cardinality 目前仍参与固定表容量 sizing，严重低估需要更成熟的 grow/rehash/runtime feedback；
- join 尚未接 logical plan；
- pqarrow RecordBatch / compressed Parquet production integration 未完成；
- spill 尚未实现。

这些限制都是接口已预留、但不能在本阶段假装完成的工作。
