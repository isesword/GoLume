# Adaptive hash tables and NULL semantics

## 1. 为什么 cardinality estimate 不能决定 correctness

查询优化器只能估算 distinct count。真实生产数据会遇到 stale stats、skew、predicate correlation 和突发新值。如果 hash table 只按 estimate 分配且永不 grow，结果不是简单“慢一点”，而可能：

- probe 永不找到 empty slot；
- panic；
- 数据丢失；
- 强制非常保守地过度分配内存。

Phase 2.2 的原则是：

> estimate 决定第一笔 allocation；observed cardinality 决定后续 grow。

## 2. Open addressing grow

当前表保持 power-of-two capacity 和线性 probe。达到 load factor 前不会 grow：

```text
capacity = power of two
mask = capacity - 1
maxEntries ~= capacity * 0.70
```

Grow：

1. 计算新 capacity = 2x；
2. 先通过 Reserve callback 申请增量 retained bytes；
3. allocation；
4. 只 rehash current generation；
5. 原子地替换 table fields；
6. 继续原 insert。

如果第 2 步失败，旧 table 保持可用，错误返回 Engine。

## 3. Prepared workspace accounting

PreparedQuery 的 retained memory 是长期 reservation。Grow 后增加的 capacity 也保持 reservation，直到 PreparedQuery Close。

因此：

```text
memory.Manager.Used()
```

反映真实 retained workspace，而不是“逻辑执行结束以后假装释放”。

## 4. NULL as a logical key

单列 NULL 不存入 slot directory，使用 dedicated state。这样避免 sentinel collision。

Composite key 使用 validity mask。无效 payload canonicalize 的原因是：

```text
(NULL payload=123, 1)
(NULL payload=999, 1)
```

必须认为是同一个 key。

## 5. Nullable pair radix

Pair radix workspace 将每个 compact row 的两位 validity mask 与 `Key128` 一起 scatter。Partition-local table 以 `(A, B, ValidityMask)` 为完整 identity，所以 NULL payload canonicalization 在 radix 前后保持一致。

## 6. Radix local table

Radix partition local table 的目标是小而 cache-local，因此不直接 adaptive grow。错误估计走 coarse fallback：

```text
ErrLocalTableFull -> adaptive fallback table
```

长期路线是 planner/runtime feedback 调整 radix bits 和 local cardinality，而不是在最内层 partition table 引入复杂 grow。
