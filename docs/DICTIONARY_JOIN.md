# Dictionary string join

## 1. Correctness contract

Dictionary code 不是全局字符串 ID。

```text
Domain A: code 7 = "SG"
Domain B: code 7 = "US"
```

因此只有稳定 domain 相同时才能直接比较 code。

## 2. Build

`join.DictionaryTable.Build` 在 dictionary cardinality 上建立 canonical directory，并在 row-level build 阶段保存 duplicate row chain。

NULL build key 被跳过，因为 SQL equality join 中 NULL 不与任何值匹配。

## 3. Probe — same domain

相同 domain 时只构造 code translation，热路径：

```text
probe code -> canonical integer -> hash directory
```

## 4. Probe — different domain

不同 domain 时：

```text
probe dictionary value
       -> build value directory lookup
       -> translation[probe code]
```

这一步最多执行 probe dictionary cardinality 次，而不是 probe rows 次。

随后 row hot loop只：

```text
code -> translation -> integer lookup
```

## 5. Duplicate values

一个 dictionary 自身可能包含重复 bytes。Build canonicalization 会把重复 value 映射到同一 canonical code，因此 row equality 仍按 string semantic value，而不是物理 dictionary position。

## 6. 当前边界

已支持：inner、left、semi、anti、duplicate build keys、NULL no-match、cross-domain normalize。

尚未实现：

- 128-bit composite dictionary join；
- radix partitioned dictionary join；
- bounded output batch operator；
- generic engine logical Join node；
- spillable build side。
