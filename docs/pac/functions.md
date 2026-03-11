# PAC Functions

Scalar and aggregate functions introduced by the PAC extension. These implement the SIMD-PAC-DB approach: a single query execution maintaining 64 stochastic counters that implicitly capture 64 sub-samples.

Under normal usage, standard SQL aggregates (`SUM`, `COUNT`, etc.) are automatically rewritten into the PAC functions below. They may also be called directly.

## Aggregates

### pac_count

`pac_count(UBIGINT key_hash) → BIGINT`

Stochastic COUNT. Maintains 64 counters, each incremented when the corresponding bit in `key_hash` is set. Internally uses SWAR (SIMD Within A Register): 8 packed `uint8_t` counters per `uint64_t` lane, cascading into 64-bit totals every 255 updates to avoid overflow. State allocation is deferred and values are buffered in groups of 4 to reduce cache misses.

```sql
SELECT department, pac_count(pac_hash(id)) FROM employees GROUP BY department;
```

### pac_sum

`pac_sum(UBIGINT key_hash, value) → numeric`

Stochastic SUM. Adds `value` to counter `j` when bit `j` of `key_hash` is set, using predication rather than branching. Internally maintains separate positive and negative counter arrays (two-sided sum) to prevent cancellation with mixed-sign data. Uses approximate 16-bit counters across 25 lazily allocated cascading levels, yielding a worst-case relative error of ~0.024% — negligible compared to PAC noise.

```sql
SELECT pac_sum(pac_hash(id), salary) FROM employees;
```

### pac_avg

`pac_avg(UBIGINT key_hash, value) → DOUBLE`

Stochastic AVG. Maintains both sum and count counters; finalizes as `sum[j] / count[j]`. For DECIMAL inputs, the result is divided by `10^scale` during finalization to preserve correct decimal positioning.

```sql
SELECT department, pac_avg(pac_hash(id), salary)::INTEGER FROM employees GROUP BY department;
```

### pac_min / pac_max

`pac_min(UBIGINT key_hash, value) → type`
`pac_max(UBIGINT key_hash, value) → type`

Stochastic MIN and MAX. Maintains 64 extreme values per counter, updated via predication: `value * bit + extreme * (1 - bit)` followed by min/max. A pruning optimization tracks a running global bound (the worst extreme across all 64 counters) and skips incoming values that cannot improve any counter; the bound is refreshed every 2048 updates.

```sql
SELECT pac_min(pac_hash(id), salary), pac_max(pac_hash(id), salary) FROM employees;
```

## Counter Variants

Each aggregate has a `_counters` variant that returns all 64 raw counters as `LIST<FLOAT>` instead of a single noised scalar. These are used internally by the top-K and categorical rewriters to defer noise application until after selection.

Available variants: `pac_sum_counters`, `pac_count_counters`, `pac_avg_counters`, `pac_min_counters`, `pac_max_counters`.

```sql
SELECT pac_sum_counters(pac_hash(id), salary) FROM employees;
```

## Scalar Functions

### pac_hash

`pac_hash(UBIGINT) → UBIGINT`

Converts a DuckDB hash value into a PAC hash with **exactly 32 bits set**. Each bit represents one sub-sample: bit `j = 1` indicates the privacy unit is included in sub-sample `j`. The constraint of exactly 32 set bits guarantees a uniform 50% MIA prior and maximum entropy across sub-samples.

```sql
SELECT pac_hash(hash(id)) FROM employees;
```

The input hash is XOR'd with a per-query hash derived from `pac_seed`, so the same privacy unit receives different sub-sample assignments on each query. The repair procedure applies up to 16 rounds of multiplicative hashing with a 64-bit prime (itself having 32 bits set), flipping bits until exactly 32 are set; if all rounds fail, returns `0xAAAAAAAAAAAAAAAA` as a fallback. Setting `pac_hash_repair` (default: `true`) controls whether this repair is applied.

### pac_select

`pac_select(UBIGINT hash, LIST<BOOL>) → UBIGINT`

Combines a privacy unit hash with a per-sub-sample boolean predicate by converting the boolean list to a 64-bit mask and ANDing it with the hash. This ensures counter `j` is active only when both the sub-sample includes the row and the predicate holds for sub-sample `j`. Used by the categorical rewriter when comparisons are evaluated against PAC aggregate results.

```sql
SELECT pac_select(pac_hash(id), list_transform(counters, x -> x > 100)) FROM employees;
```

Fused comparison variants avoid lambda overhead:
`pac_select_gt`, `pac_select_gte`, `pac_select_lt`, `pac_select_lte`, `pac_select_eq`, `pac_select_neq` — each takes `(hash, val, counters)` and returns the masked hash.

### pac_filter

`pac_filter(UBIGINT hash) → BOOLEAN`

Probabilistic row filter at the outermost categorical query boundary. Checks the bit at position `query_hash % 64`; the row passes if the bit is set. This implements noised row selection for queries that compare against PAC aggregate results without performing their own aggregation. When `pac_mi = 0`, deterministic majority voting is used instead (passes if popcount > 32).

```sql
SELECT * FROM (SELECT department, pac_hash(hash(id)) AS h FROM employees) WHERE pac_filter(h);
```

### pac_mean / pac_noisedis the deter

`pac_mean(LIST<FLOAT>) → FLOAT` — Computes the mean of non-NULL list elements. Used internally for ordering by the true aggregate value in top-K queries.

`pac_noised(LIST<FLOAT>) → FLOAT` — Applies the PAC noise mechanism to a counter list: selects counter `J`, computes variance across all counters, and adds Gaussian noise.

```sql
SELECT pac_noised(pac_sum_counters(pac_hash(id), salary)) FROM employees;
```