# PAC Functions

This document describes the scalar and aggregate functions introduced by the PAC extension. These functions implement the SIMD-PAC-DB approach: a single query execution with 64 stochastic counters that implicitly capture m=64 sub-samples.

## pac_hash

**Signature**: `pac_hash(UBIGINT) -> UBIGINT`

Converts a DuckDB hash value into a PAC hash with **exactly 32 bits set**. Each of the 64 bits in the output represents one "possible world" (sub-sample). A 1-bit at position j means the PU is included in sub-sample j; a 0-bit means it's excluded.

### How the bits get set

1. **Per-query shuffling**: the input hash is XOR'd with a query-specific hash derived from `pac_seed` and the active query ID. This reshuffles which PUs land in which sub-samples on every query, so the privacy budget is per-query rather than per-session.

2. **Repair to exactly 32 bits** (when `pac_hash_repair=true`, which is the default): the XOR'd value goes through up to 16 rounds of multiplicative hashing with a 64-bit prime that itself has 32 bits set. In each round:
   - Multiply by the prime and XOR with the prime to scramble bits
   - Check if the popcount (number of 1-bits) is too high or too low
   - If too high (>32): conditionally flip all bits (XOR with all-ones) to bring the popcount closer to 32
   - If the popcount is in the range [26, 38]: enter a targeted repair loop that flips individual bits (using bit positions extracted from the hash itself) until exactly 32 bits are set
   - If repair succeeds, return immediately; otherwise, the next round scrambles again

3. **Fallback**: if all 16 rounds fail to produce exactly 32 bits, return `0xAAAAAAAAAAAAAAAA` — the alternating `10101010...` pattern, which has exactly 32 bits set.

### Why exactly 32 bits

With exactly 32 set bits, each PU appears in exactly half (32/64) of the sub-samples. This guarantees:
- A **uniform MIA prior of 50%**: an adversary trying to infer whether a specific PU is in the dataset starts with a 50/50 guess, which is the optimal starting point for privacy
- **Maximum entropy**: the hash carries the most information about sub-sample membership
- **Stable variance**: the counters across sub-samples have well-behaved variance (no outlier counters from PUs appearing in too many or too few sub-samples)

### Query-specific hashing

The XOR with `query_hash` means that the same PU gets a completely different 64-bit pattern for each query. This is important because:
- The "secret" sub-sample index J (which counter gets returned) is derived from `query_hash`
- Multiple PAC aggregates in the same query share the same J for utility (consistent sub-sample)
- Different queries see different sub-samples, so the privacy budget applies per-query

**Settings**:
- `pac_seed`: base seed for query_hash derivation (default: 42, randomized per query unless `pac_mi=0`)
- `pac_hash_repair`: whether to repair hash to exactly 32 bits (default: true). When false, the raw XOR'd hash is returned as-is (approximately but not exactly 32 bits).

## pac_select

**Signature**: `pac_select(UBIGINT hash, LIST<BOOL>) -> UBIGINT`

Combines a PU hash with a boolean predicate evaluated across the 64 sub-samples. Converts the boolean list to a 64-bit mask and ANDs it with the hash.

**Use case**: in categorical queries, when an inner PAC aggregate produces 64 counter values and a comparison (e.g., `value > counters[j]`) produces 64 boolean results, `pac_select` combines this with the PU hash so that counter j is active only when both the sub-sample includes the row AND the predicate passes for sub-sample j.

### pac_select_\<cmp\> variants

Fused versions that combine comparison + mask application in one function call, avoiding the overhead of lambda expressions over lists:

- `pac_select_gt(hash, val, counters)` -> UBIGINT: mask where `val > counters[j]`
- `pac_select_gte(hash, val, counters)` -> UBIGINT
- `pac_select_lt(hash, val, counters)` -> UBIGINT
- `pac_select_lte(hash, val, counters)` -> UBIGINT
- `pac_select_eq(hash, val, counters)` -> UBIGINT
- `pac_select_neq(hash, val, counters)` -> UBIGINT

## pac_filter

**Signature**: `pac_filter(UBIGINT hash) -> BOOLEAN`

Probabilistic row filter at the outermost categorical query boundary. Returns true with probability `popcount(hash) / 64`.

Checks the bit at position `query_hash % 64`. If the bit is set, the row passes; otherwise it's filtered out. This implements the noised row selection for categorical queries (queries that compare against PAC aggregate results without their own aggregate).

**Overloads**:
- `pac_filter(UBIGINT hash)` — filter from pre-computed hash/mask
- `pac_filter(LIST<BOOL>)` — converts list to mask, then filters

**Settings**: `pac_mi` controls probabilistic vs deterministic mode. When `mi=0`, uses deterministic majority voting (passes if popcount > 32).

## Counters

All PAC aggregates internally maintain an array of 64 counters (`counter[0..63]`). For each input tuple with `key_hash`:
- Counter j is updated if bit j of `key_hash` is set
- Since `pac_hash` guarantees 32 bits set, each tuple contributes to exactly 32 of the 64 counters

At finalization:
1. A **counter selector** `J = counter_selector % 64` deterministically picks one counter (same within a query via `query_hash`)
2. **Noise variance** is computed from the variance across all 64 counters: `delta = Var(counters) / (2 * mi)`
3. **Gaussian noise** `N(0, sqrt(delta))` is added to `counter[J]`
4. Result is `correction * counter[J] + noise` (correction is typically 2x because each counter sees ~50% of tuples)

### _counters Variants

Each PAC aggregate has a `_counters` variant that returns the raw 64 counters as `LIST<FLOAT>` instead of a single noised scalar. Used by:
- **Top-K rewriter**: to defer noise until after TopN selection
- **Categorical rewriter**: to evaluate predicates against all 64 values

Examples: `pac_sum_counters`, `pac_count_counters`, `pac_avg_counters`, `pac_min_counters`, `pac_max_counters`

### pac_mean and pac_noised

- `pac_mean(LIST<FLOAT>) -> FLOAT`: computes mean of non-NULL list elements. Used by top-k pushdown for ordering by true aggregate value.
- `pac_noised(LIST<FLOAT>) -> FLOAT`: applies the noise mechanism to a counter list. Selects counter J, computes variance, adds Gaussian noise.

## PAC Aggregates

### pac_count

**Signature**: `pac_count(UBIGINT key_hash) -> BIGINT`

Stochastic COUNT: maintains 64 counters, each incremented when the corresponding bit in `key_hash` is set.

**SIMD optimization (SWAR)**: Instead of `for(i=0;i<64;i++) count[i] += (key_hash >> i) & 1`, uses SWAR (SIMD Within A Register):
- 8 `uint64_t` values hold 64 packed `uint8_t` counters
- Extracts 8 bits per iteration using `(key_hash >> i) & MASK` and adds with a single `uint64_t ADD`
- Flushes to `uint64_t[64]` every 255 updates (before byte overflow)

**Optimizations**:
- **Cascading**: 8-bit counters cascade into 64-bit totals (reduces from 64-bit lane to 8-bit lane for SIMD)
- **Buffering**: delays state allocation; buffers 4 values before flushing to aggregate state (reduces cache misses)
- **Lazy allocation**: counters are only allocated when the aggregate actually receives values

### pac_sum

**Signature**: `pac_sum(UBIGINT key_hash, value) -> numeric`

Stochastic SUM. For each `(key_hash, value)` pair, adds `value * ((key_hash >> j) & 1)` to counter j (predication, not branching).

**Optimizations**:
- **Two-sided sum**: keeps separate positive and negative counter arrays. Finalizes as `2 * (pos[j] - neg[j])`. Prevents cancellation when summing mixed-sign data.
- **Approximate counters**: uses 16-bit counters in 25 lazily allocated levels that cascade every 4 values. Worst-case relative error ~0.024% — negligible compared to PAC noise.
- **Cascading + SWAR**: similar to pac_count, thin 16-bit SIMD lanes
- **Buffering**: batches 4 updates before flushing

### pac_avg

**Signature**: `pac_avg(UBIGINT key_hash, value) -> DOUBLE`

Maintains both sum and count counters. Finalizes as `sum[j] / count[j]` for each counter, then noises.

Supports DECIMAL inputs: divides by `10^scale` in finalization for correct decimal positioning.

### pac_min / pac_max

**Signature**: `pac_min(UBIGINT key_hash, value) -> type` / `pac_max(UBIGINT key_hash, value) -> type`

Maintains 64 extreme values. For each `(key_hash, value)`: `extremes[j] = min/max(extremes[j], value)` if bit j is set. Uses predication: `value * bit + extreme * (1 - bit)` then min/max.

**Pruning optimization**: maintains a running global bound (worst extreme across all 64 counters). Incoming values that can't improve any counter are skipped entirely. Bound is refreshed every 2048 updates.

**Buffering**: same delayed allocation as other aggregates.

### pac_count_distinct

**Signature**: `pac_count_distinct(UBIGINT key_hash, UBIGINT value_hash) -> BIGINT`

Tracks a hash map of `value_hash -> OR(key_hashes)`. In finalization, each distinct value contributes 1 to `counter[j]` if bit j of its accumulated key_hash is set. Uses a flat hash map for the distinct tracking.

### pac_sum_distinct / pac_avg_distinct

**Signature**: `pac_sum_distinct(UBIGINT key_hash, DOUBLE value) -> DOUBLE`

Tracks `bitcast(value) -> (OR(key_hashes), value)`. Each distinct value contributes its value to `counter[j]` if bit j is set. `pac_avg_distinct` divides each counter by its per-counter distinct count.

## pac_aggregate (Scalar)

**Signature**: `pac_aggregate(LIST<numeric> values, LIST<numeric> counts, DOUBLE mi, INTEGER k) -> DOUBLE`

A scalar function (not an aggregate) for applying the PAC noise mechanism to pre-computed per-world results. Takes:
- `values`: list of 64 aggregate results (one per possible world)
- `counts`: list of 64 counts (for NULL rejection: returns NULL if max count < k)
- `mi`: mutual information budget
- `k`: minimum count threshold

This is the function used by the original PAC-DB approach where m separate queries are executed. SIMD-PAC-DB's aggregate functions (`pac_sum`, `pac_count`, etc.) make this largely unnecessary but it remains available for manual use.

## Bit Operations Throughout the System

This section documents every significant bit-level operation used in the PAC extension and why it exists.

### Seed and Query Hash Derivation

Every query gets a unique `query_hash` derived from the seed:
```
seed ^= PAC_MAGIC_HASH * active_query_id   (skip if mi=0, deterministic mode)
query_hash = (seed * PAC_MAGIC_HASH) ^ PAC_MAGIC_HASH
```
- `PAC_MAGIC_HASH` is a 64-bit constant (`2983746509182734091`). Multiplying by it and XOR'ing scrambles the bits to produce a pseudo-random query_hash.
- `active_query_id` is DuckDB's internal query counter, so each query in a session gets a different hash.
- When `mi=0` (deterministic/no-noise mode), the seed is not randomized — queries are reproducible.

### pac_hash: Input XOR and Repair

```
xored = input_hash ^ query_hash
result = repair_to_32_bits(xored)
```
- **XOR with query_hash**: every PU gets a different bit pattern per query. This is what makes sub-sample membership query-specific (the 64 "possible worlds" are reshuffled).
- **Repair**: multiplicative hashing (`prime * (num ^ prime)`) followed by conditional negation (`num ^= flip` where flip is all-1s if popcount < 32, all-0s otherwise) and targeted bit-setting. See the pac_hash section above for full details.

### Composite PK Hashing (XOR of Multiple Columns)

When a PU has a composite key (e.g., `PAC_KEY(col1, col2)`):
```
combined = hash(col1) XOR hash(col2) XOR ...
result = pac_hash(combined)
```
XOR is used because it's commutative, associative, and preserves randomness — if any input hash is random, the XOR result is random. The pac_hash repair step then ensures exactly 32 bits.

### Multiple PU Hashing (AND of Hashes)

When a query involves multiple privacy units (e.g., both `customer` and `supplier`):
```
final_hash = hash_pu1 AND hash_pu2
```
Bitwise AND means counter j is active only if the row's PU is included in sub-sample j for **both** privacy units. This is the strictest intersection — the row contributes only to sub-samples where all PUs agree.

### Counter Updates: Bit Extraction

In every PAC aggregate, the update loop extracts individual bits from `key_hash`:
```
for j in 0..63:
    counter[j] += (key_hash >> j) & 1    // 1 if bit j is set, 0 otherwise
```
For SUM: `counter[j] += value * ((key_hash >> j) & 1)` — this is predication (multiply by 0 or 1) rather than branching (`if (bit) counter += value`), which is critical for SIMD auto-vectorization.

For COUNT with SWAR: instead of extracting one bit at a time, 8 bits are extracted simultaneously:
```
for i in 0..7:
    packed_counters[i] += (key_hash >> i) & 0x0101010101010101
```
The mask `0x0101010101010101` selects the lowest bit of each byte, giving 8 independent 8-bit counters packed in one 64-bit integer.

### Accumulator OR (NULL Tracking)

Every aggregate maintains a `key_hash` accumulator that tracks which sub-samples have been seen:
```
state.key_hash |= row_key_hash    // on every update
```
At finalization, `~state.key_hash` gives a bitmask where bit j = 1 means sub-sample j never received any update. These "unseen" sub-samples produce NULL counters. The `popcount(~key_hash)` determines the NULL probability.

### bit_or Aggregate (Correlated Subqueries)

When the compiler needs to propagate a hash through a nested aggregate (inner aggregate groups by something, outer aggregate needs the hash), it uses `bit_or(key_hash)` with an IS NOT NULL filter:
```
bit_or(key_hash) FILTER (WHERE some_col IS NOT NULL)
```
`bit_or` OR's all key_hashes within a group, producing the union of all sub-sample memberships. This is used when the inner aggregate's grouping key corresponds to the PU key (e.g., TPC-H Q13 where inner aggregates group by customer).

### Counter Selection (Choosing J)

At finalization, one counter is selected:
```
J = counter_selector % 64
```
where `counter_selector` is derived from `query_hash`. All aggregates in the same query pick the same J (consistent sub-sample). The `~key_hash` is used as the `is_null` bitmask: counters where the bit is 0 in the accumulator are treated as 0.

### pac_select: AND of Hash and Predicate Mask

```
result = hash & predicate_mask
```
Where `predicate_mask` has bit j = 1 if the boolean predicate passes for sub-sample j. The AND ensures counter j is active only when BOTH the PU is in sub-sample j AND the predicate holds for sub-sample j. This is the core mechanism for categorical queries.

### pac_filter: Popcount-Based Decision

```
bit_pos = query_hash % 64
return (hash >> bit_pos) & 1
```
Checks a single bit position determined by the query hash. Since pac_hash ensures 32 bits set, this gives a ~50% base rate. After pac_select applies a predicate mask, the popcount may be less than 32, reducing the probability of returning true.

### NOT Handling (Negation)

When a predicate is negated (e.g., `NOT (value > threshold)`), the mask is bitwise-negated:
```
negated_mask = ~mask
```
This flips all 64 bits, turning "passes for sub-sample j" into "fails for sub-sample j".

## P-Tracking (Persistent Secret Composition)

All PAC aggregates share a per-query `PacPState` that tracks a Bayesian posterior probability distribution over the 64 worlds. When `pac_ptracking=true` (default):

1. **Before noising**: compute p-weighted variance `w_var = sum(p[k] * (val[k] - w_mean)^2)`
2. **Noise variance**: `noise_var = w_var / (2 * mi)` — adapts to the current posterior
3. **After noising**: Bayesian update — compute log-likelihood for each world, log-sum-exp normalize

This ensures that noise calibration correctly composes information leaked by all cells in the same query, implementing the adaptive noise mechanism from the PAC-DB framework.
