# Runtime Checks

This document describes the runtime safety checks performed by PAC aggregates during execution. These checks ensure that PAC noise provides meaningful privacy guarantees and that degenerate inputs don't silently produce unsafe outputs.

## Sample Diversity Check

**Problem**: If a GROUP BY key is strongly correlated with (or equal to) the PU key, then each group contains tuples from a single PU. In this case, all 64 counters receive updates from the same `key_hash`, so there is no variance across sub-samples — PAC noising produces either NULL or the exact answer (no privacy).

**Detection**: All PAC aggregates maintain a 64-bit **accumulator** that gets OR'ed with each incoming `key_hash` on every update (`accumulator |= key_hash`). At finalization, the zero bits in this accumulator indicate which sub-samples never received an update.

A group is classified as **suspicious** when:
1. The zero-bit count is between 29 and 35 (close to the expected 32 for a single PU's hash, which has exactly 32 zero bits after repair)
2. All active (non-zero) counters have the **exact same value** (no variance across sub-samples)

Both conditions together strongly suggest the group was populated by a single PU — if only one PU contributes, all 32 active counters get the same updates, and the zero-bit pattern matches a single hash.

**Population check**: Per-group classifications are accumulated across all finalized groups:
- `suspicious_count`: groups matching both criteria above
- `nonsuspicious_count`: all other groups
- `total_update_count`: sum of all updates across groups

When `total_update_count >= 500` and `suspicious_count > 2 * nonsuspicious_count`, the aggregate throws:

```
<aggregate_name> detected absence of sample diversity -- which clearly is privacy unsafe
```

**Rationale**: With hash repair enabled, single-PU groups always have exactly 32 zero bits (in the [29,35] range). The 2:1 ratio threshold handles small datasets where a few suspicious groups are expected by chance. The threshold of 500 total updates prevents false positives on tiny test datasets.

**Note**: This check is a safety net. The compiler should reject such queries at compile time (by checking if GROUP BY keys contain PU keys). The runtime check catches cases that slip through the static analysis.

## NULL Handling

PAC aggregates use a probabilistic NULL mechanism to handle groups with insufficient sample diversity.

### NULL Probability

At finalization, each group's result may be set to NULL based on how many sub-samples saw data:

**Probabilistic mode** (`mi > 0`):
- `P(NULL) = popcount(~key_hash) / (64 * correction)`
- The more zero-bits in the accumulator (sub-samples that never saw data), the higher the NULL probability
- Uses RNG for the decision: `NULL if popcount(~key_hash) > random_value % (64 * correction)`

**Deterministic mode** (`mi <= 0`):
- `NULL when popcount(key_hash) * correction < 1`
- Groups with zero or near-zero coverage are always NULL

The `correction` parameter reduces NULL probability — a correction of 2.0 halves the NULL rate (used when the aggregate result is multiplied by 2, e.g., because each counter sees ~50% of tuples).

### NULL and Counters

During aggregation, the 64-bit accumulator (`key_hash |= row_key_hash`) tracks which sub-samples have contributed at least one row. At finalization:

- Bits that are 0 in the accumulator indicate worlds that never saw any tuple for this group
- The `is_null` bitmask used by the noise mechanism is `~key_hash` — counters for unseen sub-samples are treated as 0

This naturally scales with data sparsity: sparse groups (few contributing PUs) have more NULL counters, wider variance, and higher NULL probability.

### pac_coalesce

**Signature**: `pac_coalesce(LIST<FLOAT>) -> LIST<FLOAT>`

If the input list is NULL (e.g., from a LEFT JOIN where no matching rows exist), returns a list of 64 NULLs instead. This prevents downstream operations from crashing on NULL list inputs.

## Utility NULLing

**Problem**: PAC noise can dominate the true aggregate value for small or sparse groups, producing results that are technically private but practically useless (e.g., a true sum of 5 reported as 2000 due to noise). Returning such low-utility cells can mislead analysts.

**Mechanism**: When `privacy_min_group_count` is set to a numeric value (e.g., `SET privacy_min_group_count = 4`), PAC post-processes each noised result cell by computing its z-score:

```
z = |noised_value| / noise_std_dev
```

where `noise_std_dev = sqrt(noise_variance)` is the standard deviation of the noise that was added. Cells with low z-scores (signal dominated by noise) are probabilistically NULLed using a smooth sigmoid:

```
P(keep) = 1 / (1 + exp(-steepness * (z - threshold)))
```

with `steepness = 3`. At the threshold z-score, P(keep) ≈ 50%. Well above threshold, cells are almost always kept; well below, almost always NULLed.

**Variance scaling**: For aggregates that use 2x compensation (SUM, COUNT — which double the result to account for ~50% sampling), the noise variance is scaled by 4x (since `Var(2X) = 4·Var(X)`). MIN/MAX use the raw noise variance (no compensation).

**Privacy safety**: This is safe post-processing of the already-noised output. By the data processing inequality, any function of a differentially private output is at least as private. The sigmoid decision uses independent random coins (from the same seeded RNG stream), not the underlying data.

**Scope**: Utility NULLing applies to all PAC aggregate finalize paths:
- `pac_noised_sum`, `pac_noised_count`, `pac_noised_min`, `pac_noised_max`
- `pac_noised_clip_sum`, `pac_noised_clip_min`, `pac_noised_clip_max` (when `pac_clip_support` is also active)
- Categorical terminals: `pac_noised`, `pac_noised_div`

**Configuration**:
```sql
SET privacy_min_group_count = 4;     -- enable: z < 4 → likely NULLed (~20% relative error cutoff)
SET privacy_min_group_count = NULL;  -- disable (default)
```

**Default**: Disabled (`NULL`). Must be explicitly enabled.

## Stability

### Hash Repair (pac_hash_repair)

When `pac_hash_repair=true` (default), `pac_hash` repairs its output to have **exactly 32 bits set**. Without repair, DuckDB's internal hash function produces hashes with approximately but not exactly 32 bits. The repair ensures:

1. **Uniform MIA prior**: every PU appears in exactly half the sub-samples
2. **Stable noise calibration**: the variance across counters is well-behaved (no outlier counters from PUs that appear in too many or too few sub-samples)
3. **Deterministic diversity**: the diversity check can rely on exact bit counts

### Bound Pruning Stability (MIN/MAX)

`pac_min` and `pac_max` maintain a global bound `g` (worst extreme across all 64 counters):
- For MAX: `g = min_j(max_value[j])` — the minimum of all maximums
- For MIN: `g = max_j(min_value[j])` — the maximum of all minimums

Incoming values worse than `g` are skipped entirely. The bound is recomputed every `BOUND_RECOMPUTE_INTERVAL = 2048` updates to keep it fresh.

This optimization works on all distributions **except** monotonically increasing (for MAX) or decreasing (for MIN) data, which are adversarial since the aggregate changes on every update. On random data, pruning achieves ~3x speedup over unpruned.

### Two-Sided Sum Stability

`pac_sum` keeps separate positive and negative counter arrays. This prevents cancellation when summing mixed-sign data:

- Without two-sided: positive and negative values cancel within each counter, collapsing totals to near-zero, destroying variance, and inflating `z^2` to ~210 (unusable)
- With two-sided: `result[j] = 2 * (pos[j] - neg[j])`, the counter hierarchy preserves natural spread. `z^2` normalizes to ~0.004, `var_ratio` to ~1.0

Columns with signed types (e.g., DECIMAL) that only contain positive values still benefit because the negative side is lazily allocated and stays NULL, giving unsigned performance as a bonus.