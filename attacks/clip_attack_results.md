# pac_clip_sum Attack Evaluation

Evaluates whether `pac_clip_sum`'s support-based outlier attenuation defeats the
variance side-channel membership inference attack (MIA).

## Background

`pac_clip_sum` (commit 948a504) introduces a two-level aggregation:

1. **Lower aggregate**: `GROUP BY pu_hash` with plain `SUM` — pre-aggregates per user
2. **Upper aggregate**: `pac_noised_clip_sum` — decomposes values into magnitude levels
   (each 16x the previous), tracks a 64-bit bitmap of distinct contributors per level,
   and attenuates levels with fewer contributors than `pac_clip_support`

Supported clip aggregates: SUM, COUNT, MIN, MAX (MIN/MAX not fully implemented).
float/double types not yet supported.

## Parameters

- `pac_mi = 0.0078125` (1/128)
- 30 trials per condition unless noted
- Random guess baseline: 50%

---

## Attack 1: Single-query variance classifier (small filter)

**Setup**: N=1000 users, target=999999, filter<=3 (3-4 users in aggregation)

| clip_support | Var>200k accuracy | Standard accuracy | std_in | std_out | std ratio |
|-------------|-------------------|-------------------|--------|---------|-----------|
| off | **96.0%** | 80.0% | 10,688,008 | 117,662 | 90.8x |
| 2 | **72.5%** | 62.7% | 613,511 | 122,026 | 5.0x |
| 3 | **72.5%** | 62.7% | 613,511 | 122,026 | 5.0x |

**Finding**: Clipping reduces attack accuracy from 96% to 72.5% and collapses the
variance ratio from 91x to 5x. The outlier's contribution is attenuated by ~16x
(one magnitude level). However, 72.5% is still well above random — the residual
5x variance gap remains exploitable.

clip=2 and clip=3 produce identical results because with only 3-4 users per level,
the bitmap support is at most 2-3 regardless of threshold.

---

## Attack 2: Wide filter (clipping best-case)

**Setup**: N=1000 users, target=999999, filter<=999 (all users in aggregation)

| clip_support | Var>200k accuracy | Standard accuracy | std_in | std_out | std ratio |
|-------------|-------------------|-------------------|--------|---------|-----------|
| off | 53.3% | 60.0% | 10,424,631 | 1,822,936 | 5.7x |
| 2 | **55.0%** | 53.3% | 1,816,604 | 1,875,047 | **1.0x** |
| 5 | **55.0%** | 53.3% | 1,816,604 | 1,875,047 | **1.0x** |
| 10 | **55.0%** | 53.3% | 1,816,604 | 1,875,047 | **1.0x** |

**Finding**: With many users in the aggregation, clipping completely eliminates the
variance side-channel (std ratio goes to 1.0x). Attack accuracy drops to ~53-55%, near
random. The outlier's magnitude level has only 1 bitmap contributor vs hundreds at
normal levels — it is cleanly identified and attenuated.

Note: even without clipping, the attack barely works (53%) because the outlier
signal (1M) is diluted by the large background (5M from 999 users). Clipping
further equalizes the means (5.1M vs 4.9M).

clip=2, 5, and 10 all produce identical results — with ~1000 users, all normal
levels have saturated bitmaps (est. distinct >> 10), so only the outlier level
is affected.

---

## Attack 3: 10K users, extreme outlier

**Setup**: N=10000 users, target=9,999,999, filter<=2 (2-3 users in aggregation)

| clip_support | Var>200k accuracy | Standard accuracy | std_in | std_out | std ratio |
|-------------|-------------------|-------------------|--------|---------|-----------|
| off | **97.8%** | 77.8% | 104,410,859 | 110,259 | 947x |
| 2 | **75.0%** | 47.9% | 396,383 | 112,778 | 3.5x |
| 3 | **47.9%** | 47.9% | 0 | 0 | — |

**Finding**: clip=2 reduces accuracy from 97.8% to 75% (variance ratio 947x to 3.5x).
clip=3 zeroes ALL results (returns 0 for both in and out) — with only 2-3 users,
no level reaches 3 distinct contributors, so everything is zeroed. The attack is
"defeated" at 47.9% but utility is completely destroyed.

---

## Attack 4: Over-clipping

**Setup**: N=1000 users, target=999999, filter<=3, 15 trials

| clip_support | Var>200k accuracy | mean_in | mean_out |
|-------------|-------------------|---------|----------|
| off | 91.3% | -2,067,562 | 42,892 |
| 5 | **50.0%** | 0 | 0 |
| 10 | **50.0%** | 0 | 0 |

**Finding**: With only 3-4 users in the filter, clip_support >= 5 zeroes all output.
Attack accuracy = 50% (random), but every query returns 0. This is not a useful
defense — it's equivalent to refusing to answer.

**Takeaway**: `pac_clip_support` must be set below the minimum expected number of
users in any aggregation group. For small filters, this severely limits the
clipping threshold.

---

## Attack 5: Wide filter + aggressive clipping

**Setup**: N=1000 users, target=999999, filter<=999, 15 trials

| clip_support | Var>200k accuracy | Standard accuracy | std_in | std_out |
|-------------|-------------------|-------------------|--------|---------|
| off | 53.3% | 63.3% | 11,391,246 | 2,172,216 |
| 50 | **50.0%** | 50.0% | 1,219,189 | 2,049,795 |
| 100 | **50.0%** | 50.0% | 1,219,189 | 2,049,795 |

**Finding**: With a wide filter (1000 users), even aggressive clipping (support=50, 100)
works perfectly — attack accuracy = 50% (random) and noise stds are equalized.
Normal magnitude levels have hundreds of bitmap contributors, far exceeding the
threshold. Only the outlier level (1 contributor) is affected.

---

## Attack 6: Clip-after-filter vs clip-full-table (Dandan's hypothesis)

Dandan's concern: clipping applied after filtering may leak more than clipping
applied to the entire dataset, because the filter changes which users contribute
to the bitmap, affecting which levels appear "supported."

**Setup**: N=1000 users, target=999999, filter<=3

| Method | Var>200k accuracy | std_in | std_out | std ratio |
|--------|-------------------|--------|---------|-----------|
| No clipping | **96.0%** | 10,688,008 | 117,662 | 90.8x |
| clip-after-filter (pac_clip_support=2) | **72.5%** | 613,511 | 122,026 | 5.0x |
| clip-full-table (pre-clip to mu+3sigma) | **56.9%** | 180,457 | 124,641 | 1.4x |

**Finding: Dandan is correct.** Pre-clipping the full table then filtering gives
significantly better protection (56.9% vs 72.5%). The reasons:

1. **Full-table pre-clipping** clamps the billionaire to 13,661 BEFORE PAC sees it.
   PAC computes noise from the clamped range [1, 13661], and the noise is nearly
   the same for in/out (std ratio 1.4x).

2. **Clip-after-filter** only sees 3-4 rows. The bitmap has very few bits set per
   level, making it harder to distinguish the outlier level from normal levels.
   The attenuation is only ~16x (one level), leaving a 5x variance gap.

However, clip-after-filter is still much better than no clipping (72.5% vs 96%),
confirming Dandan's second point: "this approach is still significantly better
than not applying clipping at all."

---

## Summary

| Scenario | No clip | pac_clip_support=2 | Pre-clip full table |
|----------|---------|-------------------|---------------------|
| Small filter (3-4 users) | 96% | 72.5% | 56.9% |
| Wide filter (1000 users) | 53% | 55% (side-channel gone) | — |
| 10K users, filter<=2 | 97.8% | 75% | — |

## Attack 7: 20K small items user (multi-row outlier)

The core argument for per-user pre-aggregation: a user with 20,000 purchases of
$50 each has normal individual values but a total contribution of $1,000,000.
Per-row Winsorization won't catch this. Does pac_clip_sum's GROUP BY pu_hash?

**Setup**: N=1000 background users (1 row each, acctbal in [1,10000]).
Target user_id=0: 20,000 rows x $50 = $1,000,000 total. filter<=3.

| Method | Var>200k accuracy | std_in | std_out | std ratio |
|--------|-------------------|--------|---------|-----------|
| No clipping | **96.0%** | 10,686,722 | 117,662 | 90.8x |
| Winsorization (per-row clip to 13661) | **94.1%** | 9,436,439 | 124,641 | 75.7x |
| pac_clip_sum (clip_support=2) | **72.5%** | 613,511 | 122,026 | 5.0x |

**Finding: Winsorization completely fails.** Each $50 value is well within the
[0, 13661] clip bounds, so nothing gets clipped. The 20,000 small rows pass
through untouched and the attack succeeds at 94.1% (barely below the 96% baseline).

**pac_clip_sum catches it** because the pre-aggregation step (`GROUP BY pu_hash`)
sums user_id=0's 20,000 rows into a single $1,000,000 entry. This lands at an
outlier magnitude level with only 1 bitmap contributor, and gets attenuated.
Attack accuracy drops to 72.5%.

This is precisely Peter's argument for why per-user contribution clipping (via
pre-aggregation) is needed instead of per-row value clipping. It validates the
two-level aggregation design from Wilson et al. 2019.

---

## Suffix attenuation modes compared

We tested three suffix attenuation strategies for unsupported outlier levels.
"Soft-clamp" is Peter's original (scale by 16^distance). "Bitmap-proportional"
adds a factor of estimated_distinct/threshold. "Hard-zero" skips the level entirely.

**Attack 1 results (N=1000, filter<=3, clip=2):**

| Mode | Var>200k | std_in | std_out | std ratio |
|------|----------|--------|---------|-----------|
| No clipping | **96.0%** | 10,688,008 | 117,662 | 90.8x |
| Soft-clamp | **72.5%** | 613,511 | 122,026 | 5.0x |
| Bitmap-proportional | **66.7%** | 327,625 | 122,026 | 2.7x |
| **Hard zero** | **47.1%** | 106,223 | 122,026 | **0.87x** |

---

## Hard-zero stress tests

Comprehensive adversarial evaluation of the hard-zero mode.

### TEST 1: High trial count (60 trials, best-threshold search)

| truth | mean | std | n |
|-------|------|-----|---|
| in | 12,397 | 87,366 | 49 |
| out | 29,758 | 109,140 | 54 |

Best threshold accuracy (searched 10k-500k in 10k steps): **52.4%**.
Midpoint classifier: **52.4%**. Likelihood ratio: **52.4%**.
All classifiers are indistinguishable from random.

### TEST 2: Composed queries (30 trials x 10 queries)

| n_queries | accuracy |
|-----------|----------|
| 1 | 43.4% |
| 5 | 50.9% |
| 10 | 48.1% |
| Majority vote | **50.0%** |

Per-trial variance: in_std=83,747, out_std=85,116, **ratio=0.98**.
Composing 10 queries and averaging does not help the attacker.

### TEST 3: Moderate outlier (target=50,000, same magnitude level as normal)

**THIS BREAKS IT.** Target 50,000 is in level 2 (4096-65535), same as normal users.
The bitmap shows this level as supported → no clipping occurs.

| truth | mean | std |
|-------|------|-----|
| in | 139,946 | 497,518 |
| out | 20,093 | 122,026 |

Best threshold accuracy: **76.5%**. Std ratio: 4.1x.

**Implication**: pac_clip_sum only clips outliers that are at a DIFFERENT magnitude
level than normal users. A 10x outlier within the same level passes through.

### TEST 4: Two colluding outliers

Two users with 999,999 — level 3 has 2 bitmap bits, meeting threshold=2.

| truth | mean | std |
|-------|------|-----|
| in | 1,783,465 | 12,547,986 |
| out | 20,093 | 122,026 |

Best threshold accuracy: **100.0%**. Attack fully succeeds.

**Implication**: Two colluding users at the same magnitude level make that level
"supported." The clipping mechanism assumes outlier levels have few contributors.
Collusion (or any scenario with 2+ users at the same extreme level) defeats it.

### TEST 5: Dandan's filter probing

Attacker uses two queries with different filters to probe clipping behavior.

| Query | Best accuracy |
|-------|--------------|
| Filter<=3 (narrow) | 52.9% |
| Filter<=999 (wide) | 51.7% |
| Cross-filter differential | **51.0%** |

**Dandan's concern is NOT exploitable with hard-zero.** The narrow query zeroes the
outlier level, giving identical counter distributions for in/out. The wide query
has the outlier's level zeroed too (1 contributor < threshold). The cross-filter
differential reveals nothing.

### TEST 6: 20K small items ($50 x 20,000)

Best threshold accuracy: **52.9%**. Attack defeated.
Pre-aggregation collapses 20K rows into one $1M entry at level 3, which is zeroed.

### TEST 7: Borderline outlier (target=65,536, exactly level 3 boundary)

Best threshold accuracy: **52.9%**. Attack defeated.
Even the minimum level-3 value is zeroed when it's the sole contributor.

---

### Key takeaways

1. **Hard-zero fully defeats the variance side-channel** for outliers at unsupported
   magnitude levels. Attack accuracy = 50% across all classifiers, even with
   composed queries, different thresholds, and cross-filter probing.

2. **Moderate outliers within the same magnitude level are NOT caught.** A 10x outlier
   (50,000 vs normal ~5,000) sits in the same level and passes through unclipped.
   Attack accuracy: 76.5%. This is a fundamental limitation of the magnitude-level
   granularity (each level spans 16x).

3. **Two colluding outliers defeat the clipping** by making their level "supported"
   (2 contributors >= threshold 2). Attack accuracy: 100%.

4. **Dandan's filter-probing concern does not apply with hard-zero.** The zeroed level
   contributes nothing regardless of filter, so different filters reveal no info.

5. **The pre-aggregation step remains essential** — 20K small items are correctly
   collapsed and clipped.

---

## pac_clip_scale comparison (2026-04-02)

Tests `pac_clip_scale = true` (scale unsupported outlier levels to nearest supported
level) vs `false` (hard-zero / omit). Peter's hypothesis: scaling should be safe
because outliers become a minority in the already-supported bucket.

Code version: after Peter's refactoring into `pac_clip_aggr.hpp`, CLIP_LEVEL_SHIFT=2
(4x per level, was 16x previously).

### Setup

- N=1000 background users, acctbal ∈ [1, 10000]
- pac_mi = 0.0078125 (1/128), 30 trials per condition
- Background sum: filter<=3 = 18,347; filter<=999 = 4,871,091

### Small filter results (filter<=3, clip_support=2)

| Test | Outlier | scale=false best% | scale=true best% | scale=false std_in | scale=true std_in | std_out |
|------|---------|-------------------|------------------|--------------------|-------------------|---------|
| Extreme | tv=999,999 | **55.6%** | **64.8%** | 94,882 | 169,449 | 107,976 |
| Moderate | tv=50,000 | **55.6%** | **61.1%** | 94,882 | 147,406 | 107,976 |
| Multi-row | 20K×$50 | **55.6%** | **64.8%** | 94,882 | 169,449 | 107,976 |
| Borderline | tv=65,536 | **55.6%** | **57.4%** | 94,882 | 104,640 | 107,976 |

With **hard-zero** (scale=false): all outlier contributions are completely zeroed. The
"in" distribution is identical to "out" — attack accuracy ~55% (random). No side-channel.

With **scaling** (scale=true): outlier values are scaled down to the nearest supported
level. This creates a mild variance side-channel (std ratio ~1.57x). Attack accuracy
rises to 57-65% — measurable but not catastrophic.

### Small filter results (clip_support=10 and 50)

All conditions return 0 for both in/out (no level reaches 10 or 50 distinct
contributors with only 3-4 users). Scale mode is irrelevant — both modes identical.

### Wide filter results (filter<=999, clip_support=2)

| clip | scale | mean_in | std_in | mean_out | std_out | best% |
|------|-------|---------|--------|----------|---------|-------|
| 2 | false | 4,737,114 | 1,516,576 | 4,973,475 | 1,642,654 | 53.3% |
| 2 | true | 4,750,242 | 1,538,381 | 4,973,475 | 1,642,654 | 53.3% |
| 50 | false | 4,734,354 | 1,516,680 | 4,970,423 | 1,643,039 | 53.3% |
| 50 | true | 4,791,594 | 1,539,287 | 5,019,631 | 1,639,877 | 55.0% |

No meaningful difference. Both modes near random with wide filters.

### Key findings

1. **Peter is partially right**: scaling does NOT cause a variance explosion. The leak
   is moderate (~10 percentage points above random at worst), not catastrophic.
   His intuition that scaled values become a minority in the supported bucket holds.

2. **Hard-zero is still strictly better for privacy**: it produces zero information
   leakage in all tested scenarios. Scaling leaks mildly.

3. **Major improvement from 4x granularity**: the moderate outlier (tv=50,000) that
   previously defeated clipping at 76.5% accuracy (when levels were 16x wide, both
   5000 and 50000 in same level) is now caught by both modes (~55-61%). The shift
   from CLIP_LEVEL_SHIFT=4 (16x) to CLIP_LEVEL_SHIFT=2 (4x) dramatically improved
   detection of moderate outliers.

4. **Recommendation**: keep hard-zero as default (no leakage), but scaling is a
   reasonable option where utility matters more. The ~10pp accuracy gap may be
   acceptable in many threat models.
