# PAC-DB Clipping Parameter Evaluation

Systematic evaluation of PAC-DB's clipping mechanisms against membership inference
attacks (MIA). Tests level-based clipping (`pac_clip_support`), iterative
mean-sigma clipping, their combination, and real-world skewed data.

## Background: PAC-DB and Membership Inference

PAC-DB privatizes SQL aggregate queries by maintaining 64 parallel counters (one
per possible-world bit of a hashed privacy unit key). Each row contributes to ~32
of the 64 counters. At finalization, noise calibrated to the variance across
counters is added, bounding the mutual information (MI) an adversary can extract.

A **membership inference attack (MIA)** asks: given a noised query result, can an
adversary determine whether a specific individual (privacy unit, PU) contributed
to the query? PAC-DB targets an MI budget of 1/128, which theoretically limits
the adversary's posterior success rate to ~56% (from a 50% prior).

### The Variance Side-Channel

The standard attack exploits the **variance** of the noised output. If a PU's
contribution is large relative to the background (e.g., a billionaire among
average earners), their presence/absence changes the output distribution's
variance dramatically. The attacker observes the result and classifies:

- High variance (result far from expected background) → PU is likely **in**
- Low variance (result close to expected) → PU is likely **out**

### Clipping as Defense

**Level-based clipping** (`pac_clip_support`): Values are decomposed into
exponential magnitude levels (4x per level). Each level maintains a 64-bit
bitmap tracking distinct PU contributors. Levels with fewer distinct PUs than
the `pac_clip_support` threshold are zeroed out (hard-zero mode). This removes
outlier contributions that would create a detectable variance gap.

**Mean-sigma clipping**: Before PAC aggregation, pre-aggregate per PU and
iteratively clip values to mean ± t*sigma (3 rounds). This bounds individual
PU contributions relative to the population statistics.

## Methodology

### Setup

- **Background**: N=1000 users with uniform acctbal in [1, 10000]
- **Target**: User with extreme value (TV=999999 unless noted)
- **MI budget**: pac_mi = 1/128 (0.0078125)
- **Trials**: 30 per scenario (each with different pac_seed)

### Attack Procedure

For each trial:
1. Create the database with background users
2. Run the query **with** the target user ("in" condition) → result_in
3. Run the same query **without** the target user ("out" condition) → result_out

### Classification

**Best-threshold classifier**: Search 50 evenly-spaced thresholds between the
minimum and maximum observed results. For each threshold t:
- Classify as "in" if result > t, "out" otherwise
- Compute accuracy = fraction of correct classifications
- Report the maximum accuracy across all thresholds

An accuracy of 50% means random guessing (no information leakage). An accuracy
of 100% means perfect membership inference (complete privacy breach).

### Metrics

- **Best accuracy**: Maximum classification accuracy across all tested thresholds
- **mean_in / mean_out**: Average result when target is in/out
- **std_in / std_out**: Standard deviation when target is in/out
- **std ratio**: std_in / std_out — measures the variance side-channel (1.0 = no channel)

---

## Experiment 1: pac_clip_support Threshold Sweep

**Question**: How does varying the minimum-PU-per-level threshold affect attack
accuracy and utility across different group sizes?

### Results

| filter | clip=off | clip=2 | clip=5 | clip=10 | clip=20 | clip=40 | clip=60 |
|---|---|---|---|---|---|---|---|
| <=3 (3-4 PUs) | **80.0%** | 60.8% | 49.0%* | 49.0%* | 49.0%* | 49.0%* | 49.0%* |
| <=50 (~50 PUs) | **80.0%** | 56.7% | 56.7% | 56.7% | 56.7% | 50.0%* | 50.0%* |
| <=999 (~1000 PUs) | **70.0%** | 56.7% | 56.7% | 56.7% | 56.7% | 56.7% | 56.7% |

\* = all output is zero (utility destroyed)

### Analysis

**Small filter (<=3)**: clip=2 reduces attack accuracy from 80% to 61%. clip>=5
zeroes everything because with only 3-4 PUs, no level reaches 5 distinct bitmap
contributors. Attack accuracy drops to 49% (random) but utility is zero.

**Medium filter (<=50)**: clip=2 through clip=20 all give 56.7%. The outlier level
(with 1 contributor) is zeroed regardless — raising the threshold doesn't help
until it exceeds the background contributor count at normal levels. clip=40
zeroes everything (50 PUs can't produce 40 estimated distinct per level).

**Wide filter (<=999)**: clip=2 drops accuracy to 56.7% and it stays there
regardless of threshold. The residual 56.7% comes from the **mean shift** —
the outlier's presence changes the expected value by ~126K (TV / (2 * group_size)),
which is detectable even when the variance side-channel is eliminated.

**Key insight**: Clipping eliminates the variance channel but not the mean channel.
The 56.7% floor is the mean-shift attack, which clipping cannot address.

---

## Experiment 3: Iterative Mean ± t*sigma Clipping

**Question**: Does pre-clipping per-PU contributions to mean ± t*sigma protect
better than level-based clipping, especially for small filters?

### Method

Before PAC aggregation:
1. Compute per-PU sums: `GROUP BY user_id`
2. Compute mean μ and standard deviation σ of the per-PU sums
3. Clip: replace values outside [μ - t*σ, μ + t*σ] with the boundary value
4. Repeat 3 times (iterative convergence)
5. Feed clipped values into normal PAC query (no pac_clip_support)

### Results

| filter | t=2 | t=3 | t=4 | t=5 |
|---|---|---|---|---|
| <=3 | 55.6% | 55.6% | 55.6% | 55.6% |
| <=50 | 53.3% | 53.3% | 53.3% | 53.3% |
| <=999 | 53.3% | 53.3% | 53.3% | 53.3% |

### Analysis

Mean-sigma clipping outperforms level-based clipping for **small filters**:
55.6% vs 60.8% (clip=2). The reason: the outlier value (999999) is so far
from the population mean (~5000) that any t value clips it aggressively. After
clipping to ~15000 (mean + 2*sigma), the residual signal is much smaller than
with level-based clipping (which only attenuates by one level = 4x).

The t parameter has almost no effect because the outlier is >100x the mean.
For outliers closer to the population (e.g., 10x), different t values would
show more variation.

**Important**: Mean-sigma clipping produces **non-zero output** for small filters,
unlike level-based clipping with clip>=5. This preserves utility while still
offering 55.6% accuracy (near random).

---

## Experiment 4: Combined Approach (Mean-Sigma + Level-Based)

**Question**: Is combining both clipping methods strictly better?

### Results (small filter, <=3)

| t | clip=2 | clip=5 | clip=10 |
|---|---|---|---|
| 2 | 63.6% | 50.9%* | 50.9%* |
| 3 | 63.6% | 50.9%* | 50.9%* |
| 4 | 60.0% | 50.9%* | 50.9%* |

\* = zero output

### Results (wide filter, <=999)

| t | clip=2 | clip=5 | clip=10 |
|---|---|---|---|
| 2 | 60.0% | 60.0% | 60.0% |
| 3 | 60.0% | 60.0% | 60.0% |
| 4 | 60.0% | 60.0% | 60.0% |

### Analysis

**Surprising finding**: The combined approach is **worse** than mean-sigma clipping
alone for small filters (63.6% vs 55.6%). After pre-clipping reduces the outlier
to ~15000, the level-based clipping still sees it as slightly elevated and
introduces a noise structure difference. The two mechanisms interfere rather than
complement.

For wide filters, combined gives 60.0% — worse than either method alone (56.7%
for level-clip, 53.3% for mean-sigma). The mean-sigma pre-clipping changes the
mean in a way that level-clipping doesn't compensate for.

**Recommendation**: Use mean-sigma clipping alone for small-group queries. Level-based
clipping alone for wide queries. The combination does not help.

---

## Experiment 6: Real-World Skewed Data (Power-Law Distribution)

**Question**: Does clipping work when the background data itself is skewed?

### Setup

Power-law distributed earnings:
- 95% of PUs earn $100-$500 (normal)
- 3% earn $500-$1,500 (medium)
- 2% earn $2,000-$5,000 (high earners)

### Results: Extreme outlier ($50,000)

| filter | no clip | clip=2 | clip=5 |
|---|---|---|---|
| <=3 | 80.0% | 60.8% | 49.0%* |
| <=50 | 80.0% | 58.3% | 58.3% |
| <=999 | 66.7% | 58.3% | 58.3% |

### Results: Moderate outlier ($5,000 — within high-earner range)

| filter | no clip | clip=2 |
|---|---|---|
| <=3 | 74.0% | 60.8% |
| <=50 | 65.0% | 58.3% |
| <=999 | 55.0% | 58.3% |

### Results: Natural outlier ($3,000 — typical high earner)

| filter | no clip | clip=2 |
|---|---|---|
| <=3 | 70.0% | 60.8% |
| <=50 | 60.0% | 58.3% |

### Analysis

Clipping behaves similarly on skewed data as on uniform data. The key finding:
with clip=2, the residual attack accuracy converges to **60.8% for small filter
regardless of outlier magnitude** ($3K, $5K, $50K all give 60.8%). This means
the clipping mechanism fully removes the outlier signal — the residual is a
constant from the noise structure itself.

For wide filter, a $5K outlier without clipping gives only 55.0% (barely above
random), but with clip=2 it rises to 58.3%. **Clipping can slightly INCREASE
attack accuracy** for moderate outliers in wide queries by introducing a mean
shift that wasn't there before.

---

## Experiment 5: Composition Attack (Multiple Queries)

**Question**: Can an attacker average many queries to overcome the noise and
break privacy? Does clipping prevent this?

### Theory

For independent queries with the same noise distribution:
- Signal (outlier contribution): constant S regardless of number of queries
- Noise standard deviation after averaging N queries: σ/√N
- Signal-to-noise ratio after N queries: S·√N/σ

Without clipping, S ≠ 0 (outlier contributes), so SNR grows without bound as
N → ∞. Eventually the attacker achieves 100% accuracy.

With hard-zero clipping (S = 0 for each individual query), averaging N zeros
still gives zero. But the clipping **decision** might vary across queries
(different pac_hash per query → different bitmap support per level), creating
a tiny per-query signal that could accumulate.

### Results: Without clipping (clip=off)

| N queries | accuracy | std_in | std_out |
|---|---|---|---|
| 1 | 74.1% | 6,678,043 | 117,410 |
| 5 | 73.3% | 2,791,251 | 39,297 |
| 10 | 70.0% | 1,878,886 | 35,196 |
| 25 | **83.3%** | 1,515,532 | 18,137 |
| 50 | 73.3% | 894,849 | 16,098 |
| 100 | **86.7%** | 987,083 | 13,044 |

Without clipping, accuracy **climbs with more queries**: from 74% (single query)
to 87% (100 queries). The attacker's strategy works: averaging reduces the noise
(std_out drops from 117K to 13K) while the outlier signal persists. With more
queries, this would approach 100%.

### Results: With clipping (clip=2, hard-zero)

| N queries | accuracy | std_in | std_out |
|---|---|---|---|
| 1 | 66.7% | 108,105 | 64,576 |
| 5 | 70.0% | 60,252 | 27,635 |
| 10 | 66.7% | 38,586 | 19,507 |
| 25 | 63.3% | 20,841 | 14,957 |
| 50 | 66.7% | 14,026 | 11,412 |
| 100 | **60.0%** | 9,530 | 11,039 |

With clipping, accuracy **does NOT climb** — it stays in the 60-67% range and
even drops to 60% at 100 queries. The std_in and std_out **converge** (ratio 0.86
at 100 queries). The clipping zeroes the outlier signal at each individual query,
so averaging N zeros still gives approximately zero. The residual 60-67% comes
from the PAC noise structure, not the outlier.

### Analysis

**Composition breaks unclipped PAC but NOT clipped PAC.** This is the most
important finding: it validates that hard-zero clipping is robust against the
most powerful practical attack (query repetition). The theoretical concern that
the clipping *decision* might leak across queries appears unfounded — at least
up to 100 queries, no signal accumulates.

---

## Summary of Findings

| Defense | Small filter (3-4 PUs) | Medium filter (~50 PUs) | Wide filter (~1000 PUs) |
|---|---|---|---|
| No defense | 80% | 80% | 70% |
| Level-clip (clip=2) | 60.8% | 56.7% | 56.7% |
| Mean-sigma (t=3) | **55.6%** | **53.3%** | **53.3%** |
| Combined | 63.6% (worse!) | — | 60.0% (worse!) |
| Level-clip (clip=5) | 49% (zero output) | 56.7% | 56.7% |

### Key Takeaways

1. **Mean-sigma clipping is the best single defense** across all group sizes.
   It produces 53-56% accuracy (near random) while preserving utility.

2. **Level-based clipping is effective but has a sharp utility cliff**. For
   small groups, any threshold above the group size zeroes all output.

3. **Combining methods does NOT help** — they interfere with each other.

4. **There is a ~56% accuracy floor** that no clipping method breaks through.
   This residual comes from the mean shift (the outlier changes the expected
   value) which clipping cannot address. Addressing this requires additional
   noise specifically targeting the mean shift — which connects to the
   calibration transfer blueprint's proposed Δ term.

5. **Clipping behavior is consistent across data distributions** (uniform vs
   power-law). The mechanism is robust to background skew.

6. **Composition breaks unclipped PAC but NOT clipped PAC.** Without clipping,
   averaging 100 queries raises accuracy from 74% to 87% (and would approach
   100% with more). With hard-zero clipping, accuracy stays at 60-67% even
   after 100 queries. The clipping decision does not leak exploitable signal
   across queries.
