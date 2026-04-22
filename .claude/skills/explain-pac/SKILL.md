---
name: explain-pac
description: Reference material for PAC privacy internals. Auto-loaded when discussing PAC mechanism, noise, counters, or clipping.
---

## PAC Privacy Overview

PAC (Probably Approximately Correct) privacy is a framework for privatizing
algorithms with provable guarantees, described in [SIMD-PAC-DB](https://arxiv.org/abs/2603.15023).

### Formal definition

Given a data distribution D, a query Q satisfies (δ, ρ, D)-PAC Privacy if no
adversary who knows D can, after observing Q(X) where X ~ D, produce an
estimate X̂ such that ρ(X̂, X) = 1 with probability ≥ (1-δ).

The key insight: **noise scales with the variance of the algorithm's output across
random subsamples** of the data. Stable algorithms (low variance) need less noise.

### The 4-step privatization template

1. **Subsample**: Draw m random 50%-subsets X₁...Xₘ from the full dataset
2. **Compute**: Run the query Q on each subset → outputs y₁...yₘ
3. **Estimate noise**: Compute variance σ² across the yᵢ. Required noise: Δ = σ²/(2β)
   where β is the MI budget
4. **Release**: Pick a random subset Xⱼ, return Q(Xⱼ) + N(0, Δ)

This is the theoretical foundation. SIMD-PAC-DB encodes this efficiently using
64 parallel counters (one per possible subset assignment bit).

### MI → posterior success rate

| MI | Max posterior (prior=50%) | Max posterior (prior=25%) |
|----|--------------------------|--------------------------|
| 1/128 | 56.2% | 30.5% |
| 1/64 | 58.8% | 32.9% |
| 1/32 | 62.4% | 36.3% |
| 1/16 | 67.5% | 41.2% |
| 1/8 | 74.5% | 48.2% |
| 1/4 | 83.8% | 58.4% |
| 1/2 | 95.2% | 72.7% |
| 1 | 100% | 91.4% |

### PAC Composition

For T adaptive queries with independent random sampling per query, the total
MI is bounded by the sum: MI(total) ≤ Σᵢ MIᵢ. This is linear composition —
each query's MI adds to the budget. The key requirement: **independent random
sampling per query** (each query uses a fresh random subset).

### PAC vs DP

- **DP**: input-independent guarantee. Requires white-boxing to compute sensitivity.
  Noise ∝ sensitivity/ε. Works for worst-case neighboring datasets.
- **PAC**: instance-dependent guarantee. No white-boxing needed. Noise ∝ Var[Q(X)]/β.
  Stable queries get less noise automatically. But the guarantee depends on the
  data distribution D.

### Core mechanism (SIMD-PAC-DB implementation)

- Each aggregate maintains **64 parallel counters** (one per bit of a hashed key)
- Each row's value is added to ~32 counters (determined by pac_hash of the PU key)
- At finalization, noise calibrated to a **mutual information bound** (pac_mi) is
  added, and the result is estimated from the counters
- PAC does NOT compute sensitivity (unlike differential privacy)
- The 64 counters encode m=64 possible subsets in one pass (SIMD-efficient)

### SWAR bitslice encoding

- Counters are packed as 4 × uint16_t per uint64_t (SWAR = SIMD Within A Register)
- This enables processing 4 counters per instruction without actual SIMD intrinsics
- Overflow cascades to 32-bit overflow counters when 16-bit counters saturate

### pac_clip_sum (contribution clipping)

- **Pre-aggregation**: Query rewriter inserts `GROUP BY pu_hash` to sum each user's
  rows into a single contribution (handles the "50K small items" case)
- **Magnitude levels**: Values decomposed into levels (4x per level, 2-bit shift).
  Level 0: 0-255, Level 1: 256-1023, Level 2: 1024-4095, etc.
- **Bitmap tracking**: Each level maintains a 64-bit bitmap of distinct contributors
  (using birthday-paradox estimation from popcount)
- **Hard-zero**: Levels with fewer distinct contributors than `pac_clip_support`
  contribute nothing to the result (prevents variance side-channel attacks)

### Key settings

- `pac_mi`: Mutual information bound (0 = deterministic/no noise)
- `pac_seed`: RNG seed for reproducible noise
- `pac_clip_support`: Minimum distinct contributors per magnitude level (NULL = disabled)
- `pac_hash_repair`: Ensure pac_hash outputs exactly 32 bits set

### Calibration Transfer (Blueprint, April 2026)

PAC calibrates noise per query from the 64 counters' variance. The data is
fixed; the "distribution" D is over random 50%-subsamples (the 64 worlds).
The calibration transfer conjecture asks: when noise calibrated under one
subsampling distribution D₀ also protects under a different distribution D₁.

**What D₀ and D₁ represent** (NOT two table versions — the data is fixed):
- Different effective subsampling distributions arising from different queries
  or different populations. E.g., D₀ = variance profile of a broad query,
  D₁ = variance profile of a narrow-filter query targeting one PU.
- Or: D₀ = subsampling with Alice present, D₁ = without Alice. The
  covariance Σ changes because Alice's contribution affects the 64 counters.

**Why narrow-filter attacks succeed**: The noise was calibrated from the full
query's variance (D₀). But the attacker's distinguishing task operates on a
narrow slice (D₁) where one PU dominates. If d(D₀, D₁) is large, calibration
doesn't transfer → the noise is insufficient → attack succeeds.

**Conjecture**: If d(D₀, D₁) ≤ t, noise Q₀ from D₀ augmented by
Δ = N(0, spectral_gap) is valid for D₁. The compensation Δ is instance-based
(proportional to actual distributional distance, not worst-case like DP).

**Connection to clipping**: pac_clip_support bounds per-PU influence on Σ.
This keeps d(D₀, D₁) small regardless of filter → calibration transfers →
attacks fail. Clipping is the mechanism that makes the transfer bound tight.

**Open questions**: optimal distance metric (Wasserstein vs Fisher-Rao),
sharp transfer constants, extending from continuous (SGD) to discrete
(PAC DB's 64-out-of-128 subsampling) setting.

Reference: "Calibration Transfer Between Close Distributions — Blueprint for
Universal Membership Inference Resistance" (working document, 05 April 2026).
Thesis: Sridhar, "Toward Provable Privacy for Black-Box Algorithms via
Algorithmic Stability" (MIT PhD, February 2026), Chapter 3.

### DDL

```sql
ALTER TABLE customer ADD PAC_KEY (c_custkey);
ALTER TABLE customer SET PU;
ALTER TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer (c_custkey);
```
