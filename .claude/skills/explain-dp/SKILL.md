---
name: explain-dp
description: Reference material for differential privacy concepts. Auto-loaded when discussing privacy, attacks, sensitivity, or clipping.
---

## Differential Privacy (DP)

### Definition

A randomized mechanism M satisfies (ε,δ)-differential privacy if for all
neighboring datasets D, D' (differing in one individual) and all outputs S:

    P[M(D) ∈ S] ≤ e^ε · P[M(D') ∈ S] + δ

Smaller ε = stronger privacy. δ is the probability of catastrophic failure.

### Key concepts

- **Sensitivity**: Maximum change in query output when one individual is
  added/removed. For SUM with values in [L,U]: sensitivity = U-L.
- **Laplace mechanism**: Add Laplace(0, sensitivity/ε) noise. Standard for counting queries.
- **Gaussian mechanism**: Add N(0, sensitivity²·2ln(1.25/δ)/ε²) noise. Better for composition.
- **Composition**: Running k queries on the same data costs k·ε total (basic),
  or O(√k·ε) with advanced composition.
- **Post-processing**: Any function of a DP output is still DP. Free to clip/transform after noise.

### Membership Inference Attack (MIA)

The adversary's game: given a query result, determine whether a specific individual
is in the dataset. Attack accuracy = fraction of correct guesses across trials.
50% = random (DP working). >50% = information leakage.

### Bounded user contribution (Wilson et al. 2019)

Standard approach for DP SQL:
1. GROUP BY user_id → compute per-user contribution
2. Clip each user's contribution to [L, U]
3. Sum clipped contributions
4. Add noise calibrated to U-L

This handles both single-large-value outliers and many-small-values users.
Reference: "Differentially Private SQL with Bounded User Contribution" (Google).

### Bounded vs. unbounded vs. user-level DP — what to pick

Three flavors, decided by what counts as a "neighboring dataset":
- **Bounded (row-level)**: D and D' differ by *changing* one tuple. Flex/elastic
  sensitivity (Johnson, Near, Song 2018) uses this. Tractable but only protects
  individual tuples, not entities.
- **Unbounded (row-level)**: D and D' differ by adding or removing one tuple.
  Adds factor 2× to sensitivity vs. bounded.
- **User-level / entity-level (unbounded at the user)**: D and D' differ by
  adding/removing all rows belonging to one user. Wilson et al. 2019 (Google)
  use this. This is what PAC's PU concept already encodes — one PU = one user
  = one row in the PU table plus all linked rows across PRIVACY_LINK joins.

For a PU+LINK system, **user-level (unbounded at the PU)** is the natural choice.
The neighboring relation is "remove all rows belonging to one PU." Elastic
sensitivity = ∏(max-frequency along the join chain) — already counts per-PU
contribution correctly. This is what the current `dp_elastic_compiler.cpp`
implements via `ExtractFKChain`/`ComputeMfK`.

### Elastic sensitivity (Flex / Johnson, Near, Song 2018)

Per-query upper bound on local sensitivity for SQL with equijoins:
- Walk the join tree bottom-up. Each table contributes a max-frequency
  metric `mf` (max number of rows sharing any FK value).
- Global elastic sensitivity = ∏ mf along the chain.
- Smooth elastic sensitivity (when δ > 0): exponential decay over distance k
  with parameter β = ε / (2 ln(2/δ)). Tighter than global ES.

**Key Flex design choices, paraphrased:**
- WHERE clauses / filters: do NOT reduce the sensitivity bound. Filters
  pass through unchanged (Flex Section 3.3, Definition 7). They only shrink
  result size, not sensitivity. Do not assume a selective filter buys privacy.
- GROUP BY: 2× sensitivity multiplier (one row change can affect two histogram
  bins — the "from" bin and the "to" bin). Bin set must be specified or
  drawn from a public domain.
- SUM/AVG: requires a value-range bound `vr(a) = max - min` per column.
  Flex does not auto-detect bounds — analyst supplies them, or column
  CHECK constraints enforce them.
- Unsupported: non-equijoins, joins on computed/aggregated keys, recursive
  queries. Flex paper: 76% of real-world SQL queries supported; 14% rejected
  for unsupported features, 7% for parsing.

### τ-thresholding for GROUP BY (Wilson et al. 2019)

Drop any group whose noisy count is below threshold τ. Required for
unbounded GROUP BY domains (otherwise the *set of keys released* leaks
who's in the data).

Formula (Wilson et al., Theorem 2):

    τ = 1 − (C_u · log(2 − 2(1−δ)^(1/C_u))) / ε

where C_u is the per-user partition contribution bound. Requires δ > 0;
without δ, τ-thresholding can't be made (ε,δ)-DP.

This is conceptually similar to PAC's `privacy_min_group_count` (NULL out
low-SNR cells), but the formula differs. A unified setting could expose
"minimum group size" with each mechanism applying its own formula.

### WHERE-clause handling — what to watch

Flex/Wilson both let WHERE pass through for sensitivity. But two practical
gotchas:
1. WHERE on a *protected* column: predicate evaluation reveals info about
   the protected value (the row appears or doesn't). Strictly speaking
   still DP-compliant if sensitivity is computed correctly, but a soft
   spot — better to reject or warn.
2. WHERE that turns COUNT into "test for one specific user": if the filter
   is selective enough that only one PU matches, the noise still protects
   them, but utility collapses. This is utility, not privacy.

References:
- Flex / Elastic Sensitivity: arXiv 1706.09479 (Johnson, Near, Song; VLDB 2018)
- Bounded User Contribution: arXiv 1909.01917 (Wilson et al., PoPETs 2020)
- Chorus follow-up: ICDE 2020 — same group's full SQL→SQL DP rewriter

### How PAC differs from DP

| | DP | PAC |
|---|---|---|
| **Guarantee type** | Input-independent (worst-case) | Instance-dependent (distribution D) |
| **Noise calibration** | Sensitivity s → noise ∝ s/ε | Variance σ² → noise ∝ σ²/(2β) |
| **White-boxing** | Required (analyze algorithm) | Not needed (black-box simulation) |
| **Composition** | k queries → k·ε (basic) | k queries → Σ MIᵢ (linear, Theorem 2) |
| **Privacy metric** | ε (log-likelihood ratio) | MI (mutual information, in nats) |
| **Conversion** | MI=1/128 ≈ ε=0.25 for prior=50% | See Table 3.2 in thesis |
| **Stable algorithms** | Same noise regardless | Less noise automatically |
| **Outlier impact** | Sensitivity explodes | Variance explodes (same practical problem) |

Key insight: PAC guarantees are **loose** — the theoretical bound on MIA success
rate is conservative. Empirical attacks achieve lower success than the bound
predicts. This means the bounds are hard to violate.

### Input clipping (Winsorization)

Clip individual values to [μ-tσ, μ+tσ] before aggregation. Reduces sensitivity.
Well-established in DP literature. Limitations: doesn't catch users with many
small values (need per-user contribution clipping instead).

### Privacy-conscious design

Rather than post-hoc privatization (build algorithm, then add noise), PAC enables
**privacy-conscious design**: optimize algorithm parameters jointly with
the privacy budget.

Key result: For a privatized estimator with budget B:
    MSE = Bias² + (1/(2B) + 1) · Var + error

This means privatization amplifies the variance by 1/(2B). At tight budgets
(small B), the optimal algorithm shifts toward lower-variance (higher-bias)
models. E.g., stronger regularization in ridge regression.

For databases: this suggests that queries producing high-variance outputs (due to
outliers, small groups, etc.) are inherently harder to privatize. Clipping reduces
variance and thus the noise needed, improving the privacy-utility tradeoff.

### DP vs PAC: Worst-Case vs Instance-Based Sensitivity

DP calibrates noise to **global sensitivity**: max over ALL possible datasets of
how much the output changes when one row is added/removed. This is a worst-case
quantity independent of the actual data.

PAC calibrates noise to the **actual data geometry**: the variance of the query
output across subsamples of the real table. Stable queries on stable data get
less noise automatically.

The calibration transfer conjecture (Blueprint, April 2026) bridges the two:
PAC's instance-based noise from one subsampling distribution D₀, augmented by
a small compensation Δ for the spectral gap, transfers to a nearby D₁ (e.g.,
a different query or different population). Δ is instance-based (proportional
to the actual distributional distance d(D₀,D₁)), much smaller than DP's global
sensitivity. Clipping bounds per-PU influence on the variance, keeping d small.
This gives PAC "universal MIA resistance" that degrades gracefully with the
effective distributional distance, rather than DP's uniform worst-case bound.
