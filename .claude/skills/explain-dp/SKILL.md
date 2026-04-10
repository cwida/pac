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
