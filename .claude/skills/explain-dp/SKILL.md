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

- PAC bounds **mutual information** (pac_mi), not ε-divergence
- PAC does NOT compute sensitivity — noise is calibrated differently
- PAC uses 64 parallel counters + bitslice encoding for efficient aggregation
- pac_clip_sum uses **support-based magnitude clipping** instead of hard [L,U] bounds

### Input clipping (Winsorization)

Clip individual values to [μ-tσ, μ+tσ] before aggregation. Reduces sensitivity.
Well-established in DP literature. Limitations: doesn't catch users with many
small values (need per-user contribution clipping instead).
