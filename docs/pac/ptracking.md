# Persistent Secret P-Tracking

## Overview

When `pac_ptracking` is enabled (default: `true`) and `pac_mi > 0`, the noise calibration step tracks a Bayesian posterior distribution `p` over the 64 worlds (bit positions) across all cells released by a query. This provides **query-level** membership inference attack (MIA) protection, rather than just cell-level.

## Background

PAC uses 64 subsamples (worlds) derived from the 64 bits of each privacy unit's key hash. For each query, one world `J` is selected as the "secret" — the same `J` for all output cells in that query.

Without p-tracking, each cell independently calibrates noise using the uniform variance over the 64 counter values:

```
variance = sum((vals - mean)^2) / (N - 1)
noise_var = variance / (2 * mi)
```

The problem: an adversary who observes all output cells can compose information across them. After seeing cell 1's noisy output, the adversary's belief about which world is the secret shifts from uniform to some posterior. Cell 2's noise should account for this shifted belief, but without p-tracking it doesn't.

## How it works

A probability vector `p` of dimension 64 is initialized to uniform `(1/64, ..., 1/64)` at the start of each query. For each cell that gets noised:

### 1. Calibrate noise using p-weighted variance

Instead of uniform variance, compute the variance weighted by the current posterior `p`:

```
mean = sum(vals[k] * p[k])
variance = sum((vals[k] - mean)^2 * p[k])
noise_var = variance / (2 * mi)
```

This corresponds to `get_noise_var(p, vals, b)` in the reference Python implementation.

### 2. Add noise (unchanged)

```
noisy_result = vals[secret_J] + Normal(0, sqrt(noise_var))
```

### 3. Bayesian posterior update

After observing the noisy result, update `p` using the likelihood of each world producing that result:

```
for each world k:
    log_p[k] = log(p[k]) + log_likelihood[k]
    where log_likelihood[k] = -0.5 * (vals[k] - noisy_result)^2 / noise_var

p = softmax(log_p)   (via log-sum-exp for numerical stability)
```

This corresponds to `update_p(p, vals, noisy_result, noise_variance)` in the reference.

As more cells are released, `p` concentrates around the true secret world. The p-weighted variance shrinks, causing the noise variance formula to produce **larger** noise to compensate — exactly what's needed for correct composition.

## Configuration

```sql
-- Enable p-tracking (default)
SET pac_ptracking = true;

-- Disable p-tracking (fall back to cell-level MIA, same as PAC-DB)
SET pac_ptracking = false;
```

P-tracking is only active when `pac_mi > 0`. When `pac_mi = 0` (deterministic mode), no noise is added and p-tracking has no effect.

## Privacy guarantees

| Setting | Guarantee |
|---------|-----------|
| `pac_ptracking = true` | Query-level MIA: adversary cannot infer if a record was used to answer **any cell** of the query |
| `pac_ptracking = false` | Cell-level MIA: adversary cannot infer if a record was used for a **specific cell** (PAC-DB guarantee) |
