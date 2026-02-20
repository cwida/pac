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

## Implementation details

### Cross-aggregate sharing

The `p` vector is shared across all aggregate functions (pac_count, pac_sum, pac_avg, pac_min, pac_max, pac_aggregate) within the same query via a global map keyed by `query_hash`. This ensures that if a query computes both `pac_count` and `pac_sum`, the information leaked by count cells is accounted for when calibrating sum cells, and vice versa.

### Concurrency

A mutex protects the shared `p` state. Each noise operation locks, reads `p`, computes weighted variance, adds noise, updates `p`, and unlocks. This serialization is inherent to the approach — the collaborator noted "the noising step will have to be done sequentially."

### Handling NULL counters

When a group has fewer than 64 valid counters (some bit positions have no data), the `p` values for non-null worlds are extracted and renormalized to sum to 1 before computing weighted variance and doing the Bayesian update. The null worlds' `p` values are preserved unchanged.

### Lifetime management

The global map stores `weak_ptr` references. Each `PacBindData` instance holds a `shared_ptr` to the `PacPState`. When all `PacBindData` instances for a query are destroyed (query finishes), the `PacPState` is automatically freed and the expired `weak_ptr` is cleaned up lazily.

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

## Files modified

- `src/include/aggregates/pac_aggregate.hpp` — `PacPState` struct, `shared_ptr<PacPState>` in `PacBindData`
- `src/aggregates/pac_aggregate.cpp` — global map, p-weighted noise in `PacNoisySampleFrom64Counters` and `PacAggregateScalar`
- `src/aggregates/pac_count.cpp` — passes `pstate` to noise function
- `src/aggregates/pac_sum.cpp` — passes `pstate` to noise function (also covers pac_avg via template)
- `src/aggregates/pac_min_max.cpp` — passes `pstate` to noise function
- `src/categorical/pac_categorical.cpp` — `pstate` in `PacCategoricalBindData`, passes to noise function
- `src/core/pac_extension.cpp` — registers `pac_ptracking` setting

## Reference

Based on the collaborator's Python reference implementation:

```python
def get_noise_var(p, vals, b):
    mean = np.sum(vals * p)
    centered = vals - mean
    variance = np.sum(centered**2 * p)
    noise_var = variance / (2 * b)
    return noise_var

def update_p(p, vals, noisy_result, noise_variance):
    diff = vals - noisy_result
    mahalanobis = (diff ** 2) / noise_variance
    log_likelihoods = -0.5 * mahalanobis
    log_p = np.log(p + 1e-300) + log_likelihoods
    c = log_p.max()
    log_sum = c + np.log(np.sum(np.exp(log_p - c)))
    p = np.exp(log_p - log_sum)
    return p
```
