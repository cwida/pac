# PAC Internals

This document describes the internal implementation of the SIMD-PAC-DB aggregation algorithm.

## Overview

PAC transforms standard SQL aggregates (SUM, COUNT, AVG, MIN, MAX) into privacy-preserving versions that use **bit-level stochastic counting**. Instead of traditional sampling where rows are included or excluded, PAC aggregation algorithms maintains 64 parallel results ("possible worlds") and uses hash bits to determine which results receive each value.

## Core Algorithm

### Hash-Based Probabilistic Counting

For each row in the query:

1. **Compute hash**: The privacy unit's PAC_KEY is hashed to produce a 64-bit value. If there are multiple key columns, their hashes are XORed. The resulting hash is further transformed by pac_hash(hash), which re-hashes the hash such that each query uses a different hash function.
2. **Distribute to counters**: Each bit in the hash of a PU key determines whether that entity is or is not part of that possible world. There are 64 possible worlds. Technically, for each of the 64 counters it is simple: if bit `j` of the hash is 1, the value is added to counter `j`

```cpp
for (int j = 0; j < 64; j++) {
    if ((key_hash >> j) & 1) {
        counter[j] += value;
    }
}
```

Our approach is called SIMD-PAC-DB because these loops are SIMD-friendly. The above loop can be executed in just five AVX512 instructions. This makes DuckDB-pac very efficient, queries are typically just 2x slower than default DuckDB.

On average, each row contributes to ~32 counters (half the bits are 1). When the COUNT and SUM functions finalize, they multiply the values in the counters by two: because each counter only receives half the values on average.

### pac_noised(counters)

A `secret_world` (0-63) is randomly chosen, for each query. This determines which counter is used for the final result:

```cpp
result = counter[secret_world] + PAC_NOISE(counters);
```

The PAC_NOISE(counters) measures the variance in the counters and uses this to compute noise. This noise computation changes also depending on how many results have been released in the query.

Normally the PAC aggregate functions, like pac_count(), return 64 counters (as a LIST<float>). Then, there is a function pac_noised(LIST<float>):float that returns a single noised value, as described above. 

For performance reasons, we also, provide fused versions, like pac_count_noised(), that are equivalent to pac_noised(pac_count(..)). 

In the general case, aggregates might be part of complex expressions. For these we use the aggregation functions that return counters (LIST<float>) and then use SQL lambda functions to iterate over the counters and compute the complex expression -- which is fed into pac_noised() to only noise the final expression result once.

## PAC Aggregate Functions

### pac_sum

Transforms `SUM(value)` into `pac_sum(hash_expr, value)`.

**Implementation details:**

- **SWAR (SIMD Within A Register)**: Packs multiple counters per 64-bit word. We use 64-bit lanes because the hashes are 64-bit and all values in a computation must have the same width to get compiler auto-vectorization.
- **Approximate Sums**: Uses 16-bit sums only. In case of overflow, cascades to a next level that cuts off the lower 4 bits and gets 4 higher bits. This can go on for 25 levels, but typically there is just 1 level or few (levels are allocated lazily).
- **Signed value handling**: for negative numbers, we allocate (also lazily) a complete second approximate SUM state, that stores all negative values negated (positive). At aggregation finalization negative (if present) is subtracted from positive total. This method turned out to be much stabler for sums involving both positive and negative numbers that can cancel each other out.

```sql
-- Original
SELECT SUM(amount) FROM orders;

-- Transformed
SELECT pac_sum(hash(customer_id), amount) FROM orders;
```

### pac_count

Transforms `COUNT(*)` or `COUNT(expr)` into `pac_count(hash_expr, 1)`.

**Implementation details:**

- Uses 64 counters of 8-bits (but stored as 8x 64-bits counters, i.e. SWAR - see pac_sum). After processing 255 values, before this can overflow, all counters are flushed to full 64-bit totals (lazily allocated).
- Buffering optimization: do not immediately process new values, but wait until there are 4 values (buffer 3 values first). This delay the allocation of the aggregation state (containing 64 counters) until the first buffer flush. Helps against Out Of Memory failures, because in difficult aggregations with very many GROUP BY keys, the first hash-table-phase would find very few or no values with the same key. Duplicates would only be found in the second phase of spilling aggregation, when partitions are re-read and combined. Buffering prevents a full state being allocated for every individual tuple.

```sql
-- Original
SELECT COUNT(*) FROM orders;

-- Transformed  
SELECT pac_count(hash(customer_id), 1) FROM orders;
```

### pac_min / pac_max

Transforms `MIN(value)` and `MAX(value)` into their PAC equivalents.

**Implementation details:**

- **Bound optimization**: Tracks current min/max bounds to skip unnecessary comparisons
- **Lazy allocation**: Only allocates memory for counters that receive values

```sql
-- Original
SELECT MIN(price), MAX(price) FROM products;

-- Transformed
SELECT pac_min(hash(product_id), price), pac_max(hash(product_id), price) FROM products;
```


## Query Transformation

### Direct Privacy Unit Scan

When the query directly scans a privacy unit table:

```sql
-- Original
SELECT SUM(balance) FROM customers;

-- Transformation
-- 1. Build hash from customers.id (PAC_KEY)
-- 2. Replace SUM with pac_sum
SELECT pac_sum(hash(id), balance) FROM customers;
```

### PAC_LINK Path

When the query scans a table linked to a privacy unit via PAC_LINK:

```sql
-- Original  
SELECT SUM(amount) FROM orders;
-- orders has PAC_LINK(customer_id) REFERENCES customers(id)

-- Transformation
-- 1. Follow FK path: orders -> customers
-- 2. Build hash from orders.customer_id (the FK column)
SELECT pac_sum(hash(customer_id), amount) FROM orders;
```

## Build-Time Optimization Flags

These compile-time flags control algorithm variants -- they are basically there for scientific reproducibility so we can quantify the benefits of the optimizations:

| Flag | Effect |
|------|--------|
| `PAC_NOBUFFERING` | disable input buffering (lazy allocation) |
| `PAC_NOCASCADING` | Pre-allocate all accumulator levels |
| `PAC_NOSIMD` | nocascading with simd-unfriendly update kernels and auto-vectorization disabled|
| `PAC_NOBOUNDOPT` | disable bound optimization (only affects min/max) |
| `PAC_SIGNEDSUM` |  disable handling negative values in signed sums using separate (negated) counters |
| `PAC_EXACTSUM` | disable approximate sum optimization, use exact cascading (implies signedsum)|




## Source Code Organization

| File | Description |
|------|-------------|
| `src/aggregates/pac_sum.cpp` | pac_sum and pac_avg implementation |
| `src/aggregates/pac_count.cpp` | pac_count implementation |
| `src/aggregates/pac_min_max.cpp` | pac_min and pac_max implementation |

