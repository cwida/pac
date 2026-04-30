# Internal Operator Transformations

How the PAC compiler transforms specific query plan nodes. For the general compilation pipeline, see [query_operators.md](query_operators.md).

## Top-K Pushdown

**Settings:** `pac_pushdown_topk` (default: `true`), `pac_topk_expansion` (default: `1.0`)

**Problem:** In the default plan, PAC aggregates produce noised scalars. TopN sorts on noised values, which can select the wrong groups (noise distorts the ranking). This is especially bad for small LIMIT values.

**Solution:** Push TopN below the noise injection. Aggregate produces raw counter lists, TopN sorts by the true mean (pre-noise), then noise is applied only to the selected rows.

### Detection

The rewriter (`pac_topk_rewriter.cpp`) runs as a post-optimizer. It walks the plan bottom-up looking for:

1. A `LogicalTopN` node
2. With a `LogicalAggregate` child (possibly through intermediate projections like `__internal_decompress_string`)
3. Where at least one ORDER BY expression references a PAC aggregate (`pac_sum`, `pac_count`, `pac_avg`, `pac_min`, `pac_max`)

### Plan transformation

**Before:**
```
TopN(limit=K, order_by=[noised_agg])
└── Aggregate(pac_sum → noised scalar)
```

**After (expansion = 1):**
```
OrderBy(order_by=[noised_agg])
└── NoisedProj(pac_noised on K selected rows)
    └── TopN(limit=K, order_by=[true_mean])
        └── MeanProj(pac_mean for ordering)
            └── Aggregate(pac_sum_counters → LIST<FLOAT>)
```

**After (expansion > 1, e.g. 3.0):**
```
TopN(limit=K, order_by=[noised_agg])
└── NoisedProj(pac_noised on ceil(3*K) candidates)
    └── TopN(limit=ceil(3*K), order_by=[true_mean])
        └── MeanProj(pac_mean for ordering)
            └── Aggregate(pac_sum_counters → LIST<FLOAT>)
```

### Step by step

1. **Convert aggregates to `_counters` variants.** Each PAC aggregate (e.g. `pac_sum`) is rebound to its counters version (`pac_sum_counters`), which returns `LIST<FLOAT>` (64 per-sample counters) instead of a noised scalar.

2. **Insert MeanProj.** A new projection is inserted between the aggregate and TopN. For each PAC aggregate column, it adds a `pac_mean(counters)` column that computes the mean of the 64 counters. This is the "true" aggregate value (pre-noise), used for ranking.

3. **Rewrite TopN ORDER BY.** Order expressions that referenced PAC aggregates are rewritten to reference the `pac_mean` columns instead.

4. **Insert NoisedProj.** A projection above TopN applies `pac_noised()` to each counter list, producing the final noised scalar. Non-PAC columns pass through unchanged.

5. **Re-sort.** Since noise changes the ranking, a final ORDER BY (or TopN if expansion > 1) re-sorts the output by the noised values.

### Intermediate projections

When there are projections between TopN and the aggregate (e.g. for VARCHAR GROUP BY columns that go through `__internal_decompress_string`), the rewriter:
- Inserts MeanProj at the bottom (between innermost projection and aggregate)
- Adds `pac_mean` passthrough columns to each intermediate projection
- Traces bindings through the projection chain to correctly remap TopN's ORDER BY

### Expansion factor

`pac_topk_expansion = c` makes the inner TopN select `ceil(c * K)` candidates instead of `K`. More candidates means better chance of finding the true top-K after noise is applied. The final TopN then limits back to `K`.

---

## COUNT(DISTINCT)

**Supported:** `COUNT(DISTINCT col)`, `SUM(DISTINCT col)`, `AVG(DISTINCT col)`

### Pre-aggregation

DISTINCT aggregates are handled entirely via plan rewriting — no dedicated aggregate functions are needed. The compiler inserts an inner aggregation that deduplicates using DuckDB's native GROUP BY.

**Single DISTINCT column, no non-DISTINCT aggregates:**

```
Before:
  Aggregate: COUNT(DISTINCT col) GROUP BY [g]

After:
  Outer Aggregate: pac_count(combined_hash) GROUP BY [g]
  └── Inner Aggregate: bit_or(key_hash) GROUP BY [g, col]
      └── Original child
```

The inner aggregate groups on both the GROUP BY keys and the DISTINCT column. `bit_or(key_hash)` accumulates all PU hashes that produced each distinct value. The outer aggregate then applies the standard PAC function on the combined hash.

**Mixed DISTINCT + non-DISTINCT, or multiple DISTINCT columns:**

The compiler splits the plan into separate branches and joins them on the group keys:

```
Projection (reorder outputs)
└── Join (on group keys)
    ├── Branch 1 (DISTINCT col_A): bit_or + pac_count GROUP BY [g, col_A]
    └── Branch 2 (non-DISTINCT): pac_sum GROUP BY [g]
```

---

## Window Functions (Proposal)

**Status:** Not yet implemented. Window functions are currently blocked in `privacy_compatibility_check.cpp`.

### Scope

Only **full-partition window aggregates** are candidates for PAC compilation — window functions with no ORDER BY and the default frame (the entire partition). These compute one aggregate value per partition and broadcast it to every row, which is semantically equivalent to a GROUP BY aggregate joined back to the original rows.

**Passthrough window functions** — ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE) and value access functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE) don't perform aggregation. Ranking assigns positional labels based on ordering; value access copies a raw value from a specific row. Neither produces an aggregate that PAC could noise. The detection rule is the same for both: allow passthrough when none of the ORDER BY, PARTITION BY, or accessed column expressions reference a `PROTECTED` column. Block otherwise — ranking on a protected column exposes relative ordering of protected data, and value access on a protected column directly exposes individual values.

**Running/cumulative aggregates** (e.g. `SUM(...) OVER (ORDER BY x ROWS UNBOUNDED PRECEDING)`) are out of scope. Each row produces a different aggregate over a different prefix of the data. Consecutive running sums differ by exactly one row's value, so differencing recovers individual values. This is N separate aggregate queries from a privacy perspective. The DP literature has solutions (binary mechanism, matrix mechanism) that reduce error from O(sqrt(n)) to O(log n), but adapting these to PAC's bitslice model is non-trivial — each of the 64 sub-samples has a different row set, requiring separate tree structures. Research-level problem.

### Key insight: no plan decomposition needed

A full-partition window aggregate like:

```sql
SELECT *, SUM(o_totalprice) OVER (PARTITION BY o_orderstatus) FROM orders;
```

does not need to be decomposed into a GROUP BY + self-join. DuckDB's `WindowConstantAggregator` already computes one aggregate state per partition using the standard `update`/`finalize` callbacks — identical to GROUP BY execution. The `LogicalWindow` operator handles broadcasting the result to all rows in the partition.

So the PAC rewrite is the same transformation PAC already does for GROUP BY aggregates: add the hash expression and swap the aggregate function.

```sql
-- PAC rewritten (orders linked to PU customer via o_custkey)
SELECT *, pac_sum(pac_hash(o_custkey), o_totalprice) OVER (PARTITION BY o_orderstatus) FROM orders;
```

For tables not directly linked to the PU (e.g. lineitem → orders → customer), PAC adds the FK join as usual:

```sql
-- Original
SELECT *, SUM(l_quantity) OVER (PARTITION BY l_returnflag) FROM lineitem;

-- PAC rewritten: join for FK path, swap aggregate inside window
SELECT li.*, pac_sum(pac_hash(o_custkey), l_quantity) OVER (PARTITION BY l_returnflag)
FROM lineitem li JOIN orders ON l_orderkey = o_orderkey;
```

### Privacy argument

Returning all rows alongside a noised aggregate does not leak additional information beyond the aggregate itself. Whether the noised value is returned as 3 rows (GROUP BY) or repeated across 1.5M rows (window), the attacker learns the same 3 numbers. The broadcasting is purely presentational.

The existing `PROTECTED` column mechanism ensures that protected columns cannot appear outside of aggregates in the SELECT list. This check works identically for window functions — if `o_totalprice` is protected, it can only appear inside `pac_sum(...) OVER (...)`, not as a raw column in the output.

### Detection

In `privacy_compatibility_check.cpp`, instead of unconditionally blocking all window functions:

1. Walk the `LogicalWindow` node's expressions
2. For each `BoundWindowExpression`:
   - Check that it is a `WINDOW_AGGREGATE` (not RANK, ROW_NUMBER, LEAD, etc.)
   - Check that it has no ORDER BY clause
   - Check that the frame is the default (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
3. If all window expressions pass, allow PAC compilation
4. Otherwise, block (conservative mode) or skip rewriting

### Plan transformation

The rewrite happens in the same pass as regular aggregate rewriting. When the plan traversal encounters a `LogicalWindow` node:

1. For each window aggregate expression, apply the same transformation as GROUP BY aggregates:
   - Ensure the hash column (from PU key or FK path) is projected into the window operator's input
   - Replace `SUM(x)` → `pac_sum(hash, x)`, `COUNT(*)` → `pac_count(hash)`, etc. inside the `BoundWindowExpression`
2. If FK joins are needed (PU table not in the plan), add them below the `LogicalWindow` node — same as for `LogicalAggregate`

No new aggregate functions, no new noise mechanisms, no plan decomposition.

---

## TODO

- Categorical expressions (`pac_categorical` rewriter)
- CTE handling (materialized vs inlined)
- UNION/UNION ALL compilation
- Correlated subqueries (DELIM_JOIN)
