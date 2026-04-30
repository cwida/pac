# Privacy — Automatic Query Privatization for DuckDB

`privacy` is a DuckDB extension that automatically privatizes SQL aggregate queries. It supports two privacy mechanisms:

- **PAC mode** (default): Empirical membership-inference-attack (MIA) resistance via the PAC Privacy framework. Adds calibrated noise derived from the query's own variance over parallel sub-samples. Tight theoretical MI bounds; no per-query sensitivity analysis needed.
- **DP-elastic mode**: Formal (ε,δ)-differential privacy via elastic sensitivity. Computes a per-query sensitivity bound from the join structure (max frequency per table), then injects calibrated Laplace noise. Provides a provable ε-DP guarantee.

Both modes share the same DDL and require no changes to query syntax — the extension rewrites aggregate query plans transparently.

This works on DuckDB v1.5 and beyond. See https://duckdb.org/install to install. Or if you do not want to install anything: this extension is also distributed in WASM, so you can run the examples also in https://shell.duckdb.org in a browser.

## Install

```sql
INSTALL privacy FROM community;
LOAD privacy;
```

## Declaring Privacy Structure

Before running private queries, you declare the privacy structure using three DDL constructs:

**`PRIVACY_KEY (col)`** — Identifies which column is the privacy unit key. The privacy unit (PU) is the entity being protected: one PU = one customer, user, or individual. Composite keys are supported.

**`PRIVACY_LINK (local_col) REFERENCES table(col)`** — Declares that a table's rows belong to a PU via a foreign key. Links propagate privacy through joins: if `lineitem` links to `orders` which links to `customer`, then every lineitem row is tied to a customer (the PU).

**`PROTECTED (col)`** — Marks a column as sensitive. Protected columns cannot be returned directly; they can only be accessed inside aggregate functions (`SUM`, `COUNT`, etc.). On PU tables, all columns are protected by default.

```sql
-- Mark the entity being protected as the privacy unit
ALTER TABLE customer ADD PRIVACY_KEY (c_custkey);
ALTER TABLE customer SET PU;

-- Declare join paths so privacy propagates through the schema
ALTER TABLE orders ADD PRIVACY_LINK (o_custkey) REFERENCES customer(c_custkey);
ALTER TABLE lineitem ADD PRIVACY_LINK (l_orderkey) REFERENCES orders(o_orderkey);

-- Mark sensitive columns (optional on PU tables — all columns protected by default)
ALTER PU TABLE customer ADD PROTECTED (c_name);
ALTER PU TABLE customer ADD PROTECTED (c_acctbal);
```

## Choosing a Mode

| | PAC (default) | DP-elastic |
|---|---|---|
| **Privacy guarantee** | Empirical MIA resistance, theoretical MI bound | Formal (ε,δ)-differential privacy |
| **Noise calibration** | From query variance across 64 sub-samples | From elastic sensitivity of the join tree |
| **Joins** | Missing PU joins injected automatically from PRIVACY_LINK | FK chain derived from PRIVACY_LINK metadata; explicit joins also work |
| **SUM / AVG** | Works automatically | Requires `SET dp_sum_bound` for value clipping |
| **When to use** | General-purpose; works on most queries | When you need a formal DP certificate |

```sql
SET privacy_mode = 'pac';        -- default
SET privacy_mode = 'dp_elastic'; -- formal ε-DP
```

## Example: PAC Mode

Using the TPC-H benchmark. Customers place orders consisting of lineitems. Customer data and purchase history are sensitive.

```sql
-- Generate TPC-H database
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=1);

-- Declare privacy structure
ALTER TABLE customer ADD PRIVACY_KEY (c_custkey);
ALTER TABLE customer SET PU;
ALTER PU TABLE customer ADD PROTECTED (c_custkey);
ALTER PU TABLE customer ADD PROTECTED (c_comment);
ALTER PU TABLE customer ADD PROTECTED (c_acctbal);
ALTER PU TABLE customer ADD PROTECTED (c_name);
ALTER PU TABLE customer ADD PROTECTED (c_address);
ALTER TABLE orders ADD PRIVACY_LINK (o_custkey) REFERENCES customer(c_custkey);
ALTER TABLE lineitem ADD PRIVACY_LINK (l_orderkey) REFERENCES orders(o_orderkey);
ALTER TABLE orders ADD PROTECTED (o_comment);
ALTER TABLE lineitem ADD PROTECTED (l_comment);

-- Protected columns cannot be returned directly
SELECT c_name FROM customer;
-- Error: protected column 'customer.c_name' can only be accessed inside
-- aggregate functions (e.g., SUM, COUNT, AVG, MIN, MAX)

-- PAC rewrites the aggregate and injects the join to orders automatically
-- The noised result is close to the true answer but safe against MIA
SELECT l_returnflag, l_linestatus, SUM(l_extendedprice) FROM lineitem GROUP BY ALL;
┌──────────────┬──────────────┬──────────────────────┐
│ l_returnflag │ l_linestatus │ sum(l_extendedprice) │
│   varchar    │   varchar    │    decimal(38,2)     │
├──────────────┼──────────────┼──────────────────────┤
│ A            │ F            │       57278925373.44 │
│ N            │ F            │        1515625185.28 │
│ N            │ O            │      116295729152.00 │
│ R            │ F            │       57318996705.28 │
└──────────────┴──────────────┴──────────────────────┘

-- The rewritten plan — note pac_noised_sum and the injected join to orders
EXPLAIN SELECT l_returnflag, l_linestatus, SUM(l_extendedprice) FROM lineitem GROUP BY ALL;
┌───────────────────────────┐
│   PERFECT_HASH_GROUP_BY   │
│    ────────────────────   │
│      Groups: #0 #1        │
│        Aggregates:        │
│   pac_noised_sum(#2, #3)  │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         HASH_JOIN         │
│    ────────────────────   │
│      Join Type: INNER     │
│    Conditions: #3 = #0    ├──────────────┐
│      ~6,036,047 rows      │              │
└─────────────┬─────────────┘              │
┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│          SEQ_SCAN         ││         PROJECTION        │
│    ────────────────────   ││    ────────────────────   │
│    memory.main.lineitem   ││ pac_pu=pac_hash(hash(#1)) │
│        l_returnflag       ││             #0            │
│        l_linestatus       ││                           │
│      l_extendedprice      ││                           │
│         l_orderkey        ││                           │
│      ~6,001,215 rows      ││      ~1,500,000 rows      │
└───────────────────────────┘└─────────────┬─────────────┘
                             ┌─────────────┴─────────────┐
                             │          SEQ_SCAN         │
                             │    ────────────────────   │
                             │     memory.main.orders    │
                             │         o_orderkey        │
                             │         o_custkey         │
                             │      ~1,500,000 rows      │
                             └───────────────────────────┘

-- Each query uses a different hash function — results vary slightly per run
SELECT l_returnflag, l_linestatus, SUM(l_extendedprice) FROM lineitem GROUP BY ALL;
┌──────────────┬──────────────┬──────────────────────┐
│ l_returnflag │ l_linestatus │ sum(l_extendedprice) │
│   varchar    │   varchar    │    decimal(38,2)     │
├──────────────┼──────────────┼──────────────────────┤
│ A            │ F            │       58988885442.56 │
│ N            │ F            │        1613206650.88 │
│ N            │ O            │      119904634142.72 │
│ R            │ F            │       58803811778.56 │
└──────────────┴──────────────┴──────────────────────┘

-- Disable noise to see the true answer
SET privacy_noise = false;
SELECT l_returnflag, l_linestatus, SUM(l_extendedprice) FROM lineitem GROUP BY ALL;
┌──────────────┬──────────────┬──────────────────────┐
│ l_returnflag │ l_linestatus │ sum(l_extendedprice) │
│   varchar    │   varchar    │    decimal(38,2)     │
├──────────────┼──────────────┼──────────────────────┤
│ A            │ F            │       56586554400.73 │
│ N            │ F            │        1487504710.38 │
│ N            │ O            │      114935210409.19 │
│ R            │ F            │       56568041380.90 │
└──────────────┴──────────────┴──────────────────────┘
```

## Example: DP-Elastic Mode

Same dataset and privacy structure. No explicit joins are needed — the extension derives the FK chain from PRIVACY_LINK metadata automatically.

```sql
-- (same DDL as above — reuse the privacy structure already declared)

SET privacy_mode = 'dp_elastic';
SET dp_epsilon = 1.0;
SET dp_delta = 1e-6;
SET dp_sum_bound = 100000;  -- max absolute l_extendedprice per row, for clipping
SET privacy_seed = 42;

-- No joins needed: the extension derives lineitem → orders → customer from PRIVACY_LINK metadata
SELECT l_returnflag, l_linestatus, SUM(l_extendedprice) FROM lineitem GROUP BY ALL ORDER BY ALL;
┌──────────────┬──────────────┬──────────────────────┐
│ l_returnflag │ l_linestatus │ sum(l_extendedprice) │
│   varchar    │   varchar    │    decimal(38,2)     │
├──────────────┼──────────────┼──────────────────────┤
│ A            │ F            │       56589946031.64 │
│ N            │ F            │        1534396959.73 │
│ N            │ O            │      114528541709.94 │
│ R            │ F            │       56505612137.93 │
└──────────────┴──────────────┴──────────────────────┘
```

The extension computes elastic sensitivity as the product of per-table max frequencies along the `lineitem → orders → customer` chain (via auxiliary `MAX(COUNT(*))` queries, not by executing the join), clips each value to `[-dp_sum_bound, dp_sum_bound]`, and injects `Laplace(sensitivity/ε)` noise into the aggregate result. This gives a formal (ε,δ)-DP guarantee at the privacy-unit (customer) level.

## How It Works

### PAC Mode

1. You declare which table is the **privacy unit** and which columns to protect.
2. You link related tables with `PRIVACY_LINK` to propagate privacy through joins.
3. The extension intercepts every aggregate query, hashes each PU key into a 64-bit value, and uses the bits to create 64 sub-samples (possible worlds). Each aggregate runs on all sub-samples independently. The final result is taken from one secret world, noised using the variance across all worlds. Each query uses a different hash function and selects a different secret world.

PAC bounds the mutual information (MI) between the query output and whether any individual is in the database. The `pac_mi` parameter sets this bound: at the default `pac_mi = 1/128`, an attacker gains at most 1/128 nats of information per query. **PAC is not differential privacy** — it provides empirical MIA resistance with theoretical MI bounds, not a formal ε-DP guarantee.

### DP-Elastic Mode

Uses elastic sensitivity (Flex, Johnson/Near/Song VLDB 2018) to derive a per-query upper bound on local sensitivity from the join structure. The sensitivity equals the product of per-table max frequencies along the FK join chain — computed from auxiliary `MAX(COUNT(*))` queries, not by executing the user's join. Each `SUM`/`AVG` input is clipped to `[-dp_sum_bound, dp_sum_bound]` and calibrated Laplace noise is added to the aggregate result.

`AVG(x)` is rewritten to noised `SUM(x) / COUNT(*)`. With *k* user-visible aggregates in a query the budget is split evenly under sequential composition — each consumes ε/k, and AVG further splits its share into ε/(2k) per component, so the total cost across the query stays at ε. `WHERE`, `HAVING`, and `FILTER (WHERE …)` aggregate clauses are honoured: the predicates run before noise is added (or, for `HAVING`, against the already-noised value, which is post-processing and DP-safe). When `privacy_min_group_count` is set, low-count groups are dropped via τ-thresholding (a filter on the noised value, also post-processing).

This gives a formal (ε,δ)-DP guarantee at the **user level** — neighboring datasets differ by removing all rows belonging to one privacy unit across all linked tables. Requires δ > 0 for smooth elastic sensitivity (tighter noise); set `dp_delta = 0` for pure ε-DP with global elastic sensitivity.

## Settings

| Setting | Default | Mode | Description |
|---------|---------|------|-------------|
| `privacy_mode` | `pac` | both | Active privacy mechanism: `pac` or `dp_elastic` |
| `privacy_seed` | random | both | Fix seed for reproducible results |
| `privacy_noise` | `true` | both | Toggle noise injection |
| `privacy_min_group_count` | `NULL` | both | Suppress GROUP BY cells below this count (PAC: low-SNR suppression; DP: τ-thresholding) |
| `pac_mi` | `1/128` | pac | Mutual information bound (higher = less noise, more leakage) |
| `pac_ctas` | `true` | pac | Propagate PAC metadata through CTAS |
| `pac_diffcols` | `NULL` | pac | [Utility diff](docs/pac/utility.md): compare noised vs exact results |
| `dp_epsilon` | `1.0` | dp_elastic | Privacy budget ε |
| `dp_delta` | `1e-6` | dp_elastic | Failure probability δ (0 = pure DP) |
| `dp_sum_bound` | required | dp_elastic | Clipping bound for `SUM`/`AVG` inputs: values are clipped to `[-bound, bound]` |

## SQL Reference

### Supported Aggregates and Operators

PAC mode supports `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, and `COUNT(DISTINCT)`. DP-elastic supports `SUM`, `COUNT`, and `AVG` — `MIN` and `MAX` have unbounded sensitivity in DP and are not supported. Joins, subqueries (correlated and uncorrelated), `UNION`/`UNION ALL`, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT` all work. Window functions and `EXCEPT`/`INTERSECT` are not yet supported.

### DDL Quick Reference

```sql
-- Create a new PU table
CREATE PU TABLE t (col1 INT, col2 INT, PRIVACY_KEY (col1), PROTECTED (col2));

-- Or convert an existing table
ALTER TABLE t ADD PRIVACY_KEY (col1);
ALTER TABLE t SET PU;
ALTER TABLE t UNSET PU;

-- Non-PU tables: use ALTER TABLE
ALTER TABLE orders ADD PRIVACY_LINK (fk_col) REFERENCES t(col1);
ALTER TABLE orders ADD PROTECTED (sensitive_col);

-- PU tables: use ALTER PU TABLE
ALTER PU TABLE t ADD PROTECTED (col2);
ALTER PU TABLE t DROP PROTECTED (col2);
```

`CREATE TABLE AS SELECT` from a PU table automatically propagates privacy metadata to the new table (see [syntax docs](docs/pac/syntax.md#derived-tables-create-table-as-select)).

## Documentation

For implementation details, see the [docs/](docs/) folder: \
[Parser](docs/pac/syntax.md) | [Query Operators](docs/pac/query_operators.md) | [PAC Functions](docs/pac/functions.md) | [Runtime Checks](docs/pac/runtime_checks.md) | [Tests](docs/test/README.md) | [Benchmarks](docs/benchmark/README.md)

## Literature

I. Battiston, D. Yuan, X. Zhu, P. Boncz. [SIMD-PAC-DB: Pretty Performant PAC Privacy](https://arxiv.org/abs/2603.15023). 2026.

N. Johnson, J.M. Near, D. Song. [Towards Practical Differential Privacy for SQL Queries](https://arxiv.org/abs/1706.09479) (Flex / Elastic Sensitivity). VLDB 2018.

R. Wilson, C. Zhang, W. Lam, D. Desfontaines, D. Simmons-Marengo, B. Gipson. [Differentially Private SQL with Bounded User Contribution](https://arxiv.org/abs/1909.01917). PoPETs 2020.

```bibtex
@misc{battiston2026simdpacdbprettyperformantpac,
      title={SIMD-PAC-DB: Pretty Performant PAC Privacy},
      author={Ilaria Battiston and Dandan Yuan and Xiaochen Zhu and Peter Boncz},
      year={2026},
      eprint={2603.15023},
      archivePrefix={arXiv},
      primaryClass={cs.DB},
      url={https://arxiv.org/abs/2603.15023},
}
```

## Maintainer

This extension is maintained by **@ila** (ilaria@cwi.nl).
