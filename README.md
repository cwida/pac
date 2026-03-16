# PAC — Automatic Query Privatization for DuckDB

PAC is a DuckDB extension that automatically privatizes SQL queries using the PAC Privacy framework, protecting against Membership Inference Attacks by adding noise to aggregate query results. Unlike Differential Privacy, PAC works automatically and transparently — no per-query analysis by a privacy specialist is needed.

This works on DuckDB v1.5 and beyond. See https://duckdb.org/install to install. Or if you do not want to install anything: this extension is also distributed in WASM, so you can run the examples also in https://shell.duckdb.org in a browser. 

## Install

```sql
INSTALL pac FROM COMMUNITY;
LOAD pac;        
```

## Example: Protecting Customers and their Purchasing Behavior

In this example we use standard the TPC-H data warehouse benchmark setup. In this warehouse, customers place orders, consisting of lineitems -- which are certain quantities of parts, provided by part suppliers. In this setup, we consider personal customer data sensitive, but also consider their purchase history sensitive. Note that not all aspects of customers get protection under the below scenario: for instance, we consider aggregating customers by market segment (c_mksegment is a non-protected column) non-senstive, nor aggregating by nation or region.

```sql
# generate TPC-H database
INSTALL tpch;
LOAD tpch;
call dbgen(sf=1);

-- Mark customer as the privacy unit, after it was created by dbgen
ALTER TABLE customer ADD PAC_KEY (c_custkey);
ALTER TABLE customer SET PU;

-- Protected columns in customer table
ALTER PU TABLE customer ADD PROTECTED (c_custkey);
ALTER PU TABLE customer ADD PROTECTED (c_comment);
ALTER PU TABLE customer ADD PROTECTED (c_acctbal);
ALTER PU TABLE customer ADD PROTECTED (c_name);
ALTER PU TABLE customer ADD PROTECTED (c_address);

-- Orders -> Customer and Lineitem->Orders links
ALTER TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer(c_custkey);
ALTER TABLE lineitem ADD PAC_LINK (l_orderkey) REFERENCES orders(o_orderkey);

-- Protect the comment columns, as they may include customer-specific notes
ALTER TABLE orders ADD PROTECTED (o_comment);
ALTER TABLE lineitem ADD PROTECTED (l_comment);

-- protected columns cannot be returned
SELECT c_name FROM employees;
-- Error: protected column 'customer.c_name' can only be accessed inside
-- aggregate functions (e.g., SUM, COUNT, AVG, MIN, MAX)

--The noised result is close to the real answer but perturbed — an attacker cannot determine whether any specific employee is in the database. 
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

-- PAC rewrites the query plan automatically
-- Note that: (1) the GROUP_BY uses a pac_noised_sum(#2, #3), not a standard sum()
--            (2) while the query only mentions lineitem, pac joins with orders to get c_cuskey (the PU key)
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

-- every time the result is noised a bit differently (the database is resampled)
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

-- the unnoised correct answer:
set pac_noise = false;
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

-- measure utility (MAPE, recall, precision)
set pac_noise = true;
set pac_diffcols = 2; -- two key columns l_returnflag, l_linestatus,
SELECT l_returnflag, l_linestatus, SUM(l_extendedprice) FROM lineitem GROUP BY ALL;
utility=0.510000 recall=1.000000 precision=1.000000 (=4 -0 +0)
┌──────────────┬──────────────┬──────────────────────┐
│ l_returnflag │ l_linestatus │ sum(l_extendedprice) │
│   varchar    │   varchar    │    decimal(38,2)     │
├──────────────┼──────────────┼──────────────────────┤
│ A            │ F            │                 0.56 │
│ N            │ F            │                 0.13 │
│ N            │ O            │                 0.58 │
│ R            │ F            │                 0.77 │
└──────────────┴──────────────┴──────────────────────┘
```

## How It Works

1. You declare which table is the **privacy unit** (`CREATE PU TABLE` or `ALTER TABLE SET PU`) and which columns to protect.
2. You link related tables with `PAC_LINK` to propagate privacy through joins.
3. PAC intercepts every aggregate query, hashes each privacy unit's key into a 64-bit value, and uses the bits to create 64 sub-samples (possible worlds). Each aggregate runs on all sub-samples independently, and the final result is taken from one secret world but  noised using the variance over all possible worlds — close to the true answer but safe against membership inference. Each query uses a different hash function, and choses a different secret world to return answers from, making such attacks harder.

### Mutual Information (MI)

PAC bounds the mutual information (MI) between the query output and whether any specific individual is in the database. The `pac_mi` parameter sets this bound: at the default `pac_mi = 0.0`, an attacker observing PAC query results gains zero additional information about any individual's presence. Higher values relax the bound, allowing less noise (more accurate results) at the cost of more information leakage.

## SQL Reference

### Defining Privacy Units

```sql
-- Create a new PU table with PAC_KEY and optional PROTECTED columns
CREATE PU TABLE t (col1 INT, col2 INT, PAC_KEY (col1), PROTECTED (col2));

-- Or convert an existing table to PU
ALTER TABLE t ADD PAC_KEY (col1);       -- PAC_KEY on non-PU table (prep for SET PU)
ALTER TABLE t SET PU;                   -- mark as PU (requires PAC_KEY)
ALTER TABLE t UNSET PU;                 -- remove PU status

-- Add metadata to non-PU tables (use ALTER TABLE)
ALTER TABLE orders ADD PAC_LINK (fk_col) REFERENCES t(col1);
ALTER TABLE orders ADD PROTECTED (col2);

-- Add metadata to PU tables (use ALTER PU TABLE)
ALTER PU TABLE t ADD PROTECTED (col2);
```

`PAC_KEY` identifies the privacy unit (composite keys supported). `PAC_LINK` declares a join path for privacy propagation. `PROTECTED` restricts columns to aggregate-only access — if omitted on a PU table, all columns are protected. Use `ALTER PU TABLE` for PU tables and `ALTER TABLE` for non-PU tables.

`CREATE TABLE AS SELECT` from a PU table automatically propagates PAC metadata to the new table (see [syntax docs](docs/pac/syntax.md#derived-tables-create-table-as-select)).

### Supported Aggregates and Operators

PAC rewrites standard aggregates: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, and `COUNT(DISTINCT)`. Joins, subqueries (correlated and uncorrelated), `UNION`/`UNION ALL`, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT` all work. Window functions and set operations like `EXCEPT`/`INTERSECT` are not yet supported.

### Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `pac_mi` | `1/128` | Mutual information bound (higher = less noise) |
| `pac_seed` | random | Fix seed for reproducible results |
| `pac_noise` | `true` | Toggle noise injection |
| `pac_ctas` | `true` | Propagate PAC metadata through CTAS |
| `pac_diffcols` | `NULL` | [Utility diff](docs/pac/utility.md): compare noised vs exact results |

## Documentation

For implementation details, see the [docs/](docs/) folder:
[Parser](docs/pac/syntax.md) | [Query Operators](docs/pac/query_operators.md) | [PAC Functions](docs/pac/functions.md) | [Runtime Checks](docs/pac/runtime_checks.md) | [Tests](docs/test/README.md) | [Benchmarks](docs/benchmark/README.md)

## Literature

I. Battiston, D. Yuan, X. Zhu, P. Boncz. [SIMD-PAC-DB: Pretty Performant PAC Privacy](https://arxiv.org/abs/2603.15023). 2026. 

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
Note to reviewers: the above is updated w.r.t. the paper under submission: evaluation subsections 6.3+6.4 have been improved.

## Maintainer

This extension is maintained by **@ila** (ilaria@cwi.nl).
