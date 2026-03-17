# PAC — Automatic Query Privatization for DuckDB

PAC is a DuckDB extension that automatically privatizes SQL queries using the PAC Privacy framework, protecting against Membership Inference Attacks by adding noise to aggregate query results. Unlike Differential Privacy, PAC works automatically and transparently — no per-query analysis by a privacy specialist is needed.

## Install

```sql
INSTALL 'pac';
LOAD 'pac';        
```

## Example: Protecting Employee Salaries

```sql
SET pac_seed = 5;

-- Create a privacy unit table: employees are the entities we protect
CREATE PU TABLE employees (
    id INTEGER,
    department VARCHAR,
    salary DECIMAL(10,2),
    PAC_KEY (id),            -- identifies each privacy unit
    PROTECTED (salary)       -- salary requires aggregation, cannot be projected
);

INSERT INTO employees VALUES
    (1, 'Engineering', 95000),  (2, 'Engineering', 110000),
    (3, 'Engineering', 105000), (4, 'Engineering', 98000),
    (5, 'Sales', 80000),        (6, 'Sales', 72000),
    (7, 'Sales', 78000),        (8, 'Sales', 85000),
    (9, 'Marketing', 85000),    (10, 'Marketing', 90000),
    (11, 'Marketing', 82000),   (12, 'Marketing', 88000);

-- This works: non-protected columns can be freely queried
SELECT department FROM employees;

-- This is blocked: protected columns cannot be projected
SELECT salary FROM employees;
-- Error: protected column 'employees.salary' can only be accessed inside
-- aggregate functions (e.g., SUM, COUNT, AVG, MIN, MAX)

-- This works: aggregate queries on protected columns get automatic noise
SELECT department, AVG(salary)::INTEGER AS avg_salary, COUNT(*) AS count
FROM employees
GROUP BY department;
┌─────────────┬────────────┬───────┐
│ department  │ avg_salary │ count │
├─────────────┼────────────┼───────┤
│ Engineering │     102000 │     2 │
│ Marketing   │      86250 │     3 │
│ Sales       │      78333 │     1 │
└─────────────┴────────────┴───────┘
```

The noised result is close to the real answer but perturbed — an attacker cannot determine whether any specific employee is in the database. 

## Multi-Table Example: Protecting Customer Orders

PAC propagates privacy through join chains via `PAC_LINK`:

```sql
-- The privacy unit: customers
CREATE PU TABLE customers (
    id INTEGER,
    name VARCHAR,
    PAC_KEY (id),
    PROTECTED (name)
);

-- Orders are linked to customers — queries on orders get noised too
CREATE TABLE orders (
    order_id INTEGER,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    PAC_LINK (customer_id) REFERENCES customers(id)
);

-- Lineitem links through orders (deep chain)
CREATE TABLE lineitem (
    item_id INTEGER,
    order_id INTEGER,
    price DECIMAL(10,2),
    PAC_LINK (order_id) REFERENCES orders(order_id)
);

-- PAC automatically follows the chain: lineitem -> orders -> customers
SELECT SUM(price) FROM lineitem;
┌───────────────┐
│  sum(price)   │
├───────────────┤
│       1388.79 │
└───────────────┘
```

## How It Works

1. You declare which table is the **privacy unit** (`CREATE PU TABLE` or `ALTER TABLE SET PU`) and which columns to protect.
2. You link related tables with `PAC_LINK` to propagate privacy through joins.
3. PAC intercepts every aggregate query, hashes each privacy unit's key into a 64-bit value, and uses the bits to create 64 sub-samples. Each aggregate runs on all sub-samples independently, and the final result is the noised median — close to the true answer but safe against membership inference.

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

### Supported Aggregates and Operators

PAC rewrites standard aggregates: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, and `COUNT(DISTINCT)`. Joins, subqueries (correlated and uncorrelated), `UNION`/`UNION ALL`, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT` all work. Window functions and set operations like `EXCEPT`/`INTERSECT` are not yet supported.

### Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `pac_mi` | `1/128` | Mutual information bound (higher = less noise) |
| `pac_seed` | random | Fix seed for reproducible results |
| `pac_noise` | `true` | Toggle noise injection |
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

## Maintainer

This extension is maintained by **@ila** (ilaria@cwi.nl).
