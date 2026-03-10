# PAC — Automatic Query Privatization

A DuckDB extension that automatically privatizes SQL queries using the PAC Privacy framework. PAC protects against Membership Inference Attacks by adding noise to aggregate query results. Unlike Differential Privacy, PAC works automatically — no per-query analysis by a privacy specialist is needed.

## Build

```bash
git clone --recurse-submodules https://github.com/cwida/pac.git
cd pac
GEN=ninja make        # release build
make debug            # debug build
make test             # run tests
```

See [docs/test/README.md](docs/test/README.md) for test details.

## SQL Syntax

### CREATE PU TABLE

```sql
CREATE PU TABLE employees (
    id INTEGER,
    department VARCHAR,
    salary DECIMAL(10,2),
    PAC_KEY (id),
    PROTECTED (salary)
);
```

`PAC_KEY` defines the privacy unit identifier (composite keys supported). `PROTECTED` marks columns restricted to aggregate-only access. If no `PROTECTED` clause is given, all columns are treated as protected.

### ALTER PU TABLE ADD / DROP

```sql
ALTER PU TABLE orders ADD PAC_KEY (order_id);
ALTER PU TABLE orders ADD PAC_LINK (customer_id) REFERENCES customers(id);
ALTER PU TABLE orders ADD PROTECTED (total_amount);
ALTER PU TABLE orders DROP PAC_LINK (customer_id);
ALTER PU TABLE orders DROP PROTECTED (total_amount);
```

### ALTER TABLE SET PU / UNSET PU

```sql
ALTER TABLE customers SET PU;      -- requires PAC_KEY to be defined first
ALTER TABLE customers UNSET PU;
```

### Metadata Pragmas

```sql
PRAGMA save_pac_metadata('pac_metadata.json');
PRAGMA load_pac_metadata('pac_metadata.json');
PRAGMA clear_pac_metadata;
```

## PAC_LINK

`PAC_LINK` declares a join relationship between a regular table and a privacy unit, enabling privacy propagation. When a table has a `PAC_LINK`, queries on it are automatically noised using the linked privacy unit's key. Without a `PAC_LINK`, the PU table must appear in the query for PAC to derive the privacy hash.

```sql
CREATE TABLE orders (
    order_id INTEGER,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    PAC_LINK (customer_id) REFERENCES customers(id)
);
```

## Supported Operators

| Feature | Status | Notes |
|---------|--------|-------|
| SUM, COUNT, AVG, MIN, MAX | Supported | Transformed to pac_sum, pac_count, etc. |
| COUNT(DISTINCT col) | Supported | Distinct within aggregates |
| Other aggregates | Disallowed | Custom aggregates not supported |
| Window functions | Disallowed | OVER clauses not supported |
| JOIN | Supported | All joins work |
| Subqueries | Supported | Both correlated and uncorrelated |
| UNION / UNION ALL | Supported | Set union operations work |
| EXCEPT | Disallowed | Not implemented yet |
| INTERSECT | Disallowed | Not implemented yet |
| ORDER BY, LIMIT | Supported | On final query only |
| GROUP BY | Supported | Cannot group by protected columns |
| Projecting PROTECTED columns | Disallowed | Protected columns require aggregation |
| Projecting non-protected columns | Allowed | If PROTECTED is defined on other columns |

## Configuration Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `pac_enabled` | BOOLEAN | true | Enable/disable PAC query rewriting |
| `pac_seed` | BIGINT | (random) | Deterministic RNG seed for reproducible results |
| `pac_mi` | DOUBLE | 0.0 | Privacy parameter |
| `pac_noise` | BOOLEAN | true | Whether to add PAC noise |
| `pac_diffcols` | INTEGER | NULL | [Utility diff](docs/pac/utility.md): compare noised vs exact result (reports error %) |
| `pac_deterministic_noise` | BOOLEAN | false | Use deterministic noise (for testing) |

To eliminate noise and make things very deterministic (sometimes used in tests):
```sql
SET pac_seed = 42;
SET pac_mi = 0;
SET threads = 1;
```

## Example

```sql
-- Create a privacy unit with protected columns
CREATE PU TABLE employees (
    id INTEGER,
    department VARCHAR,
    salary DECIMAL(10,2),
    PAC_KEY (id),
    PROTECTED (salary)
);

INSERT INTO employees VALUES (1, 'Engineering', 100000), (2, 'Sales', 80000);

-- Non-protected columns can be projected freely
SELECT id, department FROM employees;

-- Protected columns require aggregation
SELECT salary FROM employees;  -- Error: protected column

-- Aggregate queries on protected columns work (with noise)
SELECT AVG(salary) FROM employees;
SELECT department, AVG(salary) FROM employees GROUP BY department;
```

## Documentation

| Document | Description |
|----------|-------------|
| [docs/pac/README.md](docs/pac/README.md) | PAC algorithm implementation details |
| [docs/pac/utility.md](docs/pac/utility.md) | Utility diff mode for measuring accuracy |
| [docs/pac/pac_functions.md](docs/pac/pac_functions.md) | PAC aggregate functions |
| [docs/pac/pac_parser.md](docs/pac/pac_parser.md) | PAC SQL parser |
| [docs/pac/query_operators.md](docs/pac/query_operators.md) | Query operator handling |
| [docs/pac/runtime_checks.md](docs/pac/runtime_checks.md) | Runtime privacy checks |
| [docs/pac/ptracking.md](docs/pac/ptracking.md) | Privacy tracking |
| [docs/test/README.md](docs/test/README.md) | Running SQL and C++ tests |
| [docs/benchmark/README.md](docs/benchmark/README.md) | Benchmark overview |

## Literature

WIP: We will add references to relevant papers and resources on PAC privacy and privacy-preserving query processing here.

## Maintainer

This extension is maintained by **@ila** (ilaria@cwi.nl).

## License

See `LICENSE` in the repository root.
