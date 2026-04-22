# PAC SQL Syntax

## DDL Statements

### `CREATE PU TABLE`

Declares a table as a Privacy Unit — the atomic entity whose presence/absence is protected.

```sql
CREATE PU TABLE customer (
    c_custkey INTEGER,
    c_name VARCHAR,
    c_address VARCHAR,
    PAC_KEY (c_custkey),
    PROTECTED (c_name, c_address)
);
```

**Clauses:**
- `PU` keyword after `CREATE` marks the table as a privacy unit.
- `PAC_KEY (col1, col2, ...)` identifies the privacy unit. Composite keys are supported. Metadata-only — no constraint enforcement overhead.
- `PROTECTED (col1, col2, ...)` lists columns that require aggregation. If omitted, all columns of a PU table are considered protected.
- `PAC_LINK (local_col) REFERENCES table(ref_col)` declares a join relationship for privacy propagation (see below).

**Validation:**
- `CREATE PU TABLE` requires `PAC_KEY`.
- PU tables cannot link to other PU tables.

Metadata is saved to a JSON file alongside the database.

### `CREATE TABLE` with PAC Clauses

Non-PU tables can declare `PAC_LINK` and `PROTECTED` columns at creation time:

```sql
CREATE TABLE orders (
    o_orderkey INTEGER,
    o_custkey INTEGER,
    o_totalprice DECIMAL,
    PAC_LINK (o_custkey) REFERENCES customer(c_custkey),
    PROTECTED (o_totalprice)
);
```

### `ALTER [PU] TABLE`

Adds or removes PAC metadata on existing tables. Metadata-only — no DDL is executed. Use `ALTER PU TABLE` for privacy unit tables and `ALTER TABLE` for non-PU tables.

```sql
-- Add metadata to a PU table.
ALTER PU TABLE customer ADD PROTECTED (c_name, c_address);

-- Add metadata to a non-PU table.
ALTER TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer(c_custkey);
ALTER TABLE orders ADD PROTECTED (o_totalprice);

-- Convert a table to PU.
ALTER TABLE t ADD PAC_KEY (id);
ALTER TABLE t SET PU;

-- Remove PU status.
ALTER TABLE t UNSET PU;

-- Remove metadata.
ALTER TABLE orders DROP PAC_LINK (o_custkey);
ALTER TABLE orders DROP PROTECTED (o_totalprice);
```

**Validation:**
- All referenced columns must exist in the table.
- `ALTER PU TABLE` on a non-PU table (and vice versa) throws an error.
- Duplicate `PROTECTED` columns throw an error.
- Conflicting `PAC_LINK` declarations (same local columns to different targets) throw an error. Exact duplicates are silently skipped.
- PU tables cannot link to other PU tables.
- `SET PU` requires a `PAC_KEY` to be defined first.
- `DROP` requires the specified link or column to exist in metadata.

### Derived Tables (`CREATE TABLE AS SELECT`)

When a `CREATE TABLE AS SELECT` (CTAS) sources from a PU or derived table, PAC automatically propagates metadata to the new table, marking it as a **derived table**:

```sql
-- Schema copy: PAC_KEY(id) and PROTECTED(value) are propagated
CREATE TABLE copy AS SELECT * FROM pu_table LIMIT 0;

-- Renames are tracked: 'amount' inherits protected status from 'value'
CREATE TABLE renamed AS SELECT id, value AS amount FROM pu_table LIMIT 0;

-- Aggregate CTAS: only GROUP BY pass-throughs stay protected
-- 'total' is NOT protected (already noised by PAC), 'grp' passes through unchanged
CREATE TABLE summary AS SELECT grp, SUM(value) AS total FROM pu_table GROUP BY grp;
```

Derived tables are **transparent to SELECT** (no PAC checks or noise) but trigger PAC noise when used as a source in DML statements containing aggregates (e.g., `INSERT INTO ... SELECT ... GROUP BY`).

Controlled by `SET pac_ctas = true` (default). Disable with `SET pac_ctas = false`.

### `DROP TABLE`

When a table with PAC metadata is dropped, the `PACDropTableRule` optimizer extension removes the table's metadata, removes `PAC_LINK`s from other tables that reference it, and saves the updated metadata.

## `PAC_KEY` and `PAC_LINK`

`PAC_KEY` and `PAC_LINK` are the only mechanisms for identifying privacy units and join chains. Database-level `PRIMARY KEY` and `FOREIGN KEY` constraints are ignored by the PAC compiler.

- `PAC_KEY` identifies the privacy unit columns. Metadata-only, no constraint enforcement.
- `PAC_LINK` defines join paths between tables. Metadata-only, no referential integrity checks. Composite keys are supported. All columns named in a `PAC_LINK` declaration are automatically considered `PROTECTED`.
- The compiler uses `PAC_LINK`s to determine the path from queried tables to the PU, decide which tables to join for hash computation, and validate that query joins use the declared `PAC_LINK` columns.

## Metadata Persistence

PAC metadata is stored in a JSON file named `pac_metadata_<db_name>_<schema_name>.json` alongside the database file. For in-memory databases, no file is saved. The file is written after every PAC DDL operation. On database load, the extension reads the file and populates the in-memory `PACMetadataManager`.

## Query Validation

When a `SELECT` query arrives, the `PACRewriteRule` optimizer checks if the plan scans any PAC-metadata tables, follows `PAC_LINK` paths to find reachable PUs, and classifies the query as:

- **Inconspicuous:** no PU or PAC-linked table referenced — passed through unchanged.
- **Rejected:** references protected data but violates constraints (see `query_operators.md`).
- **Rewritable:** valid for PAC compilation — aggregates are transformed.

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

PAC rewrites standard aggregates: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, and their `COUNT(DISTINCT)` variants. Other aggregates are not (yet) cupported, but could. Most SQL operators are supported, exceptions are RECURSIVE CTEs, Window functions and set operations like `EXCEPT`/`INTERSECT`.

There is some preliminary support for using multiple PU tables inside the same schema and query (it works but needs more study).

### Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `pac_mi` | `1/128` | Mutual information bound (higher = less noise) |
| `pac_seed` | random | Fix seed for reproducible results |
| `pac_noise` | `true` | Toggle noise injection |
| `pac_diffcols` | `NULL` | [Utility diff](docs/pac/utility.md): compare noised vs exact results |
| `pac_utility_threshold` | `NULL` (disabled) | Z-score threshold for [utility NULLing](runtime_checks.md#utility-nulling): probabilistically NULL result cells with low signal-to-noise ratio. Set to e.g. `4` to enable. |
| `pac_clip_support` | `NULL` (disabled) | Enable clipping: aggregate per-PU values with outlier elimination at the given support threshold |

