# PAC Parser

The PAC parser is a DuckDB parser extension that intercepts SQL statements to handle PAC-specific DDL syntax. It validates metadata, stores it in the `PACMetadataManager`, and strips PAC clauses before passing the cleaned SQL to DuckDB's standard parser.

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

**Clauses**:
- `PU` keyword after `CREATE`: marks the table as a privacy unit (`is_privacy_unit = true`).
- `PAC_KEY (col1, col2, ...)`: identifies the PU (composite keys supported). Metadata-only, no constraint enforcement overhead.
- `PROTECTED (col1, col2, ...)`: columns containing sensitive data that require aggregation. By default, ALL columns of a PU table are considered protected. The `PROTECTED` clause overrides this to list specific columns.
- `PAC_LINK (local_col) REFERENCES table(ref_col)`: join relationship for privacy propagation (see below).

**Validation**:
- `CREATE PU TABLE` requires `PAC_KEY`
- PAC tables cannot link to other PAC tables (no cycles)

Metadata is saved to a JSON file alongside the database.

### `CREATE TABLE` (with PAC clauses)

Non-PU tables can declare `PAC_LINK`s and `PROTECTED` columns at creation time:

```sql
CREATE TABLE orders (
    o_orderkey INTEGER,
    o_custkey INTEGER,
    o_totalprice DECIMAL,
    PAC_LINK (o_custkey) REFERENCES customer(c_custkey),
    PROTECTED (o_totalprice)
);
```

### `ALTER [PU] TABLE ADD`

Adds PAC metadata to existing tables. Metadata-only operation (no DDL executed).

Use `ALTER PU TABLE` for privacy unit tables and `ALTER TABLE` for non-PU tables — the keyword must match the table's PU status.

```sql
-- On a PU table (created with CREATE PU TABLE or after SET PU)
ALTER PU TABLE customer ADD PROTECTED (c_name, c_address);

-- On a non-PU table
ALTER TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer(c_custkey);
ALTER TABLE orders ADD PROTECTED (o_totalprice, o_comment);

-- PAC_KEY on a non-PU table (preparation for SET PU)
ALTER TABLE t ADD PAC_KEY (id);
ALTER TABLE t SET PU;   -- now t is a PU
```

**Validation**:
- All columns must exist in the table
- `ALTER PU TABLE` on a non-PU table throws an error (and vice versa)
- `PROTECTED` columns can't be added twice (throws error, not idempotent)
- `PAC_LINK`s can't conflict (same local columns to different targets)
- Exact duplicate links are silently skipped (idempotent)
- PAC tables cannot link to other PAC tables

### `ALTER [PU] TABLE DROP`

Removes PAC metadata. Metadata-only operation. Use `ALTER PU TABLE` for PU tables, `ALTER TABLE` for non-PU tables.

```sql
ALTER TABLE orders DROP PAC_LINK (o_custkey);
ALTER TABLE orders DROP PROTECTED (o_totalprice);
```

**Validation**:
- Table must have existing PAC metadata
- The specified link/column must exist in metadata

### `ALTER TABLE SET/UNSET PU`

Toggle privacy unit status on an existing table. `SET PU` requires a `PAC_KEY` to be defined first:

```sql
ALTER PU TABLE customer ADD PAC_KEY (c_custkey);
ALTER TABLE customer SET PU;    -- marks as privacy unit
ALTER TABLE customer UNSET PU;  -- removes privacy unit status
```

### `DROP TABLE`

When a table with PAC metadata is dropped, the `PACDropTableRule` optimizer extension:
1. Removes the table's metadata from `PACMetadataManager`
2. Removes `PAC_LINK`s from other tables that reference the dropped table
3. Saves updated metadata to the JSON file

## `PAC_LINK`

`PAC_LINK`s define join relationships for privacy propagation. Unlike database-enforced FKs:
- **Metadata-only**: no constraint checking overhead at insert/update time
- **Composite keys supported**: `PAC_LINK (col1, col2) REFERENCES table(ref1, ref2)`
- **Must be acyclic**: PAC tables cannot link to other PAC tables

All local and referenced columns named in a `PAC_LINK` declaration are automatically considered `PROTECTED`.

The compiler uses `PAC_LINK`s to:
1. Determine the `PAC_LINK` path from queried tables to the PU
2. Decide which tables to join for hash computation
3. Validate that joins in the query use exact `PAC_LINK` columns

## `PAC_KEY` and `PAC_LINK`

`PAC_KEY` and `PAC_LINK` are the **only** mechanisms for identifying privacy units and join chains. Database-level `PRIMARY KEY` and `FOREIGN KEY` constraints are ignored by the PAC compiler — they have no effect on privacy propagation.

- `PAC_KEY` identifies the privacy unit columns (metadata-only, no constraint enforcement)
- `PAC_LINK` defines join paths between tables (metadata-only, no referential integrity checks)
- `ALTER TABLE SET PU` requires a `PAC_KEY` to be defined first (no rowid fallback)

## Metadata Persistence

PAC metadata is stored in a JSON file named `pac_metadata_<db_name>.json` alongside the database file. For in-memory databases, no file is saved.

The file is written after every PAC DDL operation (`CREATE`, `ALTER`, `DROP`). On database load, the PAC extension reads the metadata file and populates the in-memory `PACMetadataManager`.

## Query Validation Flow

When a `SELECT` query arrives (not handled by the parser extension), the `PACRewriteRule` optimizer:

1. Checks if the plan scans any PAC-metadata tables
2. Follows `PAC_LINK` paths to find reachable PUs
3. Classifies the query:
   - **Inconspicuous**: no PU or PAC-linked table referenced -> pass through
   - **Rejected**: references protected data but violates constraints (see `query_operators.md`)
   - **Rewritable**: valid for PAC compilation -> transform aggregates
