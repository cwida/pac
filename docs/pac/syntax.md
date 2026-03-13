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
