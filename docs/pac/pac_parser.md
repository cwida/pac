# PAC Parser

The PAC parser is a DuckDB parser extension that intercepts SQL statements to handle PAC-specific DDL syntax. It validates metadata, stores it in the `PACMetadataManager`, and strips PAC clauses before passing the cleaned SQL to DuckDB's standard parser.

## Architecture

The parser has two entry points:

1. **PACParseFunction**: called for every query. Attempts to match PAC DDL patterns. Returns empty result if no PAC syntax found (DuckDB's normal parser handles it).
2. **PACPlanFunction**: converts parsed PAC statements into execution plans. Validates metadata, stores it, and executes any underlying DDL.

Supporting modules:
- `pac_parser_helpers.cpp`: regex-based clause extraction and SQL stripping
- `pac_metadata_manager.cpp`: in-memory metadata storage and retrieval
- `pac_metadata_serialization.cpp`: JSON serialization to/from disk

## DDL Statements

### CREATE PU TABLE

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
- `PU` keyword after `CREATE`: marks the table as a privacy unit (`is_privacy_unit = true`)
- `PAC_KEY (col1, col2, ...)`: identifies the PU (composite keys supported). Alternative to SQL `PRIMARY KEY` — PAC keys are metadata-only, no constraint enforcement overhead
- `PROTECTED (col1, col2, ...)`: columns containing sensitive data that require aggregation. By default, ALL columns of a PU table are considered protected. The `PROTECTED` clause overrides this to list specific columns.
- `PAC_LINK (local_col) REFERENCES table(ref_col)`: FK relationship for privacy propagation (see below)

**Validation**:
- `CREATE PU TABLE` requires either `PAC_KEY` or `PRIMARY KEY`
- PAC tables cannot link to other PAC tables (no cycles)

**Processing**: PAC clauses are stripped, `CREATE PU TABLE` becomes `CREATE TABLE`, and the clean SQL is executed via a separate connection (to avoid deadlocks). Metadata is saved to a JSON file alongside the database.

### CREATE TABLE (with PAC clauses)

Non-PU tables can declare PAC LINKs and PROTECTED columns at creation time:

```sql
CREATE TABLE orders (
    o_orderkey INTEGER,
    o_custkey INTEGER,
    o_totalprice DECIMAL,
    PAC_LINK (o_custkey) REFERENCES customer(c_custkey),
    PROTECTED (o_totalprice)
);
```

### ALTER PU TABLE ADD

Adds PAC metadata to existing tables. Metadata-only operation (no DDL executed).

```sql
-- Add PAC KEY
ALTER PU TABLE customer ADD PAC_KEY (c_custkey);

-- Add PAC LINK (FK relationship)
ALTER PU TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer(c_custkey);

-- Add protected columns
ALTER PU TABLE orders ADD PROTECTED (o_totalprice, o_comment);
```

**Validation**:
- All columns must exist in the table
- Protected columns can't be added twice (throws error, not idempotent)
- PAC LINKs can't conflict (same local columns to different targets)
- Exact duplicate links are silently skipped (idempotent)
- PAC tables cannot link to other PAC tables

### ALTER PU TABLE DROP

Removes PAC metadata. Metadata-only operation.

```sql
-- Drop a PAC LINK
ALTER PU TABLE orders DROP PAC_LINK (o_custkey);

-- Drop protected columns
ALTER PU TABLE orders DROP PROTECTED (o_totalprice);
```

**Validation**:
- Table must have existing PAC metadata
- The specified link/column must exist in metadata

### ALTER TABLE SET/UNSET PU

Toggle privacy unit status on an existing table:

```sql
ALTER TABLE customer SET PU;    -- marks as privacy unit
ALTER TABLE customer UNSET PU;  -- removes privacy unit status
```

### DROP TABLE

When a table with PAC metadata is dropped, the `PACDropTableRule` optimizer extension:
1. Removes the table's metadata from `PACMetadataManager`
2. Removes PAC LINKs from other tables that reference the dropped table
3. Saves updated metadata to the JSON file

## PAC LINKs

PAC LINKs define foreign key relationships for privacy propagation. Unlike database-enforced FKs:
- **Metadata-only**: no constraint checking overhead at insert/update time
- **Composite keys supported**: `PAC_LINK (col1, col2) REFERENCES table(ref1, ref2)`
- **Must be acyclic**: PAC tables cannot link to other PAC tables

All local and referenced columns named in a `PAC_LINK` declaration are automatically considered **protected**.

The compiler uses PAC LINKs to:
1. Determine the FK path from queried tables to the PU
2. Decide which tables to join for hash computation
3. Validate that joins in the query use exact PAC LINK columns

## PAC_KEY vs PRIMARY KEY

Both identify the PU. Differences:

| Feature | PAC_KEY | PRIMARY KEY |
|---------|---------|-------------|
| Constraint enforcement | No | Yes (uniqueness + NOT NULL) |
| Performance overhead | None | Index maintenance |
| Composite keys | Yes | Yes |
| Discovery | PAC metadata only | Catalog + PAC metadata |

When a table has a `PRIMARY KEY` but no `PAC_KEY`, the compiler reads the PK from the catalog. When both exist, `PAC_KEY` takes precedence.

## Metadata Persistence

PAC metadata is stored in a JSON file named `pac_metadata_<db_name>.json` alongside the database file. For in-memory databases, no file is saved.

The file is written after every PAC DDL operation (CREATE, ALTER, DROP). On database load, the PAC extension reads the metadata file and populates the in-memory `PACMetadataManager`.

## Query Validation Flow

When a SELECT query arrives (not handled by the parser extension), the `PACRewriteRule` optimizer:

1. Checks if the plan scans any PAC-metadata tables
2. Follows FK paths (PAC LINKs) to find reachable PUs
3. Classifies the query:
   - **Inconspicuous**: no PU or PAC-linked table referenced -> pass through
   - **Rejected**: references protected data but violates constraints (see `query_operators.md`)
   - **Rewritable**: valid for PAC compilation -> transform aggregates
