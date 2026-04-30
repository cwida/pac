---
name: explain-pac-ddl
description: Reference for PAC DDL syntax — PRIVACY_KEY, PRIVACY_LINK, PROTECTED, SET PU, and the parser. Auto-loaded when discussing table setup, privacy units, or protected columns.
---

## PAC DDL Overview

PAC extends SQL DDL with privacy annotations. The parser (`src/parser/privacy_parser.cpp`,
`src/parser/privacy_parser_helpers.cpp`) intercepts CREATE TABLE and ALTER TABLE statements
to extract PAC-specific clauses before forwarding to DuckDB.

### Privacy Unit (PU) table

The PU table is the entity being protected (e.g., customer). One row = one individual.

```sql
-- Mark a table as the privacy unit
ALTER TABLE customer ADD PRIVACY_KEY (c_custkey);
ALTER TABLE customer SET PU;

-- Protect specific columns from direct projection
ALTER PU TABLE customer ADD PROTECTED (c_acctbal, c_name, c_address);
```

- `PRIVACY_KEY (col)`: Designates the column(s) that uniquely identify a privacy unit.
  Must be set before `SET PU`.
- `SET PU`: Marks the table as the privacy unit. After this, aggregates on linked
  tables get PAC noise.
- `PROTECTED (col1, col2, ...)`: Columns that cannot be directly projected.
  Aggregates (SUM, COUNT, AVG) on protected columns go through PAC.

### Linking tables to the PU

Non-PU tables reference the PU table via foreign-key-like links:

```sql
ALTER TABLE orders ADD PRIVACY_LINK (o_custkey) REFERENCES customer (c_custkey);
ALTER TABLE lineitem ADD PRIVACY_LINK (l_orderkey) REFERENCES orders (o_orderkey);
```

- `PRIVACY_LINK (local_col) REFERENCES table(ref_col)`: Declares how to join this
  table back to the PU. The compiler uses these links to inject the PU hash
  into the query plan.
- Links can be chained: `lineitem → orders → customer`.

### CREATE TABLE syntax (inline)

PAC clauses can be inlined in CREATE TABLE:

```sql
CREATE PU TABLE employees (
    id INTEGER,
    department VARCHAR,
    salary DECIMAL(10,2),
    PRIVACY_KEY (id),
    PROTECTED (salary)
);
```

The parser strips PRIVACY_KEY, PRIVACY_LINK, and PROTECTED clauses from the CREATE
statement, forwards the clean SQL to DuckDB, then applies the PAC metadata
via ALTER TABLE internally.

### Common mistakes

- `PRIVACY_LINK(col, table, ref)` — wrong. Use `PRIVACY_LINK (col) REFERENCES table(ref)`.
- `PROTECTED salary` — wrong. Must have parentheses: `PROTECTED (salary)`.
- ALTER TABLE on a PU table requires `ALTER PU TABLE`, not `ALTER TABLE`.

### Metadata files

PAC metadata (PU tables, links, protected columns) is stored in JSON sidecar files
next to the database file. The naming convention is:

```
privacy_metadata_<dbname>_<schema>.json
```

For example, `tpch_sf1.db` produces `privacy_metadata_tpch_sf1_main.json` in the same
directory.

**Auto-loading**: When the PAC extension loads (`LOAD privacy`), it automatically looks
for a matching metadata file next to the attached database and loads it. No manual
`PRAGMA load_privacy_metadata` needed for persistent databases.

**Saving**: After setting up PRIVACY_KEY/PRIVACY_LINK/PROTECTED, save with:
```sql
PRAGMA save_privacy_metadata('/path/to/privacy_metadata_mydb_main.json');
```

**Clearing**: Reset all in-memory PAC metadata:
```sql
PRAGMA clear_privacy_metadata;
```

**Important**: If you delete or recreate a database file, also delete the
corresponding `privacy_metadata_*.json` file. Stale metadata causes confusing errors
(references to tables/columns that no longer exist).

For in-memory databases, metadata file is named `privacy_metadata_memory_main.json`
in the current working directory.

### Key source files

- `src/parser/privacy_parser.cpp` — main parser hook (intercepts SQL statements)
- `src/parser/privacy_parser_helpers.cpp` — extraction of PRIVACY_KEY, PRIVACY_LINK, PROTECTED
- `src/core/pac_metadata.cpp` — in-memory metadata storage for PU/link/protected info
- `src/core/privacy_extension.cpp` — auto-loading of metadata on extension load (LoadInternal)
