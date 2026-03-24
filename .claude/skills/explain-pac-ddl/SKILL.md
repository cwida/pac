---
name: explain-pac-ddl
description: Reference for PAC DDL syntax — PAC_KEY, PAC_LINK, PROTECTED, SET PU, and the parser. Auto-loaded when discussing table setup, privacy units, or protected columns.
---

## PAC DDL Overview

PAC extends SQL DDL with privacy annotations. The parser (`src/parser/pac_parser.cpp`,
`src/parser/pac_parser_helpers.cpp`) intercepts CREATE TABLE and ALTER TABLE statements
to extract PAC-specific clauses before forwarding to DuckDB.

### Privacy Unit (PU) table

The PU table is the entity being protected (e.g., customer). One row = one individual.

```sql
-- Mark a table as the privacy unit
ALTER TABLE customer ADD PAC_KEY (c_custkey);
ALTER TABLE customer SET PU;

-- Protect specific columns from direct projection
ALTER PU TABLE customer ADD PROTECTED (c_acctbal, c_name, c_address);
```

- `PAC_KEY (col)`: Designates the column(s) that uniquely identify a privacy unit.
  Must be set before `SET PU`.
- `SET PU`: Marks the table as the privacy unit. After this, aggregates on linked
  tables get PAC noise.
- `PROTECTED (col1, col2, ...)`: Columns that cannot be directly projected.
  Aggregates (SUM, COUNT, AVG) on protected columns go through PAC.

### Linking tables to the PU

Non-PU tables reference the PU table via foreign-key-like links:

```sql
ALTER TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer (c_custkey);
ALTER TABLE lineitem ADD PAC_LINK (l_orderkey) REFERENCES orders (o_orderkey);
```

- `PAC_LINK (local_col) REFERENCES table(ref_col)`: Declares how to join this
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
    PAC_KEY (id),
    PROTECTED (salary)
);
```

The parser strips PAC_KEY, PAC_LINK, and PROTECTED clauses from the CREATE
statement, forwards the clean SQL to DuckDB, then applies the PAC metadata
via ALTER TABLE internally.

### Common mistakes

- `PAC_LINK(col, table, ref)` — wrong. Use `PAC_LINK (col) REFERENCES table(ref)`.
- `PROTECTED salary` — wrong. Must have parentheses: `PROTECTED (salary)`.
- ALTER TABLE on a PU table requires `ALTER PU TABLE`, not `ALTER TABLE`.

### Key source files

- `src/parser/pac_parser.cpp` — main parser hook (intercepts SQL statements)
- `src/parser/pac_parser_helpers.cpp` — extraction of PAC_KEY, PAC_LINK, PROTECTED
- `src/core/pac_metadata.cpp` — in-memory metadata storage for PU/link/protected info
