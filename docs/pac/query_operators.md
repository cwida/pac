# PAC Query Rewriting

The PAC compiler is an optimizer extension that transforms standard SQL query plans into PAC-private equivalents. It operates as a **pre-optimizer**, running before DuckDB's built-in optimizers (join ordering, filter pushdown, column lifetime, compressed materialization), so the PAC-transformed plan benefits from all standard optimizations automatically.

The compiler proceeds in two phases, matching Algorithm 1 in the paper:
1. **Top-down**: insert PU joins and derive hash expressions (including through CTEs)
2. **Bottom-up**: replace aggregates with PAC functions, rewrite categorical expressions

## Query Classification

When a query is issued, the compatibility checker traverses the logical plan and classifies it into one of three categories:

### Inconspicuous Queries
Queries that do not reference a PU or a PAC-linked table. These execute unmodified.

### Rejected Queries
Queries that reference privacy-relevant tables but cannot be safely privatized. Rejected when:
- A protected column is returned directly without aggregation (e.g., `SELECT name FROM customer`)
- Protected columns appear in `GROUP BY` keys (would leak individual values)
- The query contains **window functions** (`OVER`)
- The query contains **recursive CTEs** (`WITH RECURSIVE`)
- The query contains `SELECT DISTINCT` (the `LOGICAL_DISTINCT` operator)
- The query contains disallowed joins: `EXCEPT`, `INTERSECT` (but `UNION`/`CROSS_PRODUCT` are allowed)
- The query has **no allowed aggregation** (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`)
- Multiple protected tables are joined on columns that are **not** exact PAC LINKs
- The FK path from scanned tables to PU is **cyclic**

### Rewritable Queries
All other queries that reference a PU (directly or indirectly via FK path) and contain at least one allowed aggregation. These are compiled by the bitslice compiler.

## Case A: Query With PU Present

When the Privacy Unit table is scanned in the query (e.g., `SELECT COUNT(*) FROM customer`):

1. **Find all aggregates** that have the PU table in their subtree
2. **For each PU table**, compute a hash from the PU's key columns:
   - Single PAC_KEY: `pac_hash(hash(key_col))`
   - Composite PAC_KEY: `pac_hash(hash(key1) XOR hash(key2) XOR ...)` — XOR combines multiple column hashes into one, then pac_hash repairs it to 32 bits
3. **Insert a projection** above the PU table scan that computes the hash as an extra column
4. **Propagate** the hash column through all intermediate operators (projections, joins, filters) between the PU scan and the aggregate — this means adding the column to every operator's output along the path so the aggregate can see it
5. **Replace** each standard aggregate with its PAC equivalent: `SUM(x)` becomes `pac_sum(hash, x)`, `COUNT(*)` becomes `pac_count(hash)`, etc.
6. For **multiple PUs**, AND all hashes together: `hash1 AND hash2` — a tuple is in sub-sample j only if ALL its PUs have bit j set

### Example: Direct PU scan
```sql
-- Original
SELECT COUNT(*) FROM customer WHERE c_mktsegment = 'BUILDING'
-- Rewritten (conceptual)
SELECT pac_count_noised(pac_hash(hash(c_custkey)))
FROM customer WHERE c_mktsegment = 'BUILDING'
```

## Case B: Query Without PU Present

When the query scans dependent tables but not the PU itself (e.g., `SELECT SUM(l_quantity) FROM lineitem`):

1. **Follow FK path** from scanned tables to PU (e.g., `lineitem -> orders -> customer`)
2. **Identify missing tables** in the path that need to be joined
3. **Join elimination**: if `pac_join_elimination=true`, skip joining the PU table itself when only FK columns are needed (e.g., join only `orders`, not `customer`, because `orders.o_custkey` is the FK)
4. **For each instance** of the connecting table:
   - Create fresh `LogicalGet` nodes for missing tables
   - Build join chain with appropriate FK conditions
   - Replace the connecting table's node with the join chain
5. **Hash the FK columns** that reference the PU (e.g., `pac_hash(hash(o_custkey))`)
6. **Propagate and replace** aggregates as in Case A

### Example: FK chain
```sql
-- Original
SELECT l_returnflag, SUM(l_quantity) FROM lineitem
WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag
-- Rewritten (conceptual)
SELECT l_returnflag, pac_sum_noised(pac_hash(hash(o_custkey)), l_quantity)
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag
```

## Uncorrelated Subqueries

Uncorrelated subqueries (e.g., `WHERE x > (SELECT AVG(y) FROM t)`) are handled by DuckDB as a `SINGLE` join (returns exactly one row). The PAC compiler:

1. Finds **all aggregates** in the plan (including those inside the subquery)
2. Filters to **target aggregates** — only those that have PU/FK-linked tables in their direct subtree (not through nested aggregates)
3. Inserts hash projections and transforms each aggregate independently
4. The outer query's aggregate and the subquery's aggregate each get their own hash

The subquery's aggregate result feeds into the outer query's filter/comparison. When the subquery contains a PAC aggregate and the outer query doesn't aggregate (categorical query), the categorical rewriter handles it (see below).

## Correlated Subqueries (DELIM_JOIN)

Correlated subqueries (e.g., TPC-H Q17: `WHERE l1.quantity < (SELECT AVG(l2.quantity) FROM lineitem l2 WHERE l2.partkey = l1.partkey)`) are compiled by DuckDB into `DELIM_JOIN` operators. The PAC compiler handles these specially:

1. **Detect** when the same connecting table appears in both outer and inner query (e.g., `lineitem` appears twice)
2. **Find all instances** of the connecting table in the plan tree
3. **Add independent join chains** to each instance (each gets its own `orders` join)
4. **DELIM_JOIN propagation**: DuckDB compiles correlated subqueries into DELIM_JOIN operators, which have a "duplicate eliminated columns" mechanism for passing values from the outer query into the inner subquery. When the hash source is in the outer query but the target aggregate is in the subquery branch, the compiler adds the hash column to the DELIM_JOIN's duplicate-eliminated columns and corresponding DELIM_GET nodes, so the inner aggregate can access it.
5. **Accessibility checks**: verify FK table columns aren't blocked by SEMI/ANTI/MARK joins (which don't pass right-side columns). If blocked, find an accessible alternative or add a fresh join.

### SEMI/ANTI Join Handling
When the FK table is on the right side of a SEMI/ANTI join (e.g., `WHERE EXISTS (SELECT ... FROM orders ...)`), its columns are inaccessible to the outer aggregate. The compiler:
- Searches for an accessible FK table instance in the outer query
- If none found, looks for an alternative accessible table with an FK to the blocked table and adds a new join

## CTEs (Common Table Expressions)

The compiler supports **materialized CTEs** (`WITH ... AS MATERIALIZED`):

1. **Build CTE table map**: maps each CTE's index to the set of base tables it transitively references (including through nested CTE refs). This is resolved once at the start of compilation.
2. **CTE-aware subtree checks**: when checking if a table is reachable from an operator, the compiler follows CTE_REF nodes through the map to check if the referenced CTE transitively contains the target table.
3. **Hash projection above CTE_SCAN**: when the PU table is accessed through a CTE, the compiler can't modify the CTE definition (it's shared across all scan sites). Instead, it inserts a hash projection above the CTE_SCAN node that reads the PU key columns from the CTE's output and computes the hash there.
4. **CTE definition propagation**: the CTE definition must expose the PU key columns in its output for the hash to be computable on the scan side.

Non-materialized CTEs are inlined by DuckDB before the PAC optimizer runs, so they appear as regular subquery trees.

## Inner + Outer Aggregates (Nested Aggregation)

The compiler handles the pattern where an inner aggregate groups by PU key and an outer aggregate aggregates over those results (e.g., TPC-H Q13: inner counts orders per customer, outer counts how many customers have each order count):

1. **Detect PU-key grouping**: when filtering which aggregates to transform, the compiler checks if an inner aggregate's GROUP BY keys contain the PU's PAC_KEY columns or PAC_LINK columns referencing a PU. This is done by inspecting the column bindings in the aggregate's group expressions and tracing them back to their source table scans.
2. When detected, the **inner aggregate is skipped** (it's already partitioned by PU key, so no noise needed there — each group corresponds to a single PU)
3. The **outer aggregate is noised** instead, using the inner aggregate's PU key group column as the hash input. The compiler locates the inner aggregate, finds the column binding for the PU key in its output, and uses that as the hash source for the outer aggregate.
4. This means the hash for the outer aggregate comes from the inner aggregate's GROUP BY output rather than from a table scan — the inner aggregate preserves PU identity in its grouping columns.

### Example: TPC-H Q13 Pattern
```sql
-- Inner aggregate groups by c_custkey (PU key) - NOT noised
-- Outer aggregate counts over those groups - IS noised
SELECT c_count, COUNT(*) AS custdist FROM (
    SELECT c_custkey, COUNT(o_orderkey) AS c_count
    FROM customer LEFT JOIN orders ON ...
    GROUP BY c_custkey  -- groups by PU key
) GROUP BY c_count
```

## Top-K Queries

Top-K queries (`ORDER BY agg LIMIT k`) get special treatment via a dedicated post-optimizer rule:

**Problem**: In the default plan, PAC noise is applied at the aggregate level (below TopN). TopN then operates on noisy values, potentially selecting wrong groups.

**Solution** (`pac_pushdown_topk=true`):
1. Convert PAC aggregates to `_counters` variants (return all 64 counter values as `LIST<FLOAT>`)
2. Insert a **mean projection** (`pac_mean(counters)`) for ordering — gives the true aggregate mean
3. **TopN** selects top-k groups based on the true mean
4. Insert a **noised projection** (`pac_noised(counters)`) above TopN — applies noise only to selected rows, cast back to original type

Two paths depending on plan structure:
- **Path A** (intermediate projections, e.g., string decompress): preserves intermediate projections, adds `pac_mean` passthrough columns
- **Path B** (TopN directly above Aggregate): simpler insertion of MeanProj and NoisedProj

**Superset expansion** (`pac_topk_expansion`): select `ceil(c * K)` candidates with the inner TopN, then a final TopN limits to the original K after noising.

## DISTINCT Aggregates

`SELECT DISTINCT` is **rejected** by the compatibility checker. However, **aggregate DISTINCT** (e.g., `COUNT(DISTINCT col)`) is supported:

- `COUNT(DISTINCT x)` -> `pac_count_distinct(key_hash, value_hash)`: tracks a map of `value_hash -> OR(key_hashes)`, finalizes by building 64 counters from distinct values
- `SUM(DISTINCT x)` -> `pac_sum_distinct(key_hash, value)`: tracks map of `bitcast(value) -> (OR(key_hashes), value)`, builds 64 sum counters from distinct values
- `AVG(DISTINCT x)` -> `pac_avg_distinct(key_hash, value)`: like sum_distinct but divides each counter by its distinct count

The compiler detects when an aggregate expression is marked as distinct and routes to the appropriate distinct variant.

## Categorical Queries (Comparison Against PAC Aggregate)

When an outer query compares against an inner PAC aggregate without its own aggregate (e.g., `WHERE price > (SELECT pac_avg(...))`):

1. The inner PAC aggregate is converted to `_counters` variant (returns all 64 values)
2. The comparison is evaluated against all 64 values, producing a **64-bit mask** (`pac_select_gt`, `pac_select_lt`, etc.)
3. The outer query applies `pac_filter(mask)` for probabilistic row filtering (returns true with probability `popcount(mask)/64`)
4. When a nested PAC aggregate feeds into another PAC aggregate, the 64 booleans are AND'ed bit-by-bit with the outer key_hash (`pac_select`)

This is handled by the categorical rewriter after the standard PAC transformation.
