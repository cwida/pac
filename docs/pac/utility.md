# PAC Utility Diff

The PAC Utility Diff feature allows you to measure the accuracy of privacy-preserving query results by comparing them to exact (non-private) results. This is essential for evaluating the trade-off between privacy and statistical accuracy.

## Overview

When `pac_diffcols` is set, PAC performs a **utility comparison** by:

1. Executing the PAC-compiled (private) query
2. Executing a deep-copied reference (exact) query
3. Joining both result sets using a FULL OUTER JOIN
4. Computing relative error for each numeric measure column
5. Reporting aggregate utility metrics

## Configuration

### The `pac_diffcols` Setting

| Format | Description |
|--------|-------------|
| `'N'` | Enable utility diff with `N` key columns for matching |
| `'N:/path/to/file.csv'` | Enable utility diff and append results to CSV file |
| `'true'` | Enable utility diff with auto-detected key columns |
| `'/path/to/file.csv'` | Auto-detect key columns and append results to CSV file |
| `NULL` | Disable utility diff (default) |

The number of key columns can be omitted to enable auto-detection. When auto-detecting, PAC infers the key column count from the number of GROUP BY columns in the query. Ungrouped (scalar) aggregates default to positional matching.

**Examples:**

```sql
-- Auto-detect key columns from GROUP BY
SET pac_diffcols = 'true';

-- Auto-detect with CSV output
SET pac_diffcols = '/tmp/utility_results.csv';

-- Positional matching (no key columns) - use for ungrouped queries
SET pac_diffcols = '0';

-- Key-based matching with 1 key column (e.g., GROUP BY single column)
SET pac_diffcols = '1';

-- Key-based matching with 2 key columns
SET pac_diffcols = '2';

-- Output to CSV file
SET pac_diffcols = '1:/tmp/utility_results.csv';

-- Disable
SET pac_diffcols = NULL;
```

### Key Columns vs Measure Columns

The `pac_diffcols` value specifies how many **leading columns** in the result are used as keys for row matching:

- **Key columns** (first N columns): Used to match rows between PAC and reference results
- **Measure columns** (remaining columns): Numeric columns where relative error is computed

For example, with `pac_diffcols = '1'` on a query `SELECT grp, SUM(val) FROM t GROUP BY grp`:
- Column 0 (`grp`) is the key column — used to match rows
- Column 1 (`SUM(val)`) is the measure column — error is computed here

## Output Format

### Query Results

When utility diff is enabled, the query output is transformed:

| Column Type | Output Value |
|-------------|--------------|
| Key columns | Reference value (for matched rows) |
| Numeric measure columns | Relative error percentage: `100 * \|ref - pac\| / max(0.00001, \|ref\|)` |

**Interpretation:**
- `0` = perfect accuracy (PAC result matches reference exactly)
- `5` = 5% relative error
- `100` = 100% relative error (PAC result is twice or zero compared to reference)

### Row Semantics

The FULL OUTER JOIN produces three types of rows:

| Row Type | Description | Key Columns | Measure Columns |
|----------|-------------|-------------|-----------------|
| `=` Matched | Both PAC and reference have this row | Non-NULL (reference values) | Relative error % |
| `+` PAC-only | Row exists only in PAC result | Non-NULL | NULL |
| `-` Missing | Row exists only in reference | NULL | 0 |

### Summary Metrics

At query completion, PAC prints (or appends to CSV) two metrics:

| Metric | Formula | Description |
|--------|---------|-------------|
| **Utility** | Average relative error % across all matched rows and measure columns | Lower is better; 0 = perfect |
| **Recall** | `matched_rows / (matched_rows + missing_rows)` | 1.0 = all reference rows found |

## Examples

### Ungrouped Query (Positional Matching)

```sql
SET pac_noise = true;
SET pac_diffcols = '0';

SELECT SUM(val) AS total_val FROM data_table;
-- Output: relative error % for total_val
```

### Grouped Query (Key-Based Matching)

```sql
SET pac_noise = true;
SET pac_diffcols = '1';

SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY category;
-- Output: category (ref value), relative error % for total
```

### Multiple Key Columns

```sql
SET pac_diffcols = '2';

SELECT region, year, SUM(revenue) FROM sales GROUP BY region, year;
-- Matches rows by (region, year), computes error on SUM(revenue)
```

### CSV Output for Benchmarking

```sql
SET pac_diffcols = '1:/tmp/benchmark_utility.csv';

-- Run multiple queries; each appends: utility,recall
SELECT dept, AVG(salary) FROM employees GROUP BY dept;
SELECT dept, COUNT(*) FROM employees GROUP BY dept;

SET pac_diffcols = NULL;

-- /tmp/benchmark_utility.csv now contains:
-- 2.5,1.0
-- 0.8,1.0
```

## Best Practices

1. **Use deterministic settings for reproducible results:**
   ```sql
   SET pac_seed = 42;
   SET pac_deterministic_noise = true;
   SET threads = 1;
   ```

2. **Match key columns to GROUP BY:**
   If your query has `GROUP BY a, b`, use `pac_diffcols = '2'`

3. **Order results for consistent comparison:**
   Add `ORDER BY` on key columns for deterministic row ordering

4. **Use CSV output for batch benchmarking:**
   Append results from multiple queries to analyze utility across a workload

## Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| `num_key_cols must be less than number of columns` | All columns are keys, none left for measures | Reduce `pac_diffcols` value |
| `unexpected NULL in key column` | Key column contains NULL values | Ensure key columns are NOT NULL |

## Related Settings

| Setting | Relevance |
|---------|-----------|
| `pac_noise` | Must be `true` for meaningful utility comparison |
| `pac_mi` | Counter index affects noise level |
| `pac_seed` | Set for reproducible PAC results |
| `pac_deterministic_noise` | Use `true` for reproducible testing |

