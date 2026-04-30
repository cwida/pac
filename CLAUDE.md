# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working with the user

When you're stuck — either unable to fix a bug after 2-3 attempts, or tempted to work around the actual problem by redefining the objective — **stop and ask the user for directions**. Explain clearly what the specific problem is (e.g., "pac_clip_sum(UBIGINT, DOUBLE) has no matching overload — should I add a DOUBLE overload or cast?"). The user knows this codebase deeply and can often point you to the right solution in one sentence. Do not silently change the goal, declare something impossible, or add bloated workarounds without consulting first. We work as a team.

Always test your changes with real queries (e.g., TPC-H on sf1) before declaring success, not just unit tests. Unit tests with wide boolean thresholds can pass even when the code is fundamentally broken.

Never execute git commands that could lose code. Always ask the user for permission on those.

## Development rules

- **New features must have tests.** Ask the user whether to create a new test file or extend an existing one in `test/sql/`.
- **Never remove a failing test to "fix" a failure.** If a test fails, fix the underlying bug. Tests exist for a reason.
- **Before implementing anything, search the existing codebase** for similar patterns or solutions. Check if a helper function, utility, or prior approach already addresses the problem. Reuse before reinventing.
- **Use helper functions.** Factor shared logic into helpers rather than duplicating code. Check `src/include/utils/` and existing helpers in the file you're editing.
- **Never edit the `duckdb/` submodule.** The DuckDB source is read-only. All PAC logic lives in `src/` and `test/`. If you need DuckDB internals, use the public API or ask the user.
- **Keep the paper in mind.** The PAC mechanism is described in [SIMD-PAC-DB: Pretty Performant PAC Privacy](https://arxiv.org/abs/2603.15023). Refer to it for the theoretical foundations (noise calibration, mutual information bounds, counter semantics) before making changes to core aggregate logic.
- **Add `PRIVACY_DEBUG_PRINT` statements** at major code flow points (entry/exit of compilation phases, aggregate rewrites, clipping decisions). Use the existing `PRIVACY_DEBUG_PRINT` macro from `src/include/privacy_debug.hpp` — it's compiled out when `PRIVACY_DEBUG` is 0.

## What is this extension?

`privacy` is a DuckDB extension that automatically privatizes SQL aggregate queries. It supports two privacy mechanisms, selected via `SET privacy_mode`:

- **`pac`** (default): PAC Privacy — empirical MIA resistance. Maintains 64 parallel counters per aggregate (one per "world" bit), adds noise calibrated to the query variance across sub-samples. Provides theoretical mutual-information bounds. Not differential privacy.
- **`dp_elastic`**: Elastic sensitivity DP — formal (ε,δ)-differential privacy. Derives a per-query sensitivity bound from the join tree's max frequencies, injects Laplace noise calibrated to `sensitivity/epsilon`. Only `dp_elastic` provides a formal ε-DP guarantee.

Both modes share the same DDL (`PRIVACY_KEY`, `PRIVACY_LINK`, `PROTECTED`) and rewrite aggregate plans transparently — users write normal SQL.

## Build & Test

```bash
GEN=ninja make                # build (release)
make test                     # run all tests (~25 tests, ~2200 assertions)

# single test
build/release/test/unittest "test/sql/pac_sum.test"

# C++ unit tests (parser, traversal, compiler)
build/release/extension/privacy/pac_test_runner
```

Build outputs go to `build/release/`. DuckDB is a git submodule in `duckdb/`.

## Compilation Pipeline

The optimizer hook runs in `pre_optimize_function` — BEFORE DuckDB's built-in optimizers (join order, filter pushdown, column lifetime). This means:
- `plan->ResolveOperatorTypes()` must be called before accessing `LogicalProjection::types` (they're empty in raw plans)
- WHERE filters are still separate FILTER nodes
- LIMIT is a separate root node
- DuckDB's optimizers run automatically on the transformed plan

### PAC pipeline phases (in order)

1. **Compatibility check** (`privacy_compatibility_check.cpp`) — decides if query needs PAC rewrite; rejects unsupported patterns (window functions, protected columns in GROUP BY)
2. **FK join injection** (`pac_bitslice_add_fkjoins.cpp`) — adds missing joins to reach PU tables
3. **Aggregate transformation** (`pac_bitslice_compiler.cpp` → `pac_expression_builder.cpp`) — replaces SUM/COUNT/etc. with pac_noised_sum/pac_noised_count/etc., inserts pac_hash projections above scans
4. **Categorical rewrite** (`pac_categorical_rewriter.cpp`) — when PAC aggregates appear in filters/comparisons, converts to counter lists (LIST\<FLOAT\>) with pac_filter/pac_select terminals
5. **AVG decomposition** (`pac_avg_rewriter.cpp`) — rewrites pac_noised_avg into pac_noised_div(pac_sum, pac_count)
6. **Clip rewrite** (`pac_expression_builder.cpp:RewriteClipAggregates`) — when `pac_clip_support` is set, inserts lower aggregate for per-PU pre-aggregation with clipping

### DP-elastic pipeline phases (in order)

1. **Compatibility check** (`privacy_compatibility_check.cpp`) — shared structural checks; PU columns can only appear inside aggregates
2. **FK chain extraction** (`dp_elastic_compiler.cpp:ExtractFKChain`) — validates a linear FK path to the PU; chain may be explicit (joins in the SQL) or implicit (derived from PRIVACY_LINK metadata when the PU table is absent)
3. **AVG rewrite** (`dp_elastic_compiler.cpp:RewriteAvgAggregates`) — each `AVG(x)` is replaced in-place by `SUM(x)` and a `COUNT(*)` is appended; the original `FILTER (WHERE …)` predicate is propagated to both
4. **Clipping** (`dp_elastic_compiler.cpp:ClipSumInputs`) — SUM (and AVG-derived SUM) arguments wrapped in `greatest(least(v, C), -C)` where C = `dp_sum_bound`
5. **Sensitivity computation** (`dp_elastic_compiler.cpp:ComputeMfK` + `ComputeElasticSensitivity`) — per-table max frequency; global ES = ∏ mf; smooth ES with β = ε/(2·ln(2/δ)) when δ > 0
6. **Budget split** (`CompileDPElasticQuery`) — with k user-visible aggregates each gets ε/k (scale × k); AVG components further split to ε/(2k) (scale × 2k)
7. **Noise injection** (`dp_elastic_compiler.cpp:WrapAggregateWithLaplace`) — projection above the aggregate adds `dp_laplace_noise(agg, scale)` per column, cast back to the original type
8. **τ-thresholding** (`dp_elastic_compiler.cpp:ApplyGroupSuppression`) — if `privacy_min_group_count` set, a filter above the noise projection drops groups where `|noised| < threshold · √2 · scale` (skipped for AVG positions, whose ratio is not a simple Laplace output)
9. **AVG ratio projection** (`dp_elastic_compiler.cpp:WrapAvgRatioProjection`) — inserted above the suppression filter (or directly above the noise projection if no suppression). Computes `cast(noised_sum, DOUBLE) / greatest(cast(noised_count, DOUBLE), 1.0)` for each AVG; non-AVG outputs pass through. ColumnBindingReplacer remaps upstream references (including any HAVING filter) so they read the ratio rather than the raw SUM

### Aggregate naming convention

| Name pattern | Returns | Purpose |
|---|---|---|
| `pac_sum/count/min/max` | LIST\<FLOAT\> | 64 counters (used by categorical/clip rewrites) |
| `pac_noised_sum/count/min/max` | scalar | Fused counters + noise (direct query output) |
| `pac_clip_sum/count/min/max` | LIST\<FLOAT\> | Counters with per-level support clipping |
| `pac_noised_clip_sum/count/min/max` | scalar | Fused clip + noise |

pac_noised_* is the fused version of pac_noised(pac_*()). The unfused form is used when expressions operate on counters (list_transform/lambdas in categorical queries).

### Key architectural rules

- **pac_hash is always computed in a Projection above the scan**, never inside an aggregate
- **Pre-computed bindings become stale after RewriteBottomUp** — always re-compute at point of use
- **Use `binder.GenerateTableIndex()`** for new table indices, never manual tracking
- **Always call `ResolveOperatorTypes()`** after creating or modifying a LogicalAggregate
- **pac_noised_sum on DECIMAL** uses `BindDecimalPacSum` to dispatch by physical type and set return_type to DECIMAL(38, scale) — any new sum variant needs the same pattern

## Key source files

- `src/core/privacy_optimizer.cpp` — optimizer hook entry point; dispatches to PAC or DP-elastic based on `privacy_mode`
- `src/compiler/pac_bitslice_compiler.cpp` — PAC compilation orchestrator (`CompilePacBitsliceQuery`)
- `src/compiler/dp_elastic_compiler.cpp` — DP-elastic compilation (`CompileDPElasticQuery`, `ExtractFKChain`, `ComputeMfK`)
- `src/query_processing/pac_expression_builder.cpp` — aggregate modification, clip rewrite, expression binding
- `src/query_processing/pac_plan_traversal.cpp` — plan traversal utilities (FindAllAggregates, AggregateGroupsByPUKey, etc.)
- `src/include/aggregates/pac_aggregate.hpp` — PacBindData, noise calibration, p-tracking
- `src/categorical/pac_categorical_rewriter.cpp` — categorical query transformation (~1770 lines)
- `src/metadata/privacy_compatibility_check.cpp` — shared compatibility check for both modes

## Debugging

Set `#define PRIVACY_DEBUG 1` in `src/include/privacy_debug.hpp` for stderr trace output. Use `EXPLAIN` to see the transformed plan.

## DDL examples

```sql
-- Declare privacy structure (shared by both modes)
ALTER TABLE customer ADD PRIVACY_KEY (c_custkey);
ALTER TABLE customer SET PU;
ALTER TABLE orders ADD PRIVACY_LINK (o_custkey) REFERENCES customer (c_custkey);

-- PAC mode (default)
SET privacy_mode = 'pac';
SET pac_mi = 0;                      -- disable noise for testing
SET privacy_seed = 42;               -- reproducible results
SET pac_clip_support = 40;           -- enable clip rewrite with support threshold
SET privacy_min_group_count = 4;     -- suppress low-SNR cells

-- DP-elastic mode (formal ε-DP)
SET privacy_mode = 'dp_elastic';
SET dp_epsilon = 1.0;
SET dp_delta = 1e-6;
SET dp_sum_bound = 1000;             -- required for SUM: max abs value per PU
SET privacy_min_group_count = 4;     -- τ-thresholding for GROUP BY
```

## Code style (clang-tidy)

The project uses clang-tidy with DuckDB's configuration (`.clang-tidy`). Key naming rules:

- **Classes/Enums**: `CamelCase` (e.g., `PacClipSumIntState`)
- **Functions**: `CamelCase` (e.g., `GetLevel`, `AllocateLevel`)
- **Variables/parameters/members**: `lower_case` (e.g., `max_level_used`, `key_hash`)
- **Constants/static/constexpr**: `UPPER_CASE` (e.g., `PAC2_NUM_LEVELS`, `PAC2_LEVEL_SHIFT`)
- **Macros**: `UPPER_CASE` (e.g., `PRIVACY_DEBUG_PRINT`)
- **Typedefs**: `lower_case_t` suffix (e.g., `aggregate_update_t`)

Other style rules (from `.clang-format`, based on LLVM):

- **Tabs for indentation**, width 4
- **Column limit**: 120
- **Braces**: same line as statement (K&R / Allman-attached)
- **Pointers**: right-aligned (`int *ptr`, not `int* ptr`)
- **No short functions on single line**
- **Templates**: always break after `template<...>`
- **Long arguments**: align after open bracket

Run `make format-fix` to auto-format. Formatting runs automatically via hook after edits.

## Attack evaluation

Attack scripts live in `attacks/`. Results are documented in `attacks/clip_attack_results.md`.

```bash
bash attacks/clip_attack_test.sh 2>/dev/null     # main attack suite
bash attacks/clip_multirow_test.sh 2>/dev/null   # 20K small items test
bash attacks/clip_hardzero_stress.sh 2>/dev/null  # stress tests
```
