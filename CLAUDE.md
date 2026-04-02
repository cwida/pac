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
- **Add `PAC_DEBUG_PRINT` statements** at major code flow points (entry/exit of compilation phases, aggregate rewrites, clipping decisions). Use the existing `PAC_DEBUG_PRINT` macro from `src/include/pac_debug.hpp` — it's compiled out when `PAC_DEBUG` is 0.

## What is PAC?

PAC (Probably Approximately Correct) Privacy, or short: pac, is a DuckDB extension that automatically privatizes SQL aggregate queries. It protects against Membership Inference Attacks by maintaining 64 parallel counters per aggregate (one per "world" bit), adding calibrated noise at finalization. Queries are rewritten transparently — users write normal SQL and PAC transforms it.

## Build & Test

```bash
GEN=ninja make                # build (release)
make test                     # run all tests (~20 tests, ~1600 assertions)

# single test
build/release/test/unittest "test/sql/pac_sum.test"

# C++ unit tests (parser, traversal, compiler)
build/release/extension/pac/pac_test_runner
```

Build outputs go to `build/release/`. DuckDB is a git submodule in `duckdb/`.

## Compilation Pipeline

PAC runs in `pre_optimize_function` — BEFORE DuckDB's built-in optimizers (join order, filter pushdown, column lifetime). This means:
- `plan->ResolveOperatorTypes()` must be called before accessing `LogicalProjection::types` (they're empty in raw plans)
- WHERE filters are still separate FILTER nodes
- LIMIT is a separate root node
- DuckDB's optimizers run automatically on the PAC-transformed plan

### Pipeline phases (in order)

1. **Compatibility check** (`pac_compatibility_check.cpp`) — decides if query needs PAC rewrite
2. **FK join injection** (`pac_bitslice_add_fkjoins.cpp`) — adds missing joins to reach PU tables
3. **Aggregate transformation** (`pac_bitslice_compiler.cpp` → `pac_expression_builder.cpp`) — replaces SUM/COUNT/etc. with pac_noised_sum/pac_noised_count/etc., inserts pac_hash projections above scans
4. **Categorical rewrite** (`pac_categorical_rewriter.cpp`) — when PAC aggregates appear in filters/comparisons, converts to counter lists (LIST\<FLOAT\>) with pac_filter/pac_select terminals
5. **AVG decomposition** (`pac_avg_rewriter.cpp`) — rewrites pac_noised_avg into pac_noised_div(pac_sum, pac_count)
6. **Clip rewrite** (`pac_expression_builder.cpp:RewriteClipAggregates`) — when `pac_clip_support` is set, inserts lower aggregate for per-PU pre-aggregation with clipping

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

- `src/core/pac_optimizer.cpp` — optimizer hook entry point
- `src/compiler/pac_bitslice_compiler.cpp` — main compilation orchestrator (`CompilePacBitsliceQuery`)
- `src/query_processing/pac_expression_builder.cpp` — aggregate modification, clip rewrite, expression binding
- `src/query_processing/pac_plan_traversal.cpp` — plan traversal utilities (FindAllAggregates, AggregateGroupsByPUKey, etc.)
- `src/include/aggregates/pac_aggregate.hpp` — PacBindData, noise calibration, p-tracking
- `src/categorical/pac_categorical_rewriter.cpp` — categorical query transformation (~1770 lines)

## Debugging

Set `#define PAC_DEBUG 1` in `src/include/pac_debug.hpp` for stderr trace output. Use `EXPLAIN` to see the transformed plan.

## PAC DDL examples

```sql
ALTER TABLE customer ADD PAC_KEY (c_custkey);
ALTER TABLE customer SET PU;
ALTER TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer (c_custkey);
SET pac_mi = 0;        -- disable noise for testing
SET pac_seed = 42;     -- reproducible results
SET pac_clip_support = 40;  -- enable clip rewrite with support threshold
```

## Code style (clang-tidy)

The project uses clang-tidy with DuckDB's configuration (`.clang-tidy`). Key naming rules:

- **Classes/Enums**: `CamelCase` (e.g., `PacClipSumIntState`)
- **Functions**: `CamelCase` (e.g., `GetLevel`, `AllocateLevel`)
- **Variables/parameters/members**: `lower_case` (e.g., `max_level_used`, `key_hash`)
- **Constants/static/constexpr**: `UPPER_CASE` (e.g., `PAC2_NUM_LEVELS`, `PAC2_LEVEL_SHIFT`)
- **Macros**: `UPPER_CASE` (e.g., `PAC_DEBUG_PRINT`)
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
