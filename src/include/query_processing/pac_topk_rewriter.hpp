//
// PAC Top-K Pushdown Rewriter
//
// Post-optimization rule that rewrites top-k queries for privacy-safe top-k.
//
// Problem: In the default plan, PAC noise is applied at the aggregate level (below TopN).
// This means TopN operates on noisy values, potentially selecting wrong groups.
// Worse, if we were to pick a single world's top-K keys, the output key set itself
// would leak which world is the secret, breaking PAC privacy.
//
// Solution: When pac_pushdown_topk=true, rewrite the plan using union-of-all-worlds ranking:
// 1. The aggregate produces raw counter lists (pac_*_counters) instead of noised scalars
// 2. A custom window function (pac_topk_superset) independently finds the top-K groups
//    in EACH of the 64 worlds and takes the UNION. This superset is determined by all
//    worlds (public information), not any single secret world, so its composition leaks
//    nothing about the secret.
// 3. A filter keeps only superset members
// 4. A "rank projection" computes pac_mean(counters) — the deterministic mean across all
//    64 counters — for each group. This is used for re-ranking and is privacy-safe since
//    it is the same regardless of which world is secret.
// 5. A final TopN re-ranks superset members by pac_mean and selects the top K
// 6. A "noised projection" applies pac_noised() only to the final K rows, producing
//    privacy-safe output values with variance-calibrated noise
//
// Plan structure (both PATH A and PATH B):
//   NoisedProj(pac_noised(counters, keyhash) for output values only)
//     → FinalTopN(K, ORDER BY pac_mean DESC)
//       → RankProj(pac_mean(counters) for ranking, passthrough counters+keyhash)
//         → Filter(superset_flag = TRUE)
//           → Window(pac_topk_superset(counters) OVER ())
//             → [IntermediateProjs?] → Aggregate(_counters + keyhash)
//

#ifndef PAC_TOPK_REWRITER_HPP
#define PAC_TOPK_REWRITER_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

// Post-optimization rule for top-k pushdown
class PACTopKRule : public OptimizerExtension {
public:
	PACTopKRule() {
		optimize_function = PACTopKOptimizeFunction;
	}

	static void PACTopKOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

// Register the pac_mean scalar function (kept for backward compat / debugging)
void RegisterPacMeanFunction(ExtensionLoader &loader);

// Register the pac_unnoised scalar function (extracts counter[J] for debugging)
void RegisterPacUnnoisedFunction(ExtensionLoader &loader);

// Register the pac_topk_superset window aggregate function (union-of-all-worlds top-K selection)
void RegisterPacTopKSupersetFunction(ExtensionLoader &loader);

} // namespace duckdb

#endif // PAC_TOPK_REWRITER_HPP
