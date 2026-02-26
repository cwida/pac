//
// PAC Top-K Pushdown Rewriter
//
// Post-optimization rule that rewrites top-k queries for better utility.
//
// Problem: In the default plan, PAC noise is applied at the aggregate level (below TopN).
// This means TopN operates on noisy values, potentially selecting wrong groups.
//
// Solution: When pac_pushdown_topk=true, rewrite the plan using single-world ranking:
// 1. The aggregate produces raw counter lists (pac_*_counters) instead of noised scalars
// 2. A custom window function (pac_topk_superset) selects one world J (= query_hash % 64,
//    consistent with PacNoisySampleFrom64Counters) and marks the top-K groups by counter[J]
// 3. A filter keeps only those K groups
// 4. A "noised projection" applies pac_noised() to the selected groups,
//    then casts back to the original aggregate type (e.g. BIGINT for count)
// 5. A final TopN re-ranks the K groups by noised values
//
// Plan structure (both PATH A and PATH B):
//   FinalTopN(K, ORDER BY noised DESC)
//     → NoisedProj(pac_noised(counters, keyhash), passthrough group_cols)
//       → Filter(superset_flag = TRUE)
//         → Window(pac_topk_superset(counters) OVER ())
//           → [IntermediateProjs?] → Aggregate(_counters + keyhash)
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

// Register the pac_topk_superset window aggregate function (single-world top-K selection)
void RegisterPacTopKSupersetFunction(ExtensionLoader &loader);

} // namespace duckdb

#endif // PAC_TOPK_REWRITER_HPP
