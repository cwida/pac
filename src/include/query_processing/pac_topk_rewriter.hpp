//
// PAC Top-K Pushdown Rewriter
//
// Post-optimization rule that rewrites top-k queries for better utility.
//
// Problem: In the default plan, PAC noise is applied at the aggregate level (below TopN).
// This means TopN operates on noisy values, potentially selecting wrong groups.
//
// Solution: When pac_pushdown_topk=true, rewrite the plan so that:
// 1. The aggregate produces raw counter lists (pac_*_counters) instead of noised scalars
// 2. A "mean projection" computes pac_mean(counters) for ordering
// 3. TopN selects the top-k groups based on the true mean
// 4. A "noised projection" applies pac_noised() only to the selected k rows,
//    then casts back to the original aggregate type (e.g. BIGINT for count)
//
// Two paths depending on whether DuckDB inserts projections between TopN and Aggregate
// (e.g. __internal_decompress_string for VARCHAR GROUP BY columns):
//
// PATH B — No intermediate projections:
//   Before: TopN -> Aggregate
//   After:  NoisedProj -> TopN -> MeanProj -> Aggregate
//
// PATH A — With intermediate projections (e.g. string decompress):
//   Before: TopN -> Proj_outer -> ... -> Proj_inner -> Aggregate
//   After:  NoisedProj -> TopN -> Proj_outer -> ... -> Proj_inner -> MeanProj -> Aggregate
//   The intermediate projections are preserved (they may contain operations like
//   __internal_decompress_string that are needed for correct sort order). Each gets
//   a pac_mean passthrough column added so TopN can ORDER BY the mean value.
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

// Register the pac_mean scalar function (used by top-k pushdown for ordering)
void RegisterPacMeanFunction(ExtensionLoader &loader);

} // namespace duckdb

#endif // PAC_TOPK_REWRITER_HPP
