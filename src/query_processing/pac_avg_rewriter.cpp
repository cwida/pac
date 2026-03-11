//
// RewritePacAvgToDiv: post-processing pass that decomposes pac_noised_avg / pac_avg
// into sum/count + division.  Runs as a standalone optimizer extension so it
// works for both compiler-generated and user-written pac_avg() SQL.
//

#include "query_processing/pac_avg_rewriter.hpp"
#include "query_processing/pac_expression_builder.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

// Replace every BoundColumnRefExpression whose binding matches a key in |replacements|
// with a copy of the corresponding expression value.  Recurses into children.
static void ReplaceAvgRefs(unique_ptr<Expression> &expr,
                           const unordered_map<idx_t, unique_ptr<Expression>> &replacements, idx_t table_index) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		if (col_ref.binding.table_index == table_index) {
			auto it = replacements.find(col_ref.binding.column_index);
			if (it != replacements.end()) {
				expr = it->second->Copy();
				return;
			}
		}
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ReplaceAvgRefs(child, replacements, table_index); });
}

// Walk all operators above |stop| and replace column refs using |replacements|.
static void PropagateAvgReplacements(LogicalOperator &op, LogicalOperator *stop,
                                     const unordered_map<idx_t, unique_ptr<Expression>> &replacements,
                                     idx_t table_index) {
	if (&op == stop) {
		return;
	}
	for (auto &expr : op.expressions) {
		ReplaceAvgRefs(expr, replacements, table_index);
	}
	for (auto &child : op.children) {
		PropagateAvgReplacements(*child, stop, replacements, table_index);
	}
}

// Process a single LogicalAggregate: find pac_noised_avg / pac_avg, replace with sum+count,
// inject division in parent operators.
static void RewritePacAvgAggregate(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                   LogicalAggregate &agg) {
	// First pass: identify avg positions
	vector<idx_t> avg_positions;
	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		if (agg.expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &aggr = agg.expressions[i]->Cast<BoundAggregateExpression>();
		if (aggr.function.name == "pac_noised_avg" || aggr.function.name == "pac_avg") {
			avg_positions.push_back(i);
		}
	}
	if (avg_positions.empty()) {
		return;
	}

	idx_t agg_index = agg.aggregate_index;

	// For each avg, replace with sum at the same position, append count at end.
	struct AvgDecomp {
		idx_t sum_col;
		idx_t count_col;
		bool is_noised;
	};
	unordered_map<idx_t, AvgDecomp> decomp_map;

	for (auto pos : avg_positions) {
		auto &old_aggr = agg.expressions[pos]->Cast<BoundAggregateExpression>();
		bool is_noised = (old_aggr.function.name == "pac_noised_avg");

		// Extract hash and value children: pac_noised_avg(hash, col) / pac_avg(hash, col)
		auto hash_child = old_aggr.children[0]->Copy();
		auto value_child = old_aggr.children[1]->Copy();

		string sum_name = is_noised ? "pac_noised_sum" : "pac_sum";
		string count_name = is_noised ? "pac_noised_count" : "pac_count";

		// Replace avg at position with sum
		agg.expressions[pos] = BindPacAggregate(input, sum_name, hash_child->Copy(), value_child->Copy());

		// Append count at the end
		idx_t count_pos = agg.expressions.size();
		agg.expressions.push_back(BindPacAggregate(input, count_name, std::move(hash_child), std::move(value_child)));

		decomp_map[pos] = {pos, count_pos, is_noised};
	}

	agg.ResolveOperatorTypes();

	// Build replacement expressions: for each avg position, create division expression
	unordered_map<idx_t, unique_ptr<Expression>> replacements;
	for (auto &kv : decomp_map) {
		auto avg_col = kv.first;
		auto &info = kv.second;
		auto sum_type = agg.expressions[info.sum_col]->return_type;
		auto count_type = agg.expressions[info.count_col]->return_type;

		auto sum_ref = make_uniq<BoundColumnRefExpression>(sum_type, ColumnBinding(agg_index, info.sum_col));
		auto cnt_ref = make_uniq<BoundColumnRefExpression>(count_type, ColumnBinding(agg_index, info.count_col));

		unique_ptr<Expression> div_expr;
		if (info.is_noised) {
			// Noised path: pac_noised_sum returns scalar, pac_noised_count returns BIGINT.
			// Use regular "/" division and cast to DOUBLE (like SQL avg).
			auto sum_as_double = BoundCastExpression::AddDefaultCastToType(std::move(sum_ref), LogicalType::DOUBLE);
			auto cnt_as_double = BoundCastExpression::AddDefaultCastToType(std::move(cnt_ref), LogicalType::DOUBLE);
			div_expr = input.optimizer.BindScalarFunction("/", std::move(sum_as_double), std::move(cnt_as_double));
		} else {
			// Counters path: pac_sum/pac_count return LIST<FLOAT>, use pac_div for element-wise division.
			div_expr = input.optimizer.BindScalarFunction("pac_div", std::move(sum_ref), std::move(cnt_ref));
		}

		replacements[avg_col] = std::move(div_expr);
	}

	// Propagate replacements into all operators above the aggregate
	PropagateAvgReplacements(*plan, &agg, replacements, agg_index);
}

// Collect LogicalAggregate nodes bottom-up
static void CollectAggregates(LogicalOperator &op, vector<reference<LogicalAggregate>> &aggs) {
	for (auto &child : op.children) {
		CollectAggregates(*child, aggs);
	}
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		aggs.push_back(op.Cast<LogicalAggregate>());
	}
}

void RewritePacAvgToDiv(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	vector<reference<LogicalAggregate>> aggs;
	CollectAggregates(*plan, aggs);
	for (auto &agg_ref : aggs) {
		RewritePacAvgAggregate(input, plan, agg_ref.get());
	}
}

} // namespace duckdb
