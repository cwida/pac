//
// Created by ila on 1/16/26.
//

#include "query_processing/pac_projection_propagation.hpp"
#include "pac_debug.hpp"
#include "utils/pac_helpers.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

// Helper to check if an operator is a join type that has projection maps
static bool IsJoinWithProjectionMap(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_JOIN || type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	       type == LogicalOperatorType::LOGICAL_DELIM_JOIN || type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	       type == LogicalOperatorType::LOGICAL_ASOF_JOIN || type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
}

struct PathEntry {
	LogicalOperator *op;
	idx_t child_idx; // Which child led to the target (0 = left, 1 = right for joins)
};

// Find path from an operator to a specific table_index, stopping at nested aggregates.
// Matches both LogicalGet and LogicalProjection (for hash projections inserted above gets).
static bool FindDirectPathToSource(LogicalOperator *current, idx_t target_table_index, vector<PathEntry> &path,
                                   bool is_start = true) {
	if (!current) {
		return false;
	}

	// Check if this operator matches the target table_index
	if (current->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = current->Cast<LogicalGet>();
		if (get.table_index == target_table_index) {
			return true;
		}
	} else if (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = current->Cast<LogicalProjection>();
		if (proj.table_index == target_table_index) {
			return true;
		}
	}

	// Stop at nested aggregates - they have their own column scope
	if (!is_start && current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}

	for (idx_t child_idx = 0; child_idx < current->children.size(); child_idx++) {
		if (FindDirectPathToSource(current->children[child_idx].get(), target_table_index, path, false)) {
			if (current->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				path.push_back({current, child_idx});
			}
			return true;
		}
	}

	return false;
}

// Ensure a single column_index is present in a projection map. Returns true if it was already there.
static bool EnsureInProjectionMap(vector<idx_t> &proj_map, idx_t column_index) {
	if (proj_map.empty()) {
		return true; // Empty map means all columns pass through
	}
	for (auto &idx : proj_map) {
		if (idx == column_index) {
			return true;
		}
	}
	proj_map.push_back(column_index);
	return false;
}

// Verify a binding exists in an operator's output bindings
static bool BindingInOutput(LogicalOperator &op, const ColumnBinding &binding) {
	auto bindings = op.GetColumnBindings();
	for (auto &b : bindings) {
		if (b.table_index == binding.table_index && b.column_index == binding.column_index) {
			return true;
		}
	}
	return false;
}

ColumnBinding PropagateSingleBinding(LogicalOperator &plan_root, idx_t source_table_index, ColumnBinding source_binding,
                                     const LogicalType &source_type, LogicalAggregate *target_agg) {
	ColumnBinding invalid(DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);

	vector<PathEntry> path_ops;
	if (!FindDirectPathToSource(target_agg, source_table_index, path_ops, true)) {
#if PAC_DEBUG
		PAC_DEBUG_PRINT("PropagateSingleBinding: No direct path from aggregate to source #" +
		                std::to_string(source_table_index));
#endif
		return invalid;
	}

	if (path_ops.empty()) {
		// Aggregate reads directly from source — binding is already valid
		return source_binding;
	}

	// Propagate through each operator (bottom-up: closest to source first)
	ColumnBinding current = source_binding;

	for (auto &entry : path_ops) {
		auto *op = entry.op;
		idx_t child_idx = entry.child_idx;

		if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &proj = op->Cast<LogicalProjection>();

			// Check if this binding is already in the projection's expressions
			idx_t existing_idx = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < proj.expressions.size(); i++) {
				if (proj.expressions[i]->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &expr_ref = proj.expressions[i]->Cast<BoundColumnRefExpression>();
					if (expr_ref.binding == current) {
						existing_idx = i;
						break;
					}
				}
			}

			if (existing_idx != DConstants::INVALID_INDEX) {
				current = ColumnBinding(proj.table_index, existing_idx);
			} else {
				// Add a passthrough column ref for this binding
				auto col_ref = make_uniq<BoundColumnRefExpression>(source_type, current);
				proj.expressions.push_back(std::move(col_ref));
				current = ColumnBinding(proj.table_index, proj.expressions.size() - 1);
			}
			proj.ResolveOperatorTypes();

		} else if (IsJoinWithProjectionMap(op->type)) {
			auto &join = op->Cast<LogicalJoin>();
			bool is_delim_join = (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);

			// Check for incompatible join types
			if ((join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) && child_idx == 0) {
				return invalid;
			}
			if ((join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI ||
			     join.join_type == JoinType::MARK) &&
			    child_idx == 1) {
				return invalid;
			}

			// DELIM_JOIN from left child
			if (is_delim_join && child_idx == 0) {
				EnsureInProjectionMap(join.left_projection_map, current.column_index);
				join.ResolveOperatorTypes();
				// Binding passes through unchanged for DELIM_JOIN left child
				continue;
			}

			// Normal join: ensure column is in the appropriate projection map
			if (child_idx == 0) {
				EnsureInProjectionMap(join.left_projection_map, current.column_index);
			} else {
				EnsureInProjectionMap(join.right_projection_map, current.column_index);
			}
			join.ResolveOperatorTypes();

			// Verify binding is in the output (joins pass through child bindings)
			if (!BindingInOutput(join, current)) {
#if PAC_DEBUG
				PAC_DEBUG_PRINT("PropagateSingleBinding: WARNING - binding [" + std::to_string(current.table_index) +
				                "." + std::to_string(current.column_index) + "] not found in join output");
#endif
			}
			// Binding identity preserved through joins

		} else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = op->Cast<LogicalFilter>();
			EnsureInProjectionMap(filter.projection_map, current.column_index);
			filter.ResolveOperatorTypes();
			// Binding identity preserved through filters

		} else {
			op->ResolveOperatorTypes();
		}
	}

	return current;
}

// Backward-compatible wrapper: propagates a complex hash expression (possibly with multiple bindings)
// through the operator chain. Delegates to PropagateSingleBinding for each binding in the expression.
unique_ptr<Expression> PropagatePKThroughProjections(LogicalOperator &plan, LogicalGet &pu_get,
                                                     unique_ptr<Expression> hash_expr, LogicalAggregate *target_agg) {
	ColumnBinding invalid(DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);

	// Extract all column references from the expression
	struct BindingInfo {
		ColumnBinding binding;
		LogicalType type;
	};
	vector<BindingInfo> bindings;
	ExpressionIterator::EnumerateExpression(hash_expr, [&](Expression &expr) {
		if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = expr.Cast<BoundColumnRefExpression>();
			bindings.push_back({col_ref.binding, col_ref.return_type});
		}
	});

	// Propagate each binding individually and build replacement map
	auto binding_key = [](const ColumnBinding &b) -> uint64_t {
		return (static_cast<uint64_t>(b.table_index) << 32) | static_cast<uint64_t>(b.column_index);
	};
	std::unordered_map<uint64_t, ColumnBinding> replacement_map;

	for (auto &info : bindings) {
		auto result = PropagateSingleBinding(plan, pu_get.table_index, info.binding, info.type, target_agg);
		if (result.table_index == DConstants::INVALID_INDEX) {
			return nullptr;
		}
		replacement_map[binding_key(info.binding)] = result;
	}

	// Update all bindings in the expression copy
	auto updated = hash_expr->Copy();
	ExpressionIterator::EnumerateExpression(updated, [&](Expression &expr) {
		if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = expr.Cast<BoundColumnRefExpression>();
			auto key = binding_key(col_ref.binding);
			auto it = replacement_map.find(key);
			if (it != replacement_map.end()) {
				col_ref.binding = it->second;
			}
		}
	});

	return updated;
}

} // namespace duckdb
