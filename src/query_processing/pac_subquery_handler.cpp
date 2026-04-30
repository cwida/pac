//
// PAC Subquery Handler
//
// This file contains functions for handling correlated subqueries and DELIM_JOIN operations
// in PAC query compilation. These functions help manage column accessibility and propagation
// across subquery boundaries.
//
// Created by ila on 1/22/26.
//

#include "query_processing/pac_subquery_handler.hpp"
#include "privacy_debug.hpp"
#include "query_processing/pac_expression_builder.hpp"
#include "query_processing/pac_plan_traversal.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

// Recursive helper for EnsureBindingFlowsThrough.
// Traces a binding from the source operator up through the tree, modifying operators as needed.
static ColumnBinding EnsureBindingFlowsThroughRecursive(LogicalOperator *op, idx_t source_table_index,
                                                        ColumnBinding source_binding, const LogicalType &source_type) {
	ColumnBinding invalid(DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);

	// Base case: match any operator by table_index
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		if (op->Cast<LogicalGet>().table_index == source_table_index) {
			return source_binding;
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (op->Cast<LogicalProjection>().table_index == source_table_index) {
			return source_binding;
		}
	}

	// Recursively find and trace through children
	ColumnBinding child_result = invalid;
	for (auto &child : op->children) {
		child_result = EnsureBindingFlowsThroughRecursive(child.get(), source_table_index, source_binding, source_type);
		if (child_result.table_index != DConstants::INVALID_INDEX) {
			break;
		}
	}

	if (child_result.table_index == DConstants::INVALID_INDEX) {
		return child_result;
	}

	// Handle how the binding passes through this operator
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return child_result;
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &proj = op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			if (proj.expressions[i]->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = proj.expressions[i]->Cast<BoundColumnRefExpression>();
				if (col_ref.binding == child_result) {
					return ColumnBinding(proj.table_index, i);
				}
			}
		}
		return invalid;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &agg = op->Cast<LogicalAggregate>();
		// Check if binding is already in groups
		for (idx_t i = 0; i < agg.groups.size(); i++) {
			if (agg.groups[i]->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = agg.groups[i]->Cast<BoundColumnRefExpression>();
				if (col_ref.binding == child_result) {
					return ColumnBinding(agg.group_index, i);
				}
			}
		}
		// Add to groups so it passes through
		auto group_col_ref = make_uniq<BoundColumnRefExpression>(source_type, child_result);
		idx_t new_group_idx = agg.groups.size();
		agg.groups.push_back(std::move(group_col_ref));
		agg.ResolveOperatorTypes();
		return ColumnBinding(agg.group_index, new_group_idx);
	}
	default:
		return child_result;
	}
}

// Helper: Ensure a binding flows through all operators between target_op and the operator
// identified by source_table_index. Matches both LogicalGet and LogicalProjection by table_index.
// May modify operators (e.g., add to aggregate groups) to ensure the binding flows through.
static ColumnBinding EnsureBindingFlowsThrough(LogicalOperator *target_op, idx_t source_table_index,
                                               ColumnBinding source_binding, const LogicalType &source_type) {
	return EnsureBindingFlowsThroughRecursive(target_op, source_table_index, source_binding, source_type);
}

// Check if a subtree contains an operator (GET or PROJECTION) with the given table_index.
static bool HasSourceTableInSubtree(LogicalOperator *op, idx_t source_table_index) {
	if (!op) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		if (op->Cast<LogicalGet>().table_index == source_table_index) {
			return true;
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (op->Cast<LogicalProjection>().table_index == source_table_index) {
			return true;
		}
	}
	for (auto &c : op->children) {
		if (HasSourceTableInSubtree(c.get(), source_table_index)) {
			return true;
		}
	}
	return false;
}

// Recursive search for FindDelimJoinForSource.
static LogicalComparisonJoin *SearchForDelimJoin(LogicalOperator *op, idx_t source_table_index,
                                                 LogicalAggregate *target_agg) {
	if (!op) {
		return nullptr;
	}

	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();

		// Check if target_agg is in children[1] (subquery side)
		bool agg_in_right = join.children.size() >= 2 && HasNodeInSubtree(join.children[1].get(), target_agg);

		// Check if source is in children[0] (outer query side) by table_index
		bool source_in_left =
		    !join.children.empty() && HasSourceTableInSubtree(join.children[0].get(), source_table_index);

		if (agg_in_right && source_in_left) {
			return &join;
		}
	}

	for (auto &child : op->children) {
		auto *found = SearchForDelimJoin(child.get(), source_table_index, target_agg);
		if (found) {
			return found;
		}
	}
	return nullptr;
}

// Helper: find the DELIM_JOIN that connects source_table_index (in left child) to target_agg (in right child)
static LogicalComparisonJoin *FindDelimJoinForSource(LogicalOperator *root, idx_t source_table_index,
                                                     LogicalAggregate *target_agg) {
	return SearchForDelimJoin(root, source_table_index, target_agg);
}

// Helper: find the DELIM_GET in an aggregate's subtree
static LogicalDelimGet *FindDelimGetInSubtree(LogicalOperator *op) {
	if (!op) {
		return nullptr;
	}
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return &op->Cast<LogicalDelimGet>();
	}
	for (auto &child : op->children) {
		auto *found = FindDelimGetInSubtree(child.get());
		if (found) {
			return found;
		}
	}
	return nullptr;
}

// Update all DELIM_GET nodes in a subtree to include a new column type.
static void UpdateDelimGetsInSubtree(LogicalOperator *op, const LogicalType &source_type) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		auto &dg = op->Cast<LogicalDelimGet>();
#if PRIVACY_DEBUG
		PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: Updating DELIM_GET #" + std::to_string(dg.table_index) +
		                    " chunk_types before=" + std::to_string(dg.chunk_types.size()));
#endif
		dg.chunk_types.push_back(source_type);
		op->ResolveOperatorTypes();
#if PRIVACY_DEBUG
		PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: DELIM_GET #" + std::to_string(dg.table_index) +
		                    " chunk_types after=" + std::to_string(dg.chunk_types.size()) +
		                    " types after=" + std::to_string(dg.types.size()));
#endif
	}
	for (auto &child : op->children) {
		UpdateDelimGetsInSubtree(child.get(), source_type);
	}
}

// Core function: add a binding to a DELIM_JOIN's duplicate_eliminated_columns
DelimColumnResult AddBindingToDelimJoin(unique_ptr<LogicalOperator> &plan, idx_t source_table_index,
                                        ColumnBinding source_binding, const LogicalType &source_type,
                                        LogicalAggregate *target_agg) {
	DelimColumnResult invalid_result;
	invalid_result.binding = ColumnBinding(DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
	invalid_result.type = LogicalType::INVALID;

	auto *delim_join = FindDelimJoinForSource(plan.get(), source_table_index, target_agg);
	if (!delim_join) {
#if PRIVACY_DEBUG
		PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: No DELIM_JOIN found for source #" +
		                    std::to_string(source_table_index));
#endif
		return invalid_result;
	}

#if PRIVACY_DEBUG
	PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: Found DELIM_JOIN, source_table_index=" +
	                    std::to_string(source_table_index) + " binding=[" + std::to_string(source_binding.table_index) +
	                    "." + std::to_string(source_binding.column_index) + "]");
	PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: Left child type=" +
	                    std::to_string(static_cast<int>(delim_join->children[0]->type)));
#endif

	// Trace the binding through the left child of DELIM_JOIN
	auto *left_child = delim_join->children[0].get();
	ColumnBinding output_binding =
	    EnsureBindingFlowsThrough(left_child, source_table_index, source_binding, source_type);

	if (output_binding.table_index == DConstants::INVALID_INDEX) {
#if PRIVACY_DEBUG
		PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: EnsureBindingFlowsThrough failed");
#endif
		return invalid_result;
	}

#if PRIVACY_DEBUG
	PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: output_binding=[" + std::to_string(output_binding.table_index) + "." +
	                    std::to_string(output_binding.column_index) + "]");
	PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: duplicate_eliminated_columns.size()=" +
	                    std::to_string(delim_join->duplicate_eliminated_columns.size()));
	for (idx_t i = 0; i < delim_join->duplicate_eliminated_columns.size(); i++) {
		auto &dec = delim_join->duplicate_eliminated_columns[i];
		PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: existing dup_elim[" + std::to_string(i) + "] = " + dec->ToString() +
		                    " type=" + dec->return_type.ToString());
	}
	// Dump DELIM_JOIN conditions
	auto &dj = delim_join->Cast<LogicalComparisonJoin>();
	for (idx_t i = 0; i < dj.conditions.size(); i++) {
		PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: condition[" + std::to_string(i) + "] left=" +
		                    dj.conditions[i].left->ToString() + " right=" + dj.conditions[i].right->ToString());
	}
#endif

	// Add to DELIM_JOIN's duplicate_eliminated_columns
	auto col_ref = make_uniq<BoundColumnRefExpression>(source_type, output_binding);
	idx_t new_col_idx = delim_join->duplicate_eliminated_columns.size();
	delim_join->duplicate_eliminated_columns.push_back(std::move(col_ref));

	// Update all DELIM_GETs in the subquery to include the new column type
	if (delim_join->children.size() >= 2) {
		UpdateDelimGetsInSubtree(delim_join->children[1].get(), source_type);
	}

	// Find DELIM_GET in aggregate's subtree and return binding
	auto *delim_get = FindDelimGetInSubtree(target_agg);
	if (!delim_get) {
		return invalid_result;
	}

#if PRIVACY_DEBUG
	PRIVACY_DEBUG_PRINT("AddBindingToDelimJoin: DELIM_GET #" + std::to_string(delim_get->table_index) + " now has " +
	                    std::to_string(delim_get->chunk_types.size()) + " chunk_types, " +
	                    std::to_string(delim_get->types.size()) + " types");
#endif

	DelimColumnResult result;
	result.binding = ColumnBinding(delim_get->table_index, new_col_idx);
	result.type = source_type;
	return result;
}

// Thin wrapper: resolve column name to binding, then delegate to AddBindingToDelimJoin
DelimColumnResult AddColumnToDelimJoin(unique_ptr<LogicalOperator> &plan, LogicalGet &source_get,
                                       const string &column_name, LogicalAggregate *target_agg) {
	DelimColumnResult invalid_result;
	invalid_result.binding = ColumnBinding(DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
	invalid_result.type = LogicalType::INVALID;

	// Ensure the column is projected in source_get
	idx_t col_proj_idx = EnsureProjectedColumn(source_get, column_name);
	if (col_proj_idx == DConstants::INVALID_INDEX) {
		return invalid_result;
	}

	// Get the column type
	auto col_index = source_get.GetColumnIds()[col_proj_idx];
	auto col_type = source_get.GetColumnType(col_index);

	// Determine the output binding for the source_get
	idx_t output_col_idx;
	if (source_get.projection_ids.empty()) {
		output_col_idx = col_proj_idx;
	} else {
		output_col_idx = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < source_get.projection_ids.size(); i++) {
			if (source_get.projection_ids[i] == col_proj_idx) {
				output_col_idx = source_get.projection_ids[i];
				break;
			}
		}
		if (output_col_idx == DConstants::INVALID_INDEX) {
			source_get.projection_ids.push_back(col_proj_idx);
			output_col_idx = col_proj_idx;
			source_get.ResolveOperatorTypes();
		}
	}

	ColumnBinding source_binding(source_get.table_index, output_col_idx);
	return AddBindingToDelimJoin(plan, source_get.table_index, source_binding, col_type, target_agg);
}

} // namespace duckdb
