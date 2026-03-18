#include "query_processing/pac_derived_rewriter.hpp"

#include "aggregates/pac_aggregate.hpp"
#include "categorical/pac_categorical_detection.hpp"
#include "metadata/pac_metadata_manager.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

// ============================================================================
// Write path: convert pac_noised_* → pac_* counter variants for derived_pu DML
// ============================================================================

static void ConvertAggregatesRecursive(OptimizerExtensionInput &input, LogicalOperator *op,
                                       unordered_set<uint64_t> &converted) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		ConvertAggregatesRecursive(input, child.get(), converted);
	}

	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}

	auto &agg = op->Cast<LogicalAggregate>();
	auto list_type = LogicalType::LIST(PacFloatLogicalType());

	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		auto &expr = agg.expressions[i];
		if (expr->type != ExpressionType::BOUND_AGGREGATE) {
			continue;
		}

		auto &bound_agg = expr->Cast<BoundAggregateExpression>();
		if (!IsPacAggregate(bound_agg.function.name)) {
			continue;
		}

		string counters_name = GetCountersVariant(bound_agg.function.name);
		if (counters_name.empty()) {
			continue;
		}

		vector<unique_ptr<Expression>> children;
		for (auto &child_expr : bound_agg.children) {
			children.push_back(child_expr->Copy());
		}

		auto new_aggr = RebindAggregate(input.context, counters_name, std::move(children), bound_agg.IsDistinct());
		if (new_aggr) {
			agg.expressions[i] = std::move(new_aggr);
		} else {
			bound_agg.function.name = counters_name;
			bound_agg.function.return_type = list_type;
			expr->return_type = list_type;
		}

		idx_t types_index = agg.groups.size() + i;
		if (types_index < agg.types.size()) {
			agg.types[types_index] = list_type;
		}

		converted.insert(HashBinding(ColumnBinding(agg.aggregate_index, i)));
	}
}

// Fix stale column ref types after aggregate conversion (same pattern as categorical rewriter).
static void FixStaleColumnRefTypes(LogicalOperator *op, const unordered_set<uint64_t> &converted) {
	if (!op) {
		return;
	}
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) {
		std::function<void(unique_ptr<Expression> &)> Fix = [&](unique_ptr<Expression> &e) {
			if (e->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = e->Cast<BoundColumnRefExpression>();
				if (converted.count(HashBinding(col_ref.binding))) {
					col_ref.return_type = list_type;
				}
			}
			ExpressionIterator::EnumerateChildren(*e, Fix);
		};
		Fix(*expr_ptr);
	});
	for (auto &child : op->children) {
		FixStaleColumnRefTypes(child.get(), converted);
	}
}

void ConvertDerivedPuToCounters(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	unordered_set<uint64_t> converted;
	ConvertAggregatesRecursive(input, plan.get(), converted);
	if (!converted.empty()) {
		FixStaleColumnRefTypes(plan.get(), converted);
	}
}

// ============================================================================
// Read path: inject pac_finalize for SELECT on derived_pu tables
// ============================================================================

struct DerivedPuGetInfo {
	idx_t table_index;
	unordered_set<idx_t> counter_columns;
};

static void FindDerivedPuGets(LogicalOperator *op, vector<DerivedPuGetInfo> &gets) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		FindDerivedPuGets(child.get(), gets);
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op->Cast<LogicalGet>();
	auto entry = get.GetTable();
	if (!entry) {
		return;
	}
	auto &mgr = PACMetadataManager::Get();
	auto *meta = mgr.GetTableMetadata(entry->name);
	if (!meta || !meta->derived_pu) {
		return;
	}
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	DerivedPuGetInfo info;
	info.table_index = get.table_index;
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		if (get.returned_types[i] == list_type) {
			info.counter_columns.insert(i);
		}
	}
	if (!info.counter_columns.empty()) {
		gets.push_back(std::move(info));
	}
}

// Wrap column refs to counter columns with pac_finalize, propagating type changes bottom-up.
// Uses changed_bindings to track which output bindings have changed type so parent operators
// can have their stale column refs updated.
static void WrapCounterRefsWithFinalize(OptimizerExtensionInput &input, LogicalOperator *op,
                                        const vector<DerivedPuGetInfo> &gets,
                                        unordered_set<uint64_t> &changed_bindings) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		WrapCounterRefsWithFinalize(input, child.get(), gets, changed_bindings);
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto finalized_type = PacFloatLogicalType();

	// Fix stale column refs from already-changed child bindings
	std::function<void(unique_ptr<Expression> &)> FixExpr = [&](unique_ptr<Expression> &e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			if (changed_bindings.count(HashBinding(col_ref.binding))) {
				col_ref.return_type = finalized_type;
			}
		}
		ExpressionIterator::EnumerateChildren(*e, FixExpr);
	};
	for (auto &expr : op->expressions) {
		FixExpr(expr);
	}
	if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op->Cast<LogicalOrder>();
		for (auto &node : order.orders) {
			FixExpr(node.expression);
		}
	}

	// Wrap direct counter column refs with pac_finalize
	for (auto &expr : op->expressions) {
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = expr->Cast<BoundColumnRefExpression>();
			for (auto &info : gets) {
				if (col_ref.binding.table_index == info.table_index &&
				    info.counter_columns.count(col_ref.binding.column_index)) {
					expr = input.optimizer.BindScalarFunction("pac_finalize", std::move(expr));
					break;
				}
			}
		}
	}

	// Track changed output bindings for parent propagation
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			if (proj.expressions[i]->return_type == finalized_type &&
			    proj.expressions[i]->type == ExpressionType::BOUND_FUNCTION) {
				changed_bindings.insert(HashBinding(ColumnBinding(proj.table_index, i)));
			}
		}
	}
}

// After ResolveOperatorTypes, fix all column refs whose return_type doesn't match
// the source operator's output type. Handles projections, ORDER BY, filters, subqueries.
static void FixAllStaleRefs(LogicalOperator *op) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		FixAllStaleRefs(child.get());
	}

	// Build binding → type map from child operators
	unordered_map<uint64_t, LogicalType> binding_types;
	for (auto &child : op->children) {
		auto bindings = child->GetColumnBindings();
		for (idx_t i = 0; i < bindings.size() && i < child->types.size(); i++) {
			binding_types[HashBinding(bindings[i])] = child->types[i];
		}
	}

	if (binding_types.empty()) {
		return;
	}

	// Fix column refs in any expression
	std::function<void(unique_ptr<Expression> &)> Fix = [&](unique_ptr<Expression> &e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			auto it = binding_types.find(HashBinding(col_ref.binding));
			if (it != binding_types.end() && col_ref.return_type != it->second) {
				col_ref.return_type = it->second;
			}
		}
		ExpressionIterator::EnumerateChildren(*e, Fix);
	};

	for (auto &expr : op->expressions) {
		Fix(expr);
	}
	if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op->Cast<LogicalOrder>();
		for (auto &node : order.orders) {
			Fix(node.expression);
		}
	}
}

bool HasDerivedPuCounterGets(LogicalOperator *plan) {
	vector<DerivedPuGetInfo> gets;
	FindDerivedPuGets(plan, gets);
	return !gets.empty();
}

void InjectPacFinalizeForDerivedPu(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	vector<DerivedPuGetInfo> gets;
	FindDerivedPuGets(plan.get(), gets);
	if (gets.empty()) {
		return;
	}
	unordered_set<uint64_t> changed_bindings;
	WrapCounterRefsWithFinalize(input, plan.get(), gets, changed_bindings);
	plan->ResolveOperatorTypes();
	FixAllStaleRefs(plan.get());
}

} // namespace duckdb
