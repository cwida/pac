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

// Track which aggregate bindings were converted to counter type (uses HashBinding from categorical_detection.hpp)
using CounterBindings = unordered_set<uint64_t>;

static void ConvertAggregatesRecursive(OptimizerExtensionInput &input, LogicalOperator *op,
                                       CounterBindings &converted) {
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

		// Copy children and rebind with counter variant
		vector<unique_ptr<Expression>> children;
		for (auto &child_expr : bound_agg.children) {
			children.push_back(child_expr->Copy());
		}

		auto new_aggr = RebindAggregate(input.context, counters_name, std::move(children), bound_agg.IsDistinct());
		if (new_aggr) {
			agg.expressions[i] = std::move(new_aggr);
		} else {
			// Fallback: rename in place
			bound_agg.function.name = counters_name;
			bound_agg.function.return_type = LogicalType::LIST(PacFloatLogicalType());
			expr->return_type = LogicalType::LIST(PacFloatLogicalType());
		}

		// Update the aggregate's type vector
		idx_t types_index = agg.groups.size() + i;
		if (types_index < agg.types.size()) {
			agg.types[types_index] = LogicalType::LIST(PacFloatLogicalType());
		}

		// Track this binding for downstream type propagation
		converted.insert(HashBinding(ColumnBinding(agg.aggregate_index, i)));
	}
}

// Fix stale column ref types in all operators after aggregate conversion.
// Same pattern as categorical rewriter (pac_categorical_rewriter.cpp lines 503-516).
static void FixStaleColumnRefTypes(LogicalOperator *op, const CounterBindings &converted) {
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
	CounterBindings converted;
	ConvertAggregatesRecursive(input, plan.get(), converted);
	if (!converted.empty()) {
		FixStaleColumnRefTypes(plan.get(), converted);
	}
}

// ============================================================================
// Read path: inject pac_finalize for SELECT on derived_pu tables
// ============================================================================

// Collect GET table_indexes that are derived_pu with counter columns, and which column indices are counters.
struct DerivedPuGetInfo {
	idx_t table_index;
	unordered_set<idx_t> counter_columns; // column indices within the GET that are LIST<FLOAT>
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

// Wrap an expression with pac_finalize using the optimizer's BindScalarFunction
static unique_ptr<Expression> WrapWithFinalize(OptimizerExtensionInput &input, unique_ptr<Expression> expr) {
	auto result = input.optimizer.BindScalarFunction("pac_finalize", std::move(expr));
	if (!result) {
		Printer::Print("[PAC DERIVED READ] ERROR: BindScalarFunction returned null!");
	} else {
		Printer::Print("[PAC DERIVED READ] Bound pac_mean: " + result->ToString() +
		               " type=" + result->return_type.ToString());
	}
	return result;
}

// Walk all expressions in the plan. For column refs to derived_pu counter columns, wrap with pac_finalize.
// Also collects changed bindings so parent operators' column refs can be updated.
static void RewriteExpressionsForFinalize(OptimizerExtensionInput &input, LogicalOperator *op,
                                          const vector<DerivedPuGetInfo> &gets,
                                          unordered_set<uint64_t> &changed_bindings) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		RewriteExpressionsForFinalize(input, child.get(), gets, changed_bindings);
	}
	// Don't rewrite expressions inside the GET itself
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	// First: fix stale column refs from already-changed bindings (propagation from child operators)
	auto finalized_type = PacFloatLogicalType();
	std::function<void(unique_ptr<Expression> &)> FixExpr = [&](unique_ptr<Expression> &e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			if (changed_bindings.count(HashBinding(col_ref.binding))) {
				col_ref.return_type = finalized_type;
			}
		}
		ExpressionIterator::EnumerateChildren(*e, FixExpr);
	};
	// Fix expressions in the operator
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) {
		FixExpr(*expr_ptr);
	});
	// Also fix ORDER BY expressions (stored in orders, not op->expressions)
	if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op->Cast<LogicalOrder>();
		for (auto &node : order.orders) {
			FixExpr(node.expression);
		}
	}

	// Then: wrap direct counter column refs with pac_finalize
	for (idx_t i = 0; i < op->expressions.size(); i++) {
		auto &expr = op->expressions[i];
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = expr->Cast<BoundColumnRefExpression>();
			for (auto &info : gets) {
				if (col_ref.binding.table_index == info.table_index &&
				    info.counter_columns.count(col_ref.binding.column_index)) {
					expr = WrapWithFinalize(input, std::move(expr));
					break;
				}
			}
		}
	}

	// Track this operator's output bindings that changed type (for propagation to parents)
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			if (proj.expressions[i]->return_type == finalized_type) {
				// Check if this was originally a counter column (FLOAT[])
				// by seeing if the binding was in our changed set or was just wrapped
				auto binding = ColumnBinding(proj.table_index, i);
				if (proj.expressions[i]->type == ExpressionType::BOUND_FUNCTION) {
					// This is a pac_finalize wrapper — track the output binding
					changed_bindings.insert(HashBinding(binding));
				}
			}
		}
	}
}

void InjectPacFinalizeForDerivedPu(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	vector<DerivedPuGetInfo> gets;
	FindDerivedPuGets(plan.get(), gets);
	if (gets.empty()) {
		return;
	}
	Printer::Print("[PAC DERIVED READ] === PLAN BEFORE pac_finalize injection ===");
	plan->Print();
	unordered_set<uint64_t> changed_bindings;
	RewriteExpressionsForFinalize(input, plan.get(), gets, changed_bindings);
	// Resolve types so all operator type vectors are consistent
	plan->ResolveOperatorTypes();

	// Now fix ALL stale column refs: for each operator, build a type map from child bindings,
	// then update any column ref whose return_type doesn't match.
	std::function<void(LogicalOperator *)> FixAllStaleRefs = [&](LogicalOperator *op) {
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
		// Fix column refs in expressions
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
		// Also fix ORDER BY orders
		if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
			auto &order = op->Cast<LogicalOrder>();
			for (auto &node : order.orders) {
				Fix(node.expression);
			}
		}
	};
	FixAllStaleRefs(plan.get());
	Printer::Print("[PAC DERIVED READ] === PLAN AFTER pac_finalize injection ===");
	plan->Print();
}

} // namespace duckdb
