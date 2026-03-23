//
// PAC Derived Table Rewriter
//
// Handles read and write paths for derived_pu tables (materialized views of PU data).
// Derived_pu tables store PAC counter lists (LIST<FLOAT>, 64 elements) instead of scalar values.
//
// Write path (ConvertDerivedPuToCounters):
//   Converts pac_noised_* aggregates to pac_* counter variants in INSERT/CTAS plans,
//   so the table stores raw counter lists instead of finalized noised scalars.
//
// Read path (InjectPacFinalizeForDerivedPu):
//   Wraps counter column refs with pac_finalize() in SELECT plans, converting
//   LIST<FLOAT> counters to noised FLOAT scalars at read time.
//   Runs as a post-optimizer rule (after DuckDB's built-in optimizers).
//
// Key challenges:
//   - DuckDB's optimizer reorders columns and inserts intermediate projections,
//     so we use op->types (resolved output) for binding identification rather than
//     position-based matching against the original GET.
//   - AGGREGATE operators (e.g. first() in scalar subqueries) need raw FLOAT[] input,
//     so pac_finalize injection is suppressed inside aggregate subtrees.
//   - Scalar subqueries use CTEs internally; CTE_REF chunk_types and subquery
//     return types must be updated after pac_finalize changes the plan output.
//

#include "query_processing/pac_derived_rewriter.hpp"

#include "aggregates/pac_aggregate.hpp"
#include "categorical/pac_categorical_detection.hpp"
#include "metadata/pac_metadata_manager.hpp"
#include "pac_debug.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

// --- Helpers ---

static LogicalType CounterListType() {
	return LogicalType::LIST(PacFloatLogicalType());
}

// Returns true if the expression tree contains a pac_finalize call.
static bool ExpressionContainsPacFinalize(const Expression &e) {
	if (e.type == ExpressionType::BOUND_FUNCTION && e.Cast<BoundFunctionExpression>().function.name == "pac_finalize") {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(const_cast<Expression &>(e), [&](Expression &child) {
		if (!found && ExpressionContainsPacFinalize(child)) {
			found = true;
		}
	});
	return found;
}

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
	auto list_type = CounterListType();

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
			PAC_DEBUG_PRINT("[PAC DERIVED WRITE] Rebound " + bound_agg.function.name + " → " +
			                new_aggr->Cast<BoundAggregateExpression>().function.name);
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

static void FixStaleColumnRefTypes(LogicalOperator *op, const unordered_set<uint64_t> &converted) {
	if (!op) {
		return;
	}
	auto list_type = CounterListType();
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

static void StripRedundantCasts(LogicalOperator *op) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		StripRedundantCasts(child.get());
	}
	std::function<void(unique_ptr<Expression> &)> Strip = [&](unique_ptr<Expression> &e) {
		ExpressionIterator::EnumerateChildren(*e, Strip);
		if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast = e->Cast<BoundCastExpression>();
			if (cast.child->return_type == cast.return_type) {
				e = std::move(cast.child);
			}
		}
	};
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) { Strip(*expr_ptr); });
}

void ConvertDerivedPuToCounters(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	unordered_set<uint64_t> converted;
	ConvertAggregatesRecursive(input, plan.get(), converted);
	if (!converted.empty()) {
		FixStaleColumnRefTypes(plan.get(), converted);
		StripRedundantCasts(plan.get());
	}
}

// ============================================================================
// Read path: inject pac_finalize for SELECT on derived_pu tables
// ============================================================================

// Find all GET operators that scan derived_pu tables with counter columns.
static void FindDerivedPuGets(LogicalOperator *op, unordered_set<idx_t> &derived_table_indices) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		FindDerivedPuGets(child.get(), derived_table_indices);
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
	auto list_type = CounterListType();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		if (get.returned_types[i] == list_type) {
			derived_table_indices.insert(get.table_index);
			return;
		}
	}
}

// Wrap counter column refs with pac_finalize, bottom-up through the plan.
//
// Uses op->types (resolved after DuckDB's optimizer) to identify counter columns
// because the optimizer may reorder columns, making position-based matching unreliable.
//
// Suppresses wrapping inside AGGREGATE subtrees — aggregates (e.g. first() in scalar
// subqueries, SUM() over counter columns) need raw FLOAT[] input. pac_finalize is
// applied above the aggregate via projection wrapping or the registered implicit cast.
static void WrapCounterRefsWithFinalize(OptimizerExtensionInput &input, LogicalOperator *op,
                                        const unordered_set<idx_t> &derived_table_indices,
                                        unordered_set<uint64_t> &counter_bindings,
                                        unordered_set<uint64_t> &finalized_bindings, bool suppress = false) {
	if (!op) {
		return;
	}
	// Propagate suppress flag into AGGREGATE subtrees
	bool child_suppress = suppress || (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	for (auto &child : op->children) {
		WrapCounterRefsWithFinalize(input, child.get(), derived_table_indices, counter_bindings, finalized_bindings,
		                            child_suppress);
	}

	auto list_type = CounterListType();
	auto finalized_type = PacFloatLogicalType();

	// Seed counter_bindings from derived_pu GET nodes
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (derived_table_indices.count(get.table_index)) {
			auto bindings = op->GetColumnBindings();
			auto &types = op->types;
			for (idx_t i = 0; i < bindings.size() && i < types.size(); i++) {
				if (types[i] == list_type) {
					counter_bindings.insert(HashBinding(bindings[i]));
					PAC_DEBUG_PRINT("[PAC DERIVED READ] Seeded counter binding (" +
					                std::to_string(bindings[i].table_index) + "," +
					                std::to_string(bindings[i].column_index) + ")");
				}
			}
		}
		return;
	}

	// Propagate counter bindings through PROJECTION and AGGREGATE output
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	    op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto out_bindings = op->GetColumnBindings();
		auto &out_types = op->types;
		for (idx_t i = 0; i < out_bindings.size() && i < out_types.size(); i++) {
			if (out_types[i] == list_type) {
				counter_bindings.insert(HashBinding(out_bindings[i]));
			}
		}
	}

	// Skip wrapping for AGGREGATE operators and suppressed subtrees
	if (suppress || op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}

	// Fix stale column refs that were finalized by a child operator
	std::function<void(unique_ptr<Expression> &)> FixExpr = [&](unique_ptr<Expression> &e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			if (finalized_bindings.count(HashBinding(col_ref.binding))) {
				col_ref.return_type = finalized_type;
			}
		}
		ExpressionIterator::EnumerateChildren(*e, FixExpr);
	};
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) { FixExpr(*expr_ptr); });

	// Wrap FLOAT[] column refs with pac_finalize and fix stale casts
	std::function<void(unique_ptr<Expression> &)> WrapExpr = [&](unique_ptr<Expression> &e) {
		// Skip already-wrapped expressions (prevents double-wrapping via EnumerateExpressions)
		if (e->type == ExpressionType::BOUND_FUNCTION &&
		    e->Cast<BoundFunctionExpression>().function.name == "pac_finalize") {
			return;
		}
		// Skip binder-inserted CAST(FLOAT[] → scalar) — the registered cast handles finalization
		if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast = e->Cast<BoundCastExpression>();
			if (cast.child->return_type == list_type && cast.return_type != list_type) {
				return;
			}
		}
		ExpressionIterator::EnumerateChildren(*e, WrapExpr);
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			if (col_ref.return_type == list_type && !finalized_bindings.count(HashBinding(col_ref.binding))) {
				PAC_DEBUG_PRINT("[PAC DERIVED READ] Wrapping binding (" + std::to_string(col_ref.binding.table_index) +
				                "," + std::to_string(col_ref.binding.column_index) + ") with pac_finalize");
				finalized_bindings.insert(HashBinding(col_ref.binding));
				e = input.optimizer.BindScalarFunction("pac_finalize", std::move(e));
			}
		}
		// Retarget CAST(scalar→FLOAT[]) to CAST(scalar→FLOAT), strip no-op casts
		if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast = e->Cast<BoundCastExpression>();
			if (cast.return_type == list_type && cast.child->return_type != list_type) {
				e = BoundCastExpression::AddDefaultCastToType(std::move(cast.child), finalized_type);
			} else if (cast.child->return_type == cast.return_type) {
				e = std::move(cast.child);
			}
		}
	};
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) { WrapExpr(*expr_ptr); });

	// Update operator output types after wrapping
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			if (i < proj.types.size()) {
				proj.types[i] = proj.expressions[i]->return_type;
			}
			bool is_finalized = (proj.expressions[i]->return_type == finalized_type) ||
			                    ExpressionContainsPacFinalize(*proj.expressions[i]);
			if (is_finalized) {
				finalized_bindings.insert(HashBinding(ColumnBinding(proj.table_index, i)));
				if (i < proj.types.size() && proj.types[i] == list_type) {
					proj.types[i] = finalized_type;
				}
				if (proj.expressions[i]->return_type == list_type) {
					proj.expressions[i]->return_type = finalized_type;
				}
			}
		}
	} else {
		// For pass-through operators (DISTINCT, ORDER BY, etc.): sync types from child
		for (idx_t i = 0; i < op->types.size(); i++) {
			if (op->types[i] == list_type) {
				for (auto &child : op->children) {
					if (i < child->types.size() && child->types[i] == finalized_type) {
						op->types[i] = finalized_type;
						auto out_bindings = op->GetColumnBindings();
						if (i < out_bindings.size()) {
							finalized_bindings.insert(HashBinding(out_bindings[i]));
						}
					}
				}
			}
		}
	}
}

// Fix stale column refs and subquery return types after pac_finalize injection.
static void FixAllStaleRefs(LogicalOperator *op) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		FixAllStaleRefs(child.get());
	}

	unordered_map<uint64_t, LogicalType> binding_types;
	for (auto &child : op->children) {
		auto bindings = child->GetColumnBindings();
		for (idx_t i = 0; i < bindings.size() && i < child->types.size(); i++) {
			binding_types[HashBinding(bindings[i])] = child->types[i];
		}
	}

	auto finalized_type = PacFloatLogicalType();
	auto list_type = CounterListType();

	std::function<void(unique_ptr<Expression> &)> Fix = [&](unique_ptr<Expression> &e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			auto it = binding_types.find(HashBinding(col_ref.binding));
			if (it != binding_types.end() && col_ref.return_type != it->second) {
				col_ref.return_type = it->second;
			}
		}
		// Fix scalar subquery return types to match the finalized inner plan
		if (e->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
			auto &subq = e->Cast<BoundSubqueryExpression>();
			if (subq.subquery.plan) {
				subq.subquery.plan->ResolveOperatorTypes();
				FixAllStaleRefs(subq.subquery.plan.get());
				auto &plan_types = subq.subquery.plan->types;
				for (idx_t j = 0; j < plan_types.size(); j++) {
					if (plan_types[j] != finalized_type) {
						continue;
					}
					if (subq.return_type == list_type) {
						subq.return_type = finalized_type;
					}
					if (j < subq.child_types.size() && subq.child_types[j] == list_type) {
						subq.child_types[j] = finalized_type;
					}
					if (j < subq.child_targets.size() && subq.child_targets[j] == list_type) {
						subq.child_targets[j] = finalized_type;
					}
					if (j < subq.subquery.types.size() && subq.subquery.types[j] == list_type) {
						subq.subquery.types[j] = finalized_type;
					}
				}
			}
		}
		ExpressionIterator::EnumerateChildren(*e, Fix);
	};
	LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) { Fix(*expr_ptr); });
}

// Fix CTE_REF chunk_types after the CTE definition's output types changed.
static void FixCTERefTypes(LogicalOperator *op) {
	if (!op) {
		return;
	}
	auto list_type = CounterListType();
	auto finalized_type = PacFloatLogicalType();
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && op->children.size() >= 2) {
		auto &def_types = op->children[0]->types;
		std::function<void(LogicalOperator *)> FixRefs = [&](LogicalOperator *node) {
			if (node->type == LogicalOperatorType::LOGICAL_CTE_REF) {
				auto &ref = node->Cast<LogicalCTERef>();
				for (idx_t i = 0; i < ref.chunk_types.size() && i < def_types.size(); i++) {
					if (ref.chunk_types[i] == list_type && def_types[i] == finalized_type) {
						ref.chunk_types[i] = finalized_type;
					}
				}
			}
			for (auto &child : node->children) {
				FixRefs(child.get());
			}
		};
		FixRefs(op->children[1].get());
	}
	for (auto &child : op->children) {
		FixCTERefTypes(child.get());
	}
}

// --- Public API ---

bool HasDerivedPuCounterGets(LogicalOperator *plan) {
	unordered_set<idx_t> indices;
	FindDerivedPuGets(plan, indices);
	return !indices.empty();
}

void InjectPacFinalizeForDerivedPu(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	unordered_set<idx_t> derived_table_indices;
	FindDerivedPuGets(plan.get(), derived_table_indices);
	if (derived_table_indices.empty()) {
		return;
	}
	PAC_DEBUG_PRINT("[PAC DERIVED READ] Injecting pac_finalize for " + std::to_string(derived_table_indices.size()) +
	                " derived_pu table(s)");
	unordered_set<uint64_t> counter_bindings;
	unordered_set<uint64_t> finalized_bindings;
	WrapCounterRefsWithFinalize(input, plan.get(), derived_table_indices, counter_bindings, finalized_bindings);
	FixCTERefTypes(plan.get());
	plan->ResolveOperatorTypes();
	FixAllStaleRefs(plan.get());
}

} // namespace duckdb
