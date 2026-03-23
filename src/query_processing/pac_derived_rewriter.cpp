#include "query_processing/pac_derived_rewriter.hpp"

#include "aggregates/pac_aggregate.hpp"
#include "pac_debug.hpp"
#include "duckdb/common/printer.hpp"
#include "categorical/pac_categorical_detection.hpp"
#include "metadata/pac_metadata_manager.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
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
		PAC_DEBUG_PRINT("[PAC TRACE] ConvertAgg: checking aggregate '" + bound_agg.function.name +
		                "' IsPac=" + std::to_string(IsPacAggregate(bound_agg.function.name)));
		if (!IsPacAggregate(bound_agg.function.name)) {
			continue;
		}

		string counters_name = GetCountersVariant(bound_agg.function.name);
		PAC_DEBUG_PRINT("[PAC TRACE] ConvertAgg: counters_name='" + counters_name + "'");
		if (counters_name.empty()) {
			continue;
		}

		vector<unique_ptr<Expression>> children;
		for (auto &child_expr : bound_agg.children) {
			children.push_back(child_expr->Copy());
		}

		auto new_aggr = RebindAggregate(input.context, counters_name, std::move(children), bound_agg.IsDistinct());
		PAC_DEBUG_PRINT("[PAC TRACE] ConvertAgg: RebindAggregate returned " + std::to_string(new_aggr != nullptr));
		if (new_aggr) {
			PAC_DEBUG_PRINT("[PAC TRACE] ConvertAgg: rebound to '" +
			                new_aggr->Cast<BoundAggregateExpression>().function.name +
			                "' return_type=" + new_aggr->return_type.ToString());
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

// After counter conversion, the binder's inserted casts (e.g. BIGINT→FLOAT[]) may become
// redundant (FLOAT[]→FLOAT[]) because the aggregate now returns the counter list type directly.
// Strip these no-op casts to avoid "Unimplemented type for cast" errors at execution time.
static void StripRedundantCasts(LogicalOperator *op) {
	if (!op) {
		return;
	}
	for (auto &child : op->children) {
		StripRedundantCasts(child.get());
	}
	for (auto &expr : op->expressions) {
		std::function<void(unique_ptr<Expression> &)> Strip = [&](unique_ptr<Expression> &e) {
			ExpressionIterator::EnumerateChildren(*e, Strip);
			if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
				auto &cast = e->Cast<BoundCastExpression>();
				if (cast.child->return_type == cast.return_type) {
					e = std::move(cast.child);
				}
			}
		};
		Strip(expr);
	}
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
	// Identify counter columns by type — only FLOAT[] columns from derived_pu tables.
	// The counter_columns metadata is used at the propagation level to distinguish
	// user FLOAT[] columns from PAC counter columns (see WrapCounterRefsWithFinalize).
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	DerivedPuGetInfo info;
	info.table_index = get.table_index;
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		PAC_DEBUG_PRINT("[PAC FINALIZE] GET col[" + std::to_string(i) + "] type=" + get.returned_types[i].ToString() +
		                " table_idx=" + std::to_string(get.table_index));
		if (get.returned_types[i] == list_type) {
			info.counter_columns.insert(i);
		}
	}
	if (!info.counter_columns.empty()) {
		gets.push_back(std::move(info));
	}
}

// Wrap counter column refs with pac_finalize, bottom-up through the plan.
// Tracks counter column bindings as they flow through projections so that
// intermediate projections (added by DuckDB's optimizer) don't break matching.
static void WrapCounterRefsWithFinalize(OptimizerExtensionInput &input, LogicalOperator *op,
                                        const vector<DerivedPuGetInfo> &gets, unordered_set<uint64_t> &counter_bindings,
                                        unordered_set<uint64_t> &finalized_bindings, bool suppress = false) {
	if (!op) {
		return;
	}
	// Suppress wrapping inside AGGREGATE subtrees — aggregates (e.g. first() in scalar
	// subqueries) need raw FLOAT[] input. pac_finalize is applied above the aggregate.
	bool child_suppress = suppress || (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	for (auto &child : op->children) {
		WrapCounterRefsWithFinalize(input, child.get(), gets, counter_bindings, finalized_bindings, child_suppress);
	}
	bool skip_wrapping = suppress;

	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	auto finalized_type = PacFloatLogicalType();

	// At a GET node: seed counter_bindings using resolved output types + bindings
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		bool is_derived = false;
		for (auto &info : gets) {
			if (info.table_index == get.table_index) {
				is_derived = true;
				break;
			}
		}
		if (is_derived) {
			auto bindings = op->GetColumnBindings();
			auto &types = op->types;
			for (idx_t i = 0; i < bindings.size() && i < types.size(); i++) {
				PAC_DEBUG_PRINT("[PAC FINALIZE] GET binding (" + std::to_string(bindings[i].table_index) + "," +
				                std::to_string(bindings[i].column_index) + ") type=" + types[i].ToString());
				if (types[i] == list_type) {
					counter_bindings.insert(HashBinding(bindings[i]));
				}
			}
		}
		return;
	}

	// Propagate counter bindings through PROJECTION operators only.
	// Projections remap bindings (new table_index), so we need to track them.
	// Other operators (DISTINCT, ORDER BY, etc.) pass through child bindings
	// unchanged — FixExpr handles their expression types via finalized_bindings.
	// Propagate counter bindings through PROJECTION and AGGREGATE output.
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	    op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto out_bindings = op->GetColumnBindings();
		auto &out_types = op->types;
		for (idx_t i = 0; i < out_bindings.size() && i < out_types.size(); i++) {
			PAC_DEBUG_PRINT("[PAC FINALIZE] Propagate op_type=" + std::to_string((int)op->type) + " binding (" +
			                std::to_string(out_bindings[i].table_index) + "," +
			                std::to_string(out_bindings[i].column_index) + ") type=" + out_types[i].ToString());
			if (out_types[i] == list_type) {
				counter_bindings.insert(HashBinding(out_bindings[i]));
			}
		}
	}

	// Also skip wrapping for the AGGREGATE operator itself — its expressions
	// (e.g. first(#[8.0])) need raw FLOAT[] input.
	if (skip_wrapping || op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		PAC_DEBUG_PRINT("[PAC FINALIZE] skip_wrapping for op_type=" + std::to_string((int)op->type));
		return;
	}

	// First: update stale column refs that were finalized by a child operator
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

	// Wrap/fix expressions: any FLOAT[] column ref in a derived_pu plan gets pac_finalize.
	// We match by type (FLOAT[]) rather than tracking bindings through intermediate
	// projections, because DuckDB's optimizer reorders columns and breaks binding chains.
	std::function<void(unique_ptr<Expression> &)> WrapExpr = [&](unique_ptr<Expression> &e) {
		// Skip expressions already wrapped with pac_finalize (prevents double-wrapping
		// when EnumerateExpressions traverses into subquery plans)
		if (e->type == ExpressionType::BOUND_FUNCTION) {
			auto &func = e->Cast<BoundFunctionExpression>();
			if (func.function.name == "pac_finalize") {
				return;
			}
		}
		// If the binder inserted CAST(counter_col AS FLOAT) via our registered implicit cast,
		// the cast already performs pac_finalize — don't recurse into it.
		if (e->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
			auto &cast = e->Cast<BoundCastExpression>();
			if (cast.child->return_type == list_type && cast.return_type != list_type) {
				return;
			}
		}
		ExpressionIterator::EnumerateChildren(*e, WrapExpr);
		// Wrap any FLOAT[] column ref — in a plan with derived_pu GETs, these are counter columns
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			if (col_ref.return_type == list_type && !finalized_bindings.count(HashBinding(col_ref.binding))) {
				auto binding_hash = HashBinding(col_ref.binding);
				e = input.optimizer.BindScalarFunction("pac_finalize", std::move(e));
				finalized_bindings.insert(binding_hash);
			}
		}
		// Fix stale casts
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

	// After wrapping, immediately update this operator's output types so parent
	// operators see correct types during their propagation step.
	// For projections: derive from expressions. For others: sync from child types.
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			if (i < proj.types.size()) {
				proj.types[i] = proj.expressions[i]->return_type;
			}
			// Mark as finalized if the expression returns FLOAT or contains pac_finalize
			// (e.g. CASE WHEN ... ELSE pac_finalize(x) END has FLOAT[] return type but
			// is effectively finalized)
			bool is_finalized = (proj.expressions[i]->return_type == finalized_type);
			if (!is_finalized) {
				std::function<bool(const Expression &)> HasFinalize = [&](const Expression &e) -> bool {
					if (e.type == ExpressionType::BOUND_FUNCTION) {
						auto &func = e.Cast<BoundFunctionExpression>();
						if (func.function.name == "pac_finalize") {
							return true;
						}
					}
					bool found = false;
					ExpressionIterator::EnumerateChildren(const_cast<Expression &>(e), [&](Expression &child) {
						if (HasFinalize(child)) {
							found = true;
						}
					});
					return found;
				};
				is_finalized = HasFinalize(*proj.expressions[i]);
			}
			if (is_finalized) {
				finalized_bindings.insert(HashBinding(ColumnBinding(proj.table_index, i)));
				// Update the projection type and expression return_type
				if (i < proj.types.size() && proj.types[i] == list_type) {
					proj.types[i] = finalized_type;
				}
				if (proj.expressions[i]->return_type == list_type) {
					proj.expressions[i]->return_type = finalized_type;
				}
			}
		}
	} else {
		// For pass-through operators (DISTINCT, ORDER BY, etc.): if child types changed
		// from FLOAT[] to FLOAT, update our types to match.
		for (idx_t i = 0; i < op->types.size(); i++) {
			if (op->types[i] == list_type) {
				// Check if any child at this position now produces FLOAT
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

	auto finalized_type = PacFloatLogicalType();
	auto list_type = LogicalType::LIST(PacFloatLogicalType());

	// Fix column refs and subquery expressions
	std::function<void(unique_ptr<Expression> &)> Fix = [&](unique_ptr<Expression> &e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			auto it = binding_types.find(HashBinding(col_ref.binding));
			if (it != binding_types.end() && col_ref.return_type != it->second) {
				col_ref.return_type = it->second;
			}
		}
		// Fix subquery: resolve types in the subquery plan and update return_type
		if (e->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
			auto &subq = e->Cast<BoundSubqueryExpression>();
			if (subq.subquery.plan) {
				subq.subquery.plan->ResolveOperatorTypes();
				FixAllStaleRefs(subq.subquery.plan.get());
				// Update return_type and child_types/child_targets to match the plan output
				auto &plan_types = subq.subquery.plan->types;
				for (idx_t j = 0; j < plan_types.size(); j++) {
					if (plan_types[j] == finalized_type) {
						if (subq.return_type == list_type) {
							subq.return_type = finalized_type;
						}
						if (j < subq.child_types.size() && subq.child_types[j] == list_type) {
							subq.child_types[j] = finalized_type;
						}
						if (j < subq.child_targets.size() && subq.child_targets[j] == list_type) {
							subq.child_targets[j] = finalized_type;
						}
					}
				}
				// Also update the BoundStatement types
				for (idx_t j = 0; j < subq.subquery.types.size(); j++) {
					if (subq.subquery.types[j] == list_type && j < plan_types.size() &&
					    plan_types[j] == finalized_type) {
						subq.subquery.types[j] = finalized_type;
					}
				}
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

// After pac_finalize injection, CTE_REF operators still have stale chunk_types (FLOAT[]).
// Walk the plan and update CTE_REF chunk_types to match the CTE definition's output.
static void FixCTERefTypes(LogicalOperator *op, const LogicalType &list_type, const LogicalType &finalized_type) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		// child[0] = CTE definition, child[1] = consumer
		if (op->children.size() >= 2) {
			auto &def_types = op->children[0]->types;
			// Find all CTE_REF in the consumer subtree and update their chunk_types
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
	}
	for (auto &child : op->children) {
		FixCTERefTypes(child.get(), list_type, finalized_type);
	}
}

void InjectPacFinalizeForDerivedPu(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	vector<DerivedPuGetInfo> gets;
	FindDerivedPuGets(plan.get(), gets);
	if (gets.empty()) {
		return;
	}
	unordered_set<uint64_t> counter_bindings;
	unordered_set<uint64_t> finalized_bindings;
	WrapCounterRefsWithFinalize(input, plan.get(), gets, counter_bindings, finalized_bindings);
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	auto finalized_type = PacFloatLogicalType();
	FixCTERefTypes(plan.get(), list_type, finalized_type);
	plan->ResolveOperatorTypes();
	FixAllStaleRefs(plan.get());
}

} // namespace duckdb
