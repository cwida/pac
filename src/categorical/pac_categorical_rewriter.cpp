//
// PAC Categorical Query Rewriter - Implementation
//
// See pac_categorical_rewriter.hpp for design documentation.
//
// Created by ila on 1/23/26.
//
#include "categorical/pac_categorical_rewriter.hpp"
#include "aggregates/pac_aggregate.hpp"
#include "utils/pac_helpers.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

static string FindPacAggregateInExpression(Expression *expr, LogicalOperator *plan_root,
                                           ColumnBinding *out_source_binding = nullptr); // forward declaration

// Trace a column binding through the plan to find if it comes from a PAC aggregate
// Returns the PAC aggregate name if found (base name without _counters), empty string otherwise
static string TracePacAggregateFromBinding(const ColumnBinding &binding, LogicalOperator *plan_root,
                                           ColumnBinding *out_source_binding = nullptr) {
	auto *source_op = FindOperatorByTableIndex(plan_root, binding.table_index);
	if (!source_op) {
		return "";
	}
	if (source_op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto *unwrapped = RecognizeDuckDBScalarWrapper(source_op);
		if (unwrapped) { // Scalar subquery wrapper: trace through inner projection's expression
			auto &agg_child = source_op->Cast<LogicalProjection>().children[0]->Cast<LogicalAggregate>();
			auto &inner_proj = agg_child.children[0]->Cast<LogicalProjection>();
			if (!inner_proj.expressions.empty()) {
				return FindPacAggregateInExpression(inner_proj.expressions[0].get(), plan_root, out_source_binding);
			}
			return "";
		}
		auto &proj = source_op->Cast<LogicalProjection>();
		if (binding.column_index < proj.expressions.size()) {
			if (IsAlreadyWrappedInPacNoised(proj.expressions[binding.column_index].get())) {
				return "";
			}
			return FindPacAggregateInExpression(proj.expressions[binding.column_index].get(), plan_root,
			                                    out_source_binding);
		}
	} else if (source_op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggr = source_op->Cast<LogicalAggregate>();
		if (binding.table_index == aggr.aggregate_index) {
			if (binding.column_index < aggr.expressions.size()) {
				auto &agg_expr = aggr.expressions[binding.column_index];
				if (agg_expr->type == ExpressionType::BOUND_AGGREGATE) {
					auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
					if (IsAnyPacAggregate(bound_agg.function.name)) {
						if (out_source_binding) {
							*out_source_binding = binding;
						}
						return GetBasePacAggregateName(bound_agg.function.name);
					}
				}
			}
		}
	}
	return "";
}

// Helper to collect ALL distinct PAC aggregate bindings in an expression
// Returns the bindings in the order they were discovered
static void CollectPacBindingsInExpression(Expression *expr, LogicalOperator *root, vector<PacBindingInfo> &bindings,
                                           unordered_map<uint64_t, idx_t> &binding_hash_to_index) {
	// Check if this is a column reference that traces back to a PAC aggregate
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		ColumnBinding source_binding;
		string pac_name = TracePacAggregateFromBinding(col_ref.binding, root, &source_binding);
		if (!pac_name.empty()) {
			uint64_t binding_hash = HashBinding(col_ref.binding);
			if (binding_hash_to_index.find(binding_hash) == binding_hash_to_index.end()) {
				PacBindingInfo info;
				info.binding = col_ref.binding;
				info.aggregate_name = pac_name;
				info.original_type = col_ref.return_type;
				info.index = bindings.size();
				info.source_binding = source_binding;
				binding_hash_to_index[binding_hash] = info.index;
				bindings.push_back(info);
			}
		}
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](Expression &child) {
		CollectPacBindingsInExpression(&child, root, bindings, binding_hash_to_index);
	});
}

// Find all PAC aggregate bindings in an expression
static vector<PacBindingInfo> FindAllPacBindingsInExpression(Expression *expr, LogicalOperator *plan_root) {
	vector<PacBindingInfo> bindings;
	unordered_map<uint64_t, idx_t> binding_hash_to_index;
	CollectPacBindingsInExpression(expr, plan_root, bindings, binding_hash_to_index);
	return bindings;
}

// Find counter bindings in an expression using the known counter_bindings set (no plan tracing).
// Used during the rewrite phase where counter_bindings tracks all bindings rewritten to counters.
static vector<PacBindingInfo> FindCounterBindings(Expression *expr,
                                                  const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	vector<PacBindingInfo> result;
	unordered_map<uint64_t, idx_t> seen;
	std::function<void(Expression *)> Collect = [&](Expression *e) {
		if (e->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = e->Cast<BoundColumnRefExpression>();
			uint64_t h = HashBinding(col_ref.binding);
			if (counter_bindings.count(h) && !seen.count(h)) {
				PacBindingInfo info;
				info.binding = col_ref.binding;
				info.index = result.size();
				seen[h] = info.index;
				result.push_back(info);
			}
		}
		ExpressionIterator::EnumerateChildren(*e, [&](Expression &child) { Collect(&child); });
	};
	Collect(expr);
	return result;
}

// Register a binding as a counter binding, optionally tracing to its origin.
static void AddCounterBinding(unordered_map<uint64_t, ColumnBinding> &counter_bindings, const ColumnBinding &binding,
                              const ColumnBinding &origin = ColumnBinding()) {
	counter_bindings[HashBinding(binding)] = (origin.table_index != DConstants::INVALID_INDEX) ? origin : binding;
}

// For a projection expression, trace back through counter_bindings to find the origin binding.
// If the expression is a simple col_ref to a known counter, returns its tracked origin; otherwise returns the binding
// itself.
static ColumnBinding TraceCounterOrigin(const unique_ptr<Expression> &expr,
                                        const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		auto it = counter_bindings.find(HashBinding(col_ref.binding));
		if (it != counter_bindings.end()) {
			return it->second;
		}
	}
	return ColumnBinding(); // invalid — AddCounterBinding will use the binding itself
}

// Check if an expression references any counter binding.
static bool ReferencesCounterBinding(Expression *expr, const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		return counter_bindings.count(HashBinding(expr->Cast<BoundColumnRefExpression>().binding)) > 0;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(*expr, [&](Expression &child) {
		if (!found) {
			found = ReferencesCounterBinding(&child, counter_bindings);
		}
	});
	return found;
}

// Recursively search for PAC aggregate in an expression tree, with plan context for tracing column refs
// Returns the base aggregate name (without _counters suffix)
static string FindPacAggregateInExpression(Expression *expr, LogicalOperator *plan_root,
                                           ColumnBinding *out_source_binding) {
	// examine specific expression types where the PAC aggregate could be in
	if (expr->type == ExpressionType::BOUND_AGGREGATE) { // Base case: direct PAC aggregate
		auto &agg_expr = expr->Cast<BoundAggregateExpression>();
		if (IsAnyPacAggregate(agg_expr.function.name)) {
			return GetBasePacAggregateName(agg_expr.function.name);
		}
	} else if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		return TracePacAggregateFromBinding(col_ref.binding, plan_root, out_source_binding);
	} else if (expr->type == ExpressionType::SUBQUERY) {
		auto &subquery_expr = expr->Cast<BoundSubqueryExpression>();
		for (auto &child : subquery_expr.children) {
			string result = FindPacAggregateInExpression(child.get(), plan_root);
			if (!result.empty()) {
				return result; // Found PAC aggregate in the subquery's children (for IN, ANY, ALL operators)
			}
		}
		if (subquery_expr.subquery_type == SubqueryType::SCALAR) {
			string result = FindPacAggregateInOperator(plan_root);
			if (!result.empty()) {
				return result; // found PAC aggregate in scalar subquery
			}
		}
		return "";
	}
	// Generic traversal for all other expression types (comparisons, operators, casts,
	// functions, constants, CASE, BETWEEN, conjunctions, window functions, etc.)
	string result;
	ExpressionIterator::EnumerateChildren(*expr, [&](Expression &child) {
		if (result.empty()) {
			result = FindPacAggregateInExpression(&child, plan_root, out_source_binding);
		}
	});
	return result;
}

// Check if a binding in a FILTER traces to the filter's own child aggregate (HAVING clause).
// HAVING predicates should use scalar pac_noised comparison, not categorical pac_filter,
// because pac_filter's majority-vote decision can disagree with the pac_noised aggregate
// values that appear alongside it in the output.
static bool IsHavingBinding(LogicalOperator *filter_op, const ColumnBinding &binding) {
	if (!filter_op || filter_op->children.empty()) {
		return false;
	}
	// Walk through projections to find the child aggregate
	LogicalOperator *child = filter_op->children[0].get();
	while (child && child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (child->children.empty()) {
			break;
		}
		child = child->children[0].get();
	}
	if (child && child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = child->Cast<LogicalAggregate>();
		return binding.table_index == agg.aggregate_index || binding.table_index == agg.group_index;
	}
	return false;
}

// Find scalar wrapper for a binding (if any)
// Returns the outer Projection of the wrapper pattern, or nullptr

// Recursively search the plan for categorical patterns (plan-aware version)
// Now detects ANY filter expression containing a PAC aggregate, not just comparisons
// outer_pac_hash: if an outer PAC aggregate was found above, this is the hash binding resolved to the current level
void FindCategoricalPatternsInOperator(LogicalOperator *op, LogicalOperator *plan_root,
                                       vector<CategoricalPatternInfo> &patterns, bool inside_aggregate,
                                       ColumnBinding outer_pac_hash = ColumnBinding()) {
	// Track if we're entering an aggregate
	bool now_inside_aggregate = inside_aggregate || (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);

	// Track outer PAC hash for children: updated when we find a PAC aggregate or pass through a projection
	ColumnBinding hash_for_children = outer_pac_hash;

	// When entering a PAC aggregate, extract the hash binding (first child of first PAC aggregate)
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op->Cast<LogicalAggregate>();
		for (auto &agg_expr : agg.expressions) {
			if (agg_expr->type == ExpressionType::BOUND_AGGREGATE) {
				auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
				if (IsPacAggregate(bound_agg.function.name) && !bound_agg.children.empty() &&
				    bound_agg.children[0]->type == ExpressionType::BOUND_COLUMN_REF) {
					hash_for_children = bound_agg.children[0]->Cast<BoundColumnRefExpression>().binding;
					break;
				}
			}
		}
	}

	// Resolve hash binding through projections (follow col_ref chains)
	if (hash_for_children.table_index != DConstants::INVALID_INDEX &&
	    op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		if (hash_for_children.table_index == proj.table_index &&
		    hash_for_children.column_index < proj.expressions.size()) {
			auto &expr = proj.expressions[hash_for_children.column_index];
			if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
				hash_for_children = expr->Cast<BoundColumnRefExpression>().binding;
			}
		}
	}

	// Track patterns count before checking this operator AND its children.
	// Used at the end to strip scalar wrappers when any new patterns were found.
	size_t patterns_before_all = patterns.size();

	// Check filter expressions - detect ANY boolean expression containing a PAC aggregate
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		for (idx_t i = 0; i < filter.expressions.size(); i++) {
			auto &filter_expr = filter.expressions[i];
			// Find ALL PAC aggregate bindings in this expression (not just single)
			auto pac_bindings = FindAllPacBindingsInExpression(filter_expr.get(), plan_root);
			if (pac_bindings.empty()) {
				continue;
			}
			// Skip HAVING predicates: if ALL bindings trace to the filter's own child
			// aggregate, this is a HAVING clause. HAVING should use scalar pac_noised
			// comparison, not categorical pac_filter, because pac_filter's majority-vote
			// can disagree with pac_noised values shown in the same row.
			bool all_having = true;
			for (auto &bi : pac_bindings) {
				if (!IsHavingBinding(op, bi.binding)) {
					all_having = false;
					break;
				}
			}
			if (all_having) {
				continue;
			}
			{
				CategoricalPatternInfo info;
				info.parent_op = op;
				info.expr_index = i;
				info.pac_binding = pac_bindings[0].binding;
				info.has_pac_binding = true;
				info.aggregate_name = pac_bindings[0].aggregate_name;
				info.source_binding = pac_bindings[0].source_binding;
				info.original_return_type = pac_bindings[0].original_type;
				info.pac_bindings = std::move(pac_bindings);
				info.outer_pac_hash = hash_for_children;
				info.has_outer_pac_hash = (hash_for_children.table_index != DConstants::INVALID_INDEX);
				patterns.push_back(info);
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	           op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		for (idx_t i = 0; i < join.conditions.size(); i++) {
			auto &cond = join.conditions[i];
			// Check both sides; any PAC aggregate in a join condition is categorical (not HAVING).
			for (auto *side : {cond.left.get(), cond.right.get()}) {
				string pac_name = FindPacAggregateInExpression(side, plan_root);
				if (pac_name.empty()) {
					continue;
				}
				CategoricalPatternInfo info;
				info.parent_op = op;
				info.expr_index = i;
				info.aggregate_name = pac_name;
				info.outer_pac_hash = hash_for_children;
				info.has_outer_pac_hash = (hash_for_children.table_index != DConstants::INVALID_INDEX);
				if (side->type == ExpressionType::BOUND_COLUMN_REF) {
					TracePacAggregateFromBinding(side->Cast<BoundColumnRefExpression>().binding, plan_root,
					                             &info.source_binding);
				}
				patterns.push_back(info);
				break; // one match per condition is enough
			}
		}
	} else if (!inside_aggregate && patterns.empty() && op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		// Check projection expressions for arithmetic involving multiple PAC aggregates
		// This handles cases like Q08: sum(CASE...)/sum(volume) in SELECT list
		// NOTE: Only check projections if we haven't already found filter/join patterns,
		// because those patterns will handle the projections via RewriteProjectionsWithCounters.
		// We only want standalone projection patterns (no filter/join categorical patterns).
		auto &proj = op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			auto &expr = proj.expressions[i];
			if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
				continue; // Skip simple column references - they don't need rewriting
			}
			// Find ALL PAC aggregate bindings in this expression
			auto pac_bindings = FindAllPacBindingsInExpression(expr.get(), plan_root);
			if (pac_bindings.empty()) {
				continue;
			}
			// Check if this expression has arithmetic with PAC aggregates
			// - Multiple PAC bindings (e.g., pac_sum(...) / pac_sum(...))
			// - Or single PAC binding with arithmetic (e.g., pac_sum(...) * 0.5)
			bool is_arithmetic_with_pac = false;
			if (pac_bindings.size() >= 2) { // Multiple PAC aggregates - definitely needs lambda rewrite
				is_arithmetic_with_pac = true;
			} else if (pac_bindings.size() == 1) {
				// Single aggregate - check if it's in an arithmetic expression (not just a column ref or simple cast)
				if (expr->type != ExpressionType::BOUND_COLUMN_REF && expr->type != ExpressionType::OPERATOR_CAST) {
					is_arithmetic_with_pac = true;
				} else if (expr->type == ExpressionType::OPERATOR_CAST) {
					// Check if cast contains arithmetic
					auto &cast_expr = expr->Cast<BoundCastExpression>();
					if (cast_expr.child->type != ExpressionType::BOUND_COLUMN_REF) {
						is_arithmetic_with_pac = true;
					}
				}
			}
			if (is_arithmetic_with_pac) {
				if (!IsNumericalType(expr->return_type)) {
					continue; // Only create pattern if result is numerical (pac_noised only works on numbers)
				}
				CategoricalPatternInfo info;
				info.parent_op = op;
				info.expr_index = i;
				info.pac_binding = pac_bindings[0].binding;
				info.has_pac_binding = true;
				info.aggregate_name = pac_bindings[0].aggregate_name;
				info.original_return_type = expr->return_type;
				info.source_binding = pac_bindings[0].source_binding;
				info.pac_bindings = std::move(pac_bindings);
				patterns.push_back(std::move(info));
			}
		}
	}
	for (auto &child : op->children) {
		FindCategoricalPatternsInOperator(child.get(), plan_root, patterns, now_inside_aggregate, hash_for_children);
	}
	// On the way back up: if patterns were found in this subtree, strip scalar wrappers in direct children.
	// Record the resulting table_index so RewriteBottomUp knows these projections are terminal.
	if (patterns.size() > patterns_before_all) {
		for (auto &child : op->children) {
			if (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto *unwrapped = RecognizeDuckDBScalarWrapper(child.get());
				if (unwrapped) {
					StripScalarWrapperInPlace(child, true);
				}
			}
		}
	}
}

// Check if an expression traces back to a PAC _counters aggregate

// with pac_*_list variants that aggregate element-wise
// Try to rebind an aggregate expression at index i to its _list variant.
// Returns true if the aggregate was successfully rebound.
// The aggregate's value child is extracted (dropping the hash for PAC aggregates),
// its type set to LIST<FLOAT>, and the aggregate rebound to the _list variant.
static bool TryRebindToListVariant(LogicalAggregate &agg, idx_t i, ClientContext &context) {
	auto &agg_expr = agg.expressions[i];
	if (agg_expr->type != ExpressionType::BOUND_AGGREGATE) {
		return false;
	}
	auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
	if (bound_agg.children.empty()) {
		return false;
	}
	// PAC/PAC_counters aggregates have (hash, value); standard aggregates have (value).
	// For _list variants we only need the value child.
	bool is_pac_style = (IsPacAggregate(bound_agg.function.name) || IsPacCountersAggregate(bound_agg.function.name)) &&
	                    bound_agg.children.size() > 1;
	idx_t value_child_idx = is_pac_style ? 1 : 0;
	string base_name = GetBasePacAggregateName(bound_agg.function.name); // strips _counters if present
	string list_name = GetListAggregateVariant(base_name);
	if (list_name.empty()) {
		return false;
	}
	vector<unique_ptr<Expression>> children;
	auto child_copy = bound_agg.children[value_child_idx]->Copy();
	children.push_back(std::move(child_copy));
	auto new_aggr = RebindAggregate(context, list_name, std::move(children), bound_agg.IsDistinct());
	if (!new_aggr) {
		return false;
	}
	agg.expressions[i] = std::move(new_aggr);
	idx_t types_index = agg.groups.size() + i;
	if (types_index < agg.types.size()) {
		agg.types[types_index] = LogicalType::LIST(PacFloatLogicalType());
	}
	return true;
}

// Replace aggregates whose input references a counter binding with _list variants.
// Called during RewriteBottomUp so projections above get properly list_transform'd.
// Adds output bindings of converted aggregates to counter_bindings.
static void ReplaceAggregatesOverCounters(LogicalOperator *op, ClientContext &context,
                                          unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &agg = op->Cast<LogicalAggregate>();
	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		auto &agg_expr = agg.expressions[i];
		if (agg_expr->type != ExpressionType::BOUND_AGGREGATE) {
			continue;
		}
		auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
		if (bound_agg.children.empty()) {
			continue;
		}
		idx_t value_child_idx = IsPacAggregate(bound_agg.function.name) && bound_agg.children.size() > 1 ? 1 : 0;
		if (!ReferencesCounterBinding(bound_agg.children[value_child_idx].get(), counter_bindings)) {
			continue;
		}
		if (TryRebindToListVariant(agg, i, context)) {
			AddCounterBinding(counter_bindings, ColumnBinding(agg.aggregate_index, i));
		}
	}
}

// Clone an expression tree for use as a lambda body (unified single/multi binding version).
// PAC aggregate column refs are replaced with lambda element references.
// Other column refs are captured and become BoundReferenceExpression(1+i).
//
// pac_binding_map: maps binding hash -> struct field index (single: one entry mapping to 0)
// struct_type: nullptr = single binding (elem ref), non-null = multi binding (struct field extract)
static unique_ptr<Expression>
CloneForLambdaBody(Expression *expr, const unordered_map<uint64_t, idx_t> &pac_binding_map,
                   vector<unique_ptr<Expression>> &captures, unordered_map<uint64_t, idx_t> &capture_map,
                   const unordered_map<uint64_t, ColumnBinding> &counter_bindings, const LogicalType *struct_type) {
	// Helper to build the replacement expression for a matched PAC binding
	auto make_pac_replacement = [&](const unordered_map<uint64_t, idx_t>::const_iterator &it,
	                                const string &alias) -> unique_ptr<Expression> {
		if (struct_type) {
			idx_t field_idx = it->second;
			string field_name = GetStructFieldName(field_idx);
			auto elem_ref = make_uniq<BoundReferenceExpression>("elem", *struct_type, idx_t(0));
			auto child_types = StructType::GetChildTypes(*struct_type);
			LogicalType extract_return_type = PacFloatLogicalType();
			for (idx_t j = 0; j < child_types.size(); j++) {
				if (child_types[j].first == field_name) {
					extract_return_type = child_types[j].second;
					break;
				}
			}
			auto extract_func = StructExtractAtFun::GetFunction();
			auto bind_data = StructExtractAtFun::GetBindData(field_idx);
			vector<unique_ptr<Expression>> extract_children;
			extract_children.push_back(std::move(elem_ref));
			extract_children.push_back(
			    make_uniq<BoundConstantExpression>(Value::BIGINT(static_cast<int64_t>(field_idx + 1))));
			return make_uniq<BoundFunctionExpression>(extract_return_type, extract_func, std::move(extract_children),
			                                          std::move(bind_data));
		} else {
			return make_uniq<BoundReferenceExpression>(alias, PacFloatLogicalType(), idx_t(0));
		}
	};
	// rewrite all kinds of expressions:
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		uint64_t binding_hash = HashBinding(col_ref.binding);
		// Direct match
		auto it = pac_binding_map.find(binding_hash);
		if (it != pac_binding_map.end()) {
			return make_pac_replacement(it, "elem");
		}
		// Trace through counter_bindings to find the aggregate-level PAC binding
		auto cb_it = counter_bindings.find(HashBinding(col_ref.binding));
		if (cb_it != counter_bindings.end()) {
			auto traced_it = pac_binding_map.find(HashBinding(cb_it->second));
			if (traced_it != pac_binding_map.end()) {
				return make_pac_replacement(traced_it, col_ref.alias);
			}
		}
		// Other column ref — capture for use in lambda
		uint64_t hash = HashBinding(col_ref.binding);
		idx_t capture_idx;
		auto cap_it = capture_map.find(hash);
		if (cap_it != capture_map.end()) {
			capture_idx = cap_it->second;
		} else {
			capture_idx = captures.size();
			capture_map[hash] = capture_idx;
			captures.push_back(col_ref.Copy());
		}
		return make_uniq<BoundReferenceExpression>(col_ref.alias, col_ref.return_type, 1 + capture_idx);
	} else if (expr->type == ExpressionType::VALUE_CONSTANT) {
		return expr->Copy();
	} else if (expr->type == ExpressionType::OPERATOR_CAST) {
		auto &cast = expr->Cast<BoundCastExpression>();
		auto child_clone =
		    CloneForLambdaBody(cast.child.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type);
		if (child_clone->return_type == cast.return_type) {
			return child_clone; // If the child's type already matches the target type, skip the cast
		}
		// Otherwise, create a new cast with the correct function for the new child type
		return BoundCastExpression::AddDefaultCastToType(std::move(child_clone), cast.return_type);
	} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comp = expr->Cast<BoundComparisonExpression>();
		auto left_clone =
		    CloneForLambdaBody(comp.left.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type);
		auto right_clone =
		    CloneForLambdaBody(comp.right.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type);
		// Reconcile types if they differ (needed for multi where struct fields are DOUBLE
		// but original CASTs may introduce DECIMAL)
		if (left_clone->return_type != right_clone->return_type) {
			if (left_clone->return_type != PacFloatLogicalType()) {
				left_clone = BoundCastExpression::AddDefaultCastToType(std::move(left_clone), PacFloatLogicalType());
			}
			if (right_clone->return_type != PacFloatLogicalType()) {
				right_clone = BoundCastExpression::AddDefaultCastToType(std::move(right_clone), PacFloatLogicalType());
			}
		}
		return make_uniq<BoundComparisonExpression>(expr->type, std::move(left_clone), std::move(right_clone));
	} else if (expr->type == ExpressionType::BOUND_FUNCTION) {
		auto &func = expr->Cast<BoundFunctionExpression>();
		vector<unique_ptr<Expression>> new_children;
		bool any_cast_needed = false;
		for (idx_t i = 0; i < func.children.size(); i++) {
			auto child_clone = CloneForLambdaBody(func.children[i].get(), pac_binding_map, captures, capture_map,
			                                      counter_bindings, struct_type);
			// If a child's type changed (e.g., DECIMAL->DOUBLE from PAC counter conversion),
			// cast it to the type the bound function expects, so the function binding stays valid.
			if (i < func.function.arguments.size() && child_clone->return_type != func.function.arguments[i]) {
				child_clone =
				    BoundCastExpression::AddDefaultCastToType(std::move(child_clone), func.function.arguments[i]);
				any_cast_needed = true;
			}
			new_children.push_back(std::move(child_clone));
		}
		unique_ptr<Expression> result =
		    make_uniq<BoundFunctionExpression>(func.return_type, func.function, std::move(new_children),
		                                       func.bind_info ? func.bind_info->Copy() : nullptr);
		// Children were cast from PAC_FLOAT to the function's bound types (e.g., DECIMAL).
		// Cast the result back to PAC_FLOAT so the list_transform output stays LIST<PAC_FLOAT>.
		if (any_cast_needed && result->return_type != PacFloatLogicalType()) {
			result = BoundCastExpression::AddDefaultCastToType(std::move(result), PacFloatLogicalType());
		}
		return result;
	} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) { // NOT, arithmetic, COALESCE, IS NULL..
		auto &op = expr->Cast<BoundOperatorExpression>();
		vector<unique_ptr<Expression>> new_children;
		for (auto &child : op.children) {
			new_children.push_back(
			    CloneForLambdaBody(child.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type));
		}
		// For COALESCE with mismatched child types, cast all to the first child's type
		LogicalType result_type = op.return_type;
		if (expr->type == ExpressionType::OPERATOR_COALESCE && new_children.size() > 1) {
			LogicalType first_type = new_children[0]->return_type;
			bool types_mismatch = false;
			for (idx_t ci = 1; ci < new_children.size(); ci++) {
				if (new_children[ci]->return_type != first_type) {
					types_mismatch = true;
					break;
				}
			}
			if (types_mismatch) {
				for (auto &c : new_children) {
					if (c->return_type != first_type) {
						c = BoundCastExpression::AddDefaultCastToType(std::move(c), first_type);
					}
				}
				result_type = first_type;
			}
		}
		auto op_result = make_uniq<BoundOperatorExpression>(expr->type, result_type);
		for (auto &c : new_children) {
			op_result->children.push_back(std::move(c));
		}
		return op_result;
	} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) { // AND, OR
		auto &conj = expr->Cast<BoundConjunctionExpression>();
		auto result = make_uniq<BoundConjunctionExpression>(expr->type);
		for (auto &child : conj.children) {
			result->children.push_back(
			    CloneForLambdaBody(child.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type));
		}
		return result;
	} else if (expr->type == ExpressionType::CASE_EXPR) {
		auto &case_expr = expr->Cast<BoundCaseExpression>();
		if (IsScalarSubqueryWrapper(case_expr)) {
			return CloneForLambdaBody(case_expr.else_expr.get(), pac_binding_map, captures, capture_map,
			                          counter_bindings, struct_type);
		}
		// Regular CASE - recurse into all branches
		// Start with original return type, will update based on cloned branches
		auto result = make_uniq<BoundCaseExpression>(case_expr.return_type);
		for (auto &check : case_expr.case_checks) {
			BoundCaseCheck new_check;
			new_check.when_expr = CloneForLambdaBody(check.when_expr.get(), pac_binding_map, captures, capture_map,
			                                         counter_bindings, struct_type);
			new_check.then_expr = CloneForLambdaBody(check.then_expr.get(), pac_binding_map, captures, capture_map,
			                                         counter_bindings, struct_type);
			result->case_checks.push_back(std::move(new_check));
		}
		if (case_expr.else_expr) {
			result->else_expr = CloneForLambdaBody(case_expr.else_expr.get(), pac_binding_map, captures, capture_map,
			                                       counter_bindings, struct_type);
			// Update return type to match ELSE branch (the PAC element type)
			result->return_type = result->else_expr->return_type;
		}
		// Cast THEN branches to match the return type if needed
		for (auto &check : result->case_checks) {
			if (check.then_expr && check.then_expr->return_type != result->return_type) {
				check.then_expr =
				    BoundCastExpression::AddDefaultCastToType(std::move(check.then_expr), result->return_type);
			}
		}
		return result;
	}
	return expr->Copy();
}

// Build a BoundLambdaExpression from a lambda body and captures
static unique_ptr<Expression> BuildPacLambda(unique_ptr<Expression> lambda_body,
                                             vector<unique_ptr<Expression>> captures) {
	auto lambda =
	    make_uniq<BoundLambdaExpression>(ExpressionType::LAMBDA, LogicalType::LAMBDA, std::move(lambda_body), idx_t(1));
	lambda->captures = std::move(captures);
	return lambda;
}

// Build a list_transform function call with proper binding
// element_return_type: the type each element maps to (e.g., BOOLEAN for predicates, some other type for casts)
static unique_ptr<Expression> BuildListTransformCall(OptimizerExtensionInput &input,
                                                     unique_ptr<Expression> counters_list,
                                                     unique_ptr<Expression> lambda_expr,
                                                     const LogicalType &element_return_type = LogicalType::BOOLEAN) {
	// Get the lambda body for ListLambdaBindData
	auto &bound_lambda = lambda_expr->Cast<BoundLambdaExpression>();

	// Get list_transform function from catalog
	auto &catalog = Catalog::GetSystemCatalog(input.context);
	auto &func_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(input.context, DEFAULT_SCHEMA, "list_transform");

	// Get the first function overload (list_transform has only one signature pattern)
	auto &scalar_func = func_entry.functions.functions[0];

	// Create the ListLambdaBindData with the lambda body
	auto list_return_type = LogicalType::LIST(element_return_type);
	auto bind_data = make_uniq<ListLambdaBindData>(list_return_type, std::move(bound_lambda.lambda_expr), false, false);

	// Build children: [list, captures...] Note: The lambda itself is NOT a child after binding - only its captures are
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(counters_list));

	for (auto &capture : bound_lambda.captures) {
		children.push_back(std::move(capture)); // Add captures as children
	}
	// Create the bound function expression
	return make_uniq<BoundFunctionExpression>(list_return_type, scalar_func, std::move(children), std::move(bind_data));
}

// Build a list_zip function call combining multiple counter lists
// Returns LIST<STRUCT<a T1, b T2, ...>> where each field corresponds to one PAC binding
static unique_ptr<Expression> BuildListZipCall(OptimizerExtensionInput &input,
                                               vector<unique_ptr<Expression>> counter_lists,
                                               LogicalType &out_struct_type) {
	// Build the struct type for list_zip result -- list_zip returns LIST<STRUCT<a T1, b T2, ...>>
	child_list_t<LogicalType> struct_children;
	for (idx_t i = 0; i < counter_lists.size(); i++) {
		string field_name = GetStructFieldName(i);
		struct_children.push_back(
		    make_pair(field_name, PacFloatLogicalType())); // All counter lists use PAC_FLOAT element type
	}
	out_struct_type = LogicalType::STRUCT(struct_children);
	auto list_struct_type = LogicalType::LIST(out_struct_type);

	// Get list_zip function from catalog
	auto &catalog = Catalog::GetSystemCatalog(input.context);
	auto &func_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(input.context, DEFAULT_SCHEMA, "list_zip");

	// Find the appropriate overload (list_zip is variadic)
	vector<LogicalType> arg_types;
	for (auto &list : counter_lists) {
		arg_types.push_back(list->return_type);
	}
	ErrorData error;
	FunctionBinder function_binder(input.context);
	auto best_function = function_binder.BindFunction(func_entry.name, func_entry.functions, arg_types, error);
	if (best_function.IsValid()) {
		auto scalar_func = func_entry.functions.GetFunctionByOffset(best_function.GetIndex());
		return make_uniq<BoundFunctionExpression>(list_struct_type, scalar_func, std::move(counter_lists), nullptr);
	}
	return nullptr;
}

// Minimal processing for filter-pattern projection slots:
// update counter col_ref types but don't wrap with any terminal.
// The parent filter/join will expand these expressions and generate list_transform + pac_filter.
static void PrepareFilterPatternSlot(LogicalProjection &proj, idx_t i,
                                     const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	auto &expr = proj.expressions[i];
	auto pac_bindings = FindCounterBindings(expr.get(), counter_bindings);
	if (pac_bindings.empty()) {
		return;
	}
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	// col_ref types are already fixed by the universal type fix above.
	// Update projection output type
	if (i < proj.types.size()) {
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			proj.types[i] = list_type;
		} else if (expr->type == ExpressionType::BOUND_FUNCTION) {
			proj.types[i] = expr->return_type;
		}
	}
}

// Build a list_transform expression over PAC counter bindings.
// Check whether an expression tree contains any null-handling operators
// (COALESCE, IS NULL, IS NOT NULL). When absent, pac_coalesce is unnecessary
// because NULL propagates identically through arithmetic.
static bool ExpressionContainsNullHandling(Expression *expr) {
	if (expr->type == ExpressionType::OPERATOR_COALESCE || expr->type == ExpressionType::OPERATOR_IS_NULL ||
	    expr->type == ExpressionType::OPERATOR_IS_NOT_NULL) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(*expr, [&](Expression &child) {
		if (!found) {
			found = ExpressionContainsNullHandling(&child);
		}
	});
	return found;
}

// For single binding: list_transform(counters, elem -> body(elem))
// For multiple bindings: list_transform(list_zip(c1, c2, ...), elem -> body(elem.a, elem.b, ...))
//   CloneForLambdaBody (multi mode) handles per-field type casting internally.
//
// Returns the list_transform expression, or nullptr on failure.
// The caller wraps with pac_noised or pac_filter as needed.
static unique_ptr<Expression> BuildCounterListTransform(OptimizerExtensionInput &input,
                                                        const vector<PacBindingInfo> &pac_bindings,
                                                        Expression *expr_to_transform,
                                                        const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                                                        const LogicalType &result_element_type) {
	if (pac_bindings.size() == 1) { // --- SINGLE AGGREGATE ---
		auto &binding_info = pac_bindings[0];
		ColumnBinding pac_binding = binding_info.binding;
		auto counters_ref =
		    make_uniq<BoundColumnRefExpression>("pac_var", LogicalType::LIST(PacFloatLogicalType()), pac_binding);
		unique_ptr<Expression> input_list;
		if (ExpressionContainsNullHandling(expr_to_transform)) {
			input_list = input.optimizer.BindScalarFunction("pac_coalesce", std::move(counters_ref));
		} else {
			input_list = std::move(counters_ref);
		}
		// Outer lambda: clone the expression replacing PAC binding with lambda element
		unordered_map<uint64_t, idx_t> pac_binding_map;
		pac_binding_map[HashBinding(pac_binding)] = 0;
		vector<unique_ptr<Expression>> captures;
		unordered_map<uint64_t, idx_t> capture_map;
		auto lambda_body =
		    CloneForLambdaBody(expr_to_transform, pac_binding_map, captures, capture_map, counter_bindings, nullptr);
		// Ensure body returns result_element_type if needed
		if (result_element_type == PacFloatLogicalType() && lambda_body->return_type != PacFloatLogicalType()) {
			lambda_body = BoundCastExpression::AddDefaultCastToType(std::move(lambda_body), PacFloatLogicalType());
		}
		auto lambda = BuildPacLambda(std::move(lambda_body), std::move(captures));
		return BuildListTransformCall(input, std::move(input_list), std::move(lambda), result_element_type);
	} else { // --- MULTIPLE AGGREGATES ---
		vector<unique_ptr<Expression>> counter_lists;
		unordered_map<uint64_t, idx_t> binding_to_index;
		bool needs_coalesce = ExpressionContainsNullHandling(expr_to_transform);
		for (auto &bi : pac_bindings) {
			auto ref =
			    make_uniq<BoundColumnRefExpression>("pac_var", LogicalType::LIST(PacFloatLogicalType()), bi.binding);
			if (needs_coalesce) {
				counter_lists.push_back(input.optimizer.BindScalarFunction("pac_coalesce", std::move(ref)));
			} else {
				counter_lists.push_back(std::move(ref));
			}
			binding_to_index[HashBinding(bi.binding)] = bi.index;
		}
		LogicalType struct_type;
		auto zipped_list = BuildListZipCall(input, std::move(counter_lists), struct_type);
		if (zipped_list) {
			vector<unique_ptr<Expression>> captures;
			unordered_map<uint64_t, idx_t> map;
			auto lambda_body =
			    CloneForLambdaBody(expr_to_transform, binding_to_index, captures, map, counter_bindings, &struct_type);
			if (result_element_type == PacFloatLogicalType() && lambda_body->return_type != PacFloatLogicalType()) {
				lambda_body = BoundCastExpression::AddDefaultCastToType(std::move(lambda_body), PacFloatLogicalType());
			}
			auto lambda = BuildPacLambda(std::move(lambda_body), std::move(captures));
			return BuildListTransformCall(input, std::move(zipped_list), std::move(lambda), result_element_type);
		}
	}
	return nullptr;
}

// Check which child of a binary function contains a counter binding
// Returns 0 if first child has counter, 1 if second child has counter, -1 if neither or both
static int FindPacChildIndex(Expression *expr, const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (expr->type != ExpressionType::BOUND_FUNCTION) {
		return -1;
	}
	auto &func = expr->Cast<BoundFunctionExpression>();
	if (func.children.size() != 2) {
		return -1;
	}
	bool left_has = ReferencesCounterBinding(func.children[0].get(), counter_bindings);
	bool right_has = ReferencesCounterBinding(func.children[1].get(), counter_bindings);
	if (left_has && !right_has) {
		return 0;
	}
	if (!left_has && right_has) {
		return 1;
	}
	return -1; // both or neither
}

/// Try to rewrite a filter comparison into pac_filter_<cmp>(scalar, counters) or
/// pac_select_<cmp>(hash, scalar, counters). Returns the (possibly simplified) expression.
/// If successful, the result is a pac_{filter,select}_<cmp> call; otherwise a comparison
/// with algebraically simplified operands for the lambda path. Returns nullptr if the
/// expression is not a comparison or doesn't have exactly one PAC binding.
static unique_ptr<Expression> TryRewriteFilterComparison(OptimizerExtensionInput &input,
                                                         const vector<PacBindingInfo> &pac_bindings, Expression *expr,
                                                         const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                                                         PacWrapKind wrap_kind = PacWrapKind::PAC_FILTER,
                                                         ColumnBinding pac_hash = ColumnBinding()) {
	if (pac_bindings.size() != 1 || expr->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
		return nullptr; // Only works with a comparison against a single PAC binding
	}
	auto &comp = expr->Cast<BoundComparisonExpression>();
	ExpressionType cmp_type = comp.type;

	// Determine which side has the PAC binding
	auto left_bindings = FindCounterBindings(comp.left.get(), counter_bindings);
	auto right_bindings = FindCounterBindings(comp.right.get(), counter_bindings);
	bool left_has_pac = !left_bindings.empty();
	bool right_has_pac = !right_bindings.empty();

	if (left_has_pac == right_has_pac) {
		return nullptr; // both sides or neither — can't optimize
	}
	// Normalize: scalar_side CMP list_side (PAC on right)
	unique_ptr<Expression> scalar_side;
	unique_ptr<Expression> list_side;
	if (left_has_pac) { // PAC on left: flip comparison
		scalar_side = comp.right->Copy();
		list_side = comp.left->Copy();
		cmp_type = FlipComparison(cmp_type);
	} else {
		scalar_side = comp.left->Copy();
		list_side = comp.right->Copy();
	}
	// Iteratively simplify: move arithmetic from list_side to scalar_side
	while (list_side->type == ExpressionType::BOUND_FUNCTION) {
		auto &func = list_side->Cast<BoundFunctionExpression>();
		if (func.children.size() != 2) {
			break;
		}
		int pac_child = FindPacChildIndex(list_side.get(), counter_bindings);
		if (pac_child < 0) {
			break;
		}
		bool needs_positive_check = false;
		const char *inverse_op = GetInverseArithmeticOp(func.function.name, pac_child, needs_positive_check);
		if (!inverse_op) {
			break;
		}
		auto &scalar_operand = func.children[1 - pac_child];
		if (needs_positive_check && !IsPositiveConstant(scalar_operand.get())) {
			break;
		}
		// When the inverse is division, multiply by the reciprocal instead (cheaper at runtime).
		// We know scalar_operand is a positive constant, so we can compute 1/value at plan time.
		double const_val;
		if (inverse_op[0] == '/' && TryGetConstantDouble(scalar_operand.get(), const_val)) {
			auto reciprocal = make_uniq<BoundConstantExpression>(Value::DOUBLE(1.0 / const_val));
			scalar_side = input.optimizer.BindScalarFunction("*", std::move(scalar_side), std::move(reciprocal));
		} else {
			scalar_side =
			    input.optimizer.BindScalarFunction(inverse_op, std::move(scalar_side), scalar_operand->Copy());
		}
		list_side = func.children[pac_child]->Copy();
	}
	// After simplification, list_side should be a bare column ref (possibly through casts)
	Expression *stripped = StripCasts(list_side.get());
	if (stripped->type != ExpressionType::BOUND_COLUMN_REF) {
		// Can't emit pac_filter_<cmp>, but return the simplified comparison
		// so the lambda path benefits from the algebraic rewriting
		return make_uniq<BoundComparisonExpression>(cmp_type, std::move(scalar_side), std::move(list_side));
	}
	// Verify the column ref is a counter binding
	auto &col_ref = stripped->Cast<BoundColumnRefExpression>();
	if (!counter_bindings.count(HashBinding(col_ref.binding))) {
		return make_uniq<BoundComparisonExpression>(cmp_type, std::move(scalar_side), std::move(list_side));
	}
	// Build pac_{filter,select}_<cmp>(scalar_side, counters) or pac_select_<cmp>(hash, scalar, counters)
	const char *func_name =
	    (wrap_kind == PacWrapKind::PAC_SELECT) ? GetPacSelectCmpName(cmp_type) : GetPacFilterCmpName(cmp_type);
	if (!func_name) {
		return make_uniq<BoundComparisonExpression>(cmp_type, std::move(scalar_side), std::move(list_side));
	}
	if (scalar_side->return_type != PacFloatLogicalType()) {
		scalar_side = BoundCastExpression::AddDefaultCastToType(std::move(scalar_side), PacFloatLogicalType());
	}
	auto counters_ref =
	    make_uniq<BoundColumnRefExpression>("pac_var", LogicalType::LIST(PacFloatLogicalType()), col_ref.binding);

	if (wrap_kind == PacWrapKind::PAC_SELECT) {
		// pac_select_<cmp>(hash, scalar, counters) — 3 args, need manual binding
		auto hash_ref = make_uniq<BoundColumnRefExpression>("pac_pu", LogicalType::UBIGINT, pac_hash);
		vector<unique_ptr<Expression>> args;
		args.push_back(std::move(hash_ref));
		args.push_back(std::move(scalar_side));
		args.push_back(std::move(counters_ref));
		vector<LogicalType> arg_types;
		for (auto &a : args) {
			arg_types.push_back(a->return_type);
		}
		auto &catalog = Catalog::GetSystemCatalog(input.context);
		auto &entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(input.context, DEFAULT_SCHEMA, func_name);
		ErrorData error;
		FunctionBinder func_binder(input.context);
		auto best = func_binder.BindFunction(func_name, entry.functions, arg_types, error);
		if (best.IsValid()) {
			auto func = entry.functions.GetFunctionByOffset(best.GetIndex());
			auto bind_info = func.bind ? func.bind(input.context, func, args) : nullptr;
			return make_uniq<BoundFunctionExpression>(func.return_type, func, std::move(args), std::move(bind_info));
		}
		return nullptr;
	}
	return input.optimizer.BindScalarFunction(func_name, std::move(scalar_side), std::move(counters_ref));
}

// Unified expression rewrite: build list_transform over PAC counters and wrap.
// pac_bindings: all PAC aggregate bindings in the expression
// expr: the expression to transform (boolean for filter, numeric for projection)
// wrap_kind: determines terminal function and type parameters
// target_type: for PAC_NOISED, cast result to this type (ignored for PAC_FILTER/PAC_SELECT)
// pac_hash: for PAC_SELECT, the hash binding to pass to pac_select/pac_select_<cmp>
static unique_ptr<Expression>
RewriteExpressionWithCounters(OptimizerExtensionInput &input, const vector<PacBindingInfo> &pac_bindings,
                              Expression *expr, const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                              PacWrapKind wrap_kind, const LogicalType &target_type = PacFloatLogicalType(),
                              ColumnBinding pac_hash = ColumnBinding()) {
	if (pac_bindings.empty()) {
		return nullptr;
	}
	// Try optimized pac_{filter,select}_<cmp> for filter/select comparisons (single binding, simple comparison).
	// TryRewriteFilterComparison always returns the (possibly simplified) expression when it can
	// do any work. Check if the result is a pac_{filter,select}_<cmp> call (full optimization succeeded)
	// or a simplified comparison (partial — pass to lambda path which benefits from the rewriting).
	unique_ptr<Expression> simplified_expr;
	if (wrap_kind == PacWrapKind::PAC_FILTER || wrap_kind == PacWrapKind::PAC_SELECT) {
		auto cmp_result = TryRewriteFilterComparison(input, pac_bindings, expr, counter_bindings, wrap_kind, pac_hash);
		if (cmp_result) {
			if (cmp_result->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
				auto &func = cmp_result->Cast<BoundFunctionExpression>();
				if (func.function.name.rfind("pac_filter_", 0) == 0 ||
				    func.function.name.rfind("pac_select_", 0) == 0) {
					return cmp_result; // Full optimization to pac_{filter,select}_<cmp>
				}
			}
			simplified_expr = std::move(cmp_result); // Partial simplification — use for the lambda path
		}
	}
	Expression *expr_for_lambda = simplified_expr ? simplified_expr.get() : expr;

	LogicalType result_element_type =
	    (wrap_kind == PacWrapKind::PAC_NOISED) ? PacFloatLogicalType() : LogicalType::BOOLEAN;
	auto list_expr =
	    BuildCounterListTransform(input, pac_bindings, expr_for_lambda, counter_bindings, result_element_type);
	if (list_expr) {
		if (wrap_kind == PacWrapKind::PAC_NOISED) {
			auto noised = input.optimizer.BindScalarFunction("pac_noised", std::move(list_expr));
			if (target_type != PacFloatLogicalType()) {
				noised = BoundCastExpression::AddDefaultCastToType(std::move(noised), target_type);
			}
			return noised;
		} else if (wrap_kind == PacWrapKind::PAC_SELECT) {
			auto hash_ref = make_uniq<BoundColumnRefExpression>("pac_pu", LogicalType::UBIGINT, pac_hash);
			return input.optimizer.BindScalarFunction("pac_select", std::move(hash_ref), std::move(list_expr));
		} else {
			return input.optimizer.BindScalarFunction("pac_filter", std::move(list_expr));
		}
	}
	return nullptr;
}

// Replace column bindings in-place throughout an expression tree
static void ReplaceBindingInExpression(Expression &expr, const ColumnBinding &old_binding,
                                       const ColumnBinding &new_binding) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		if (col_ref.binding == old_binding) {
			col_ref.binding = new_binding;
		}
	} else {
		ExpressionIterator::EnumerateChildren(
		    expr, [&](Expression &child) { ReplaceBindingInExpression(child, old_binding, new_binding); });
	}
}

// Expand projection references in a filter/join expression.
// For each col_ref to a projection slot with counter arithmetic:
// - create counter pass-through slots in the projection
// - substitute the col_ref with the arithmetic rebased to pass-through bindings
// This lets the filter fold arithmetic + comparison into one list_transform lambda.
static void ExpandProjectionRefsInExpression(unique_ptr<Expression> &expr, LogicalOperator *plan_root,
                                             unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		if (!counter_bindings.count(HashBinding(col_ref.binding))) {
			return; // Not a counter binding — nothing to expand
		}
		auto *source_op = FindOperatorByTableIndex(plan_root, col_ref.binding.table_index);
		if (!source_op || source_op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return;
		}
		auto &proj = source_op->Cast<LogicalProjection>();
		idx_t slot = col_ref.binding.column_index;
		if (slot >= proj.expressions.size()) {
			return;
		}
		// Only expand slots with multi-binding counter arithmetic that needs decomposition.
		// Skip: simple col_refs (already pass-throughs) and already-wrapped terminals.
		auto &proj_expr = proj.expressions[slot];
		if (proj_expr->type == ExpressionType::BOUND_COLUMN_REF) {
			return;
		}
		if (IsAlreadyWrappedInPacNoised(proj_expr.get())) {
			return;
		}
		auto pac_bindings = FindCounterBindings(proj_expr.get(), counter_bindings);
		if (pac_bindings.empty()) {
			return;
		}
		auto list_type = LogicalType::LIST(PacFloatLogicalType());
		// Clone the projection expression for rebasing
		auto expanded = proj_expr->Copy();
		// Create pass-through for first binding (reuse this slot)
		proj.expressions[slot] = make_uniq<BoundColumnRefExpression>("pac_var", list_type, pac_bindings[0].binding);
		if (slot < proj.types.size()) {
			proj.types[slot] = list_type;
		}
		ReplaceBindingInExpression(*expanded, pac_bindings[0].binding, ColumnBinding(proj.table_index, slot));
		// Extra pass-throughs for additional bindings
		for (idx_t j = 1; j < pac_bindings.size(); j++) {
			idx_t extra_slot = proj.expressions.size();
			proj.expressions.push_back(
			    make_uniq<BoundColumnRefExpression>("pac_var", list_type, pac_bindings[j].binding));
			proj.types.push_back(list_type);
			auto extra_cb = ColumnBinding(proj.table_index, extra_slot);
			auto src_it = counter_bindings.find(HashBinding(pac_bindings[j].binding));
			AddCounterBinding(counter_bindings, extra_cb,
			                  (src_it != counter_bindings.end()) ? src_it->second : pac_bindings[j].binding);
			ReplaceBindingInExpression(*expanded, pac_bindings[j].binding, extra_cb);
		}
		expr = std::move(expanded);
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		ExpandProjectionRefsInExpression(child, plan_root, counter_bindings);
	});
}

// Wrap counter column references with pac_noised.
// After converting pac_noised_sum → pac_sum (LIST<FLOAT> output),
// expressions may still reference the aggregate with the original type (e.g. DECIMAL).
// This wraps those references with pac_noised() to convert back to scalar, then casts to original type.
static void WrapCounterRefsWithNoised(unique_ptr<Expression> &expr,
                                      const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                                      OptimizerExtensionInput &input) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		if (counter_bindings.count(HashBinding(col_ref.binding))) {
			auto original_type = col_ref.return_type;
			unique_ptr<Expression> noised = input.optimizer.BindScalarFunction("pac_noised", expr->Copy());
			if (original_type != PacFloatLogicalType()) {
				noised = BoundCastExpression::AddDefaultCastToType(std::move(noised), original_type);
			}
			expr = std::move(noised);
			return;
		}
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { WrapCounterRefsWithNoised(child, counter_bindings, input); });
}

// Rewrite a single projection expression: update col_ref types, build list_transform + pac_noised terminal.
// Only called for non-filter-pattern projections (filter patterns are handled by the parent filter/join).
// Returns true if the slot remains a counter binding (non-terminal pass-through).
static bool RewriteProjectionExpression(OptimizerExtensionInput &input, LogicalProjection &proj, idx_t i,
                                        LogicalOperator *plan_root, bool is_terminal,
                                        const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	auto &expr = proj.expressions[i];
	if (IsAlreadyWrappedInPacNoised(expr.get())) {
		return false;
	}
	auto pac_bindings = FindCounterBindings(expr.get(), counter_bindings);
	if (pac_bindings.empty()) {
		return false;
	}
	// Simple column reference to a counter list.
	// After the universal col_ref type fix, the col_ref type is LIST<FLOAT>.
	// proj.types[i] still holds the original scalar type (e.g. HUGEINT).
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto original_type = (i < proj.types.size()) ? proj.types[i] : expr->return_type;
		if (is_terminal) {
			unique_ptr<Expression> result = input.optimizer.BindScalarFunction("pac_noised", expr->Copy());
			if (original_type != PacFloatLogicalType()) {
				result = BoundCastExpression::AddDefaultCastToType(std::move(result), original_type);
			}
			proj.expressions[i] = std::move(result);
			proj.types[i] = original_type;
			return false; // terminal — scalar output
		} else if (i < proj.types.size()) {
			proj.types[i] = LogicalType::LIST(PacFloatLogicalType());
		}
		return true; // non-terminal pass-through — still a counter binding
	}
	if (!IsNumericalType(expr->return_type)) {
		WrapCounterRefsWithNoised(expr, counter_bindings, input);
		return false;
	}
	// Arithmetic over PAC aggregates: build list_transform + pac_noised terminal.
	Expression *expr_to_clone = expr.get();
	if (expr->type == ExpressionType::OPERATOR_CAST &&
	    expr->Cast<BoundCastExpression>().return_type != PacFloatLogicalType()) {
		expr_to_clone = expr->Cast<BoundCastExpression>().child.get();
	}
	auto result = RewriteExpressionWithCounters(input, pac_bindings, expr_to_clone, counter_bindings,
	                                            PacWrapKind::PAC_NOISED, expr->return_type);
	if (result) {
		proj.expressions[i] = std::move(result);
		proj.types[i] = expr->return_type;
	}
	return false;
}

// Insert a pass-through projection below filter_child_slot that replaces the hash column
// with pac_select_expr. Returns the new hash binding in the projection's output.
// Updates all bindings above via ColumnBindingReplacer.
static ColumnBinding InsertPacSelectProjection(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                               unique_ptr<LogicalOperator> &filter_child_slot,
                                               const ColumnBinding &hash_binding,
                                               unique_ptr<Expression> pac_select_expr) {
	filter_child_slot->ResolveOperatorTypes();
	auto old_bindings = filter_child_slot->GetColumnBindings();
	idx_t num_cols = old_bindings.size();

	idx_t proj_table_index = input.optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> proj_expressions;

	// Find which position the hash binding occupies
	idx_t hash_position = num_cols;
	for (idx_t i = 0; i < num_cols; i++) {
		if (old_bindings[i] == hash_binding) {
			hash_position = i;
			break;
		}
	}
	for (idx_t i = 0; i < num_cols; i++) {
		if (i == hash_position) {
			proj_expressions.push_back(nullptr); // placeholder — filled below
		} else {
			proj_expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(filter_child_slot->types[i], old_bindings[i]));
		}
	}
	if (hash_position < num_cols) {
		proj_expressions[hash_position] = std::move(pac_select_expr);
	} else {
		// Hash binding was not in direct output — append it
		proj_expressions.push_back(std::move(pac_select_expr));
	}

	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_expressions));
	projection->children.push_back(std::move(filter_child_slot));
	projection->ResolveOperatorTypes();

	LogicalOperator *proj_ptr = projection.get();
	filter_child_slot = std::move(projection);

	// Remap old bindings → new projection bindings
	ColumnBindingReplacer replacer;
	for (idx_t i = 0; i < num_cols; i++) {
		replacer.replacement_bindings.emplace_back(old_bindings[i], ColumnBinding(proj_table_index, i));
	}
	replacer.stop_operator = proj_ptr;
	replacer.VisitOperator(*plan);

	return ColumnBinding(proj_table_index, hash_position);
}

// Single bottom-up rewrite pass.
// Processes children first, then current operator. Handles:
// - AGGREGATE: convert pac_noised_sum → pac_sum (counters), then aggregate-over-counters → pac_sum (list)
// - PROJECTION: update simple col_ref types, build list_transform + pac_noised for arithmetic
// - FILTER (in rewrite_map): build list_transform + pac_filter
// - JOIN (in rewrite_map): rewrite conditions (two-list → CROSS_PRODUCT+FILTER, single-list → double-lambda)
static void RewriteBottomUp(unique_ptr<LogicalOperator> &op_ptr, OptimizerExtensionInput &input,
                            unique_ptr<LogicalOperator> &plan,
                            const unordered_map<LogicalOperator *, unordered_set<idx_t>> &pattern_lookup,
                            vector<CategoricalPatternInfo> &patterns, unordered_set<uint64_t> &replaced_mark_bindings,
                            unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                            bool inside_cte_definition = false) {
	auto *op = op_ptr.get();
	// Strip scalar wrappers (Projection→first()→Projection) over PAC aggregates before recursing.
	// This removes the first() aggregate that can't handle LIST<DOUBLE>, and lets the inner
	// projection be processed naturally with the outer's table_index.
	// Track whether we stripped, so the inner projection is treated as terminal (scalar output).
	bool stripped_scalar_wrapper = false;
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION && !inside_cte_definition) {
		auto *unwrapped = RecognizeDuckDBScalarWrapper(op);
		if (unwrapped && !FindPacAggregateInOperator(unwrapped).empty()) {
			StripScalarWrapperInPlace(op_ptr, true);
			op = op_ptr.get();
			stripped_scalar_wrapper = true;
		}
	}
	for (idx_t ci = 0; ci < op->children.size(); ci++) { // Recurse into children first (bottom-up)
		// Child 0 of MATERIALIZED_CTE is the CTE definition — its output types must remain
		// stable (numeric, not LIST) because CTE_SCAN consumers may re-aggregate the results.
		bool child_in_cte =
		    inside_cte_definition || (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && ci == 0);
		RewriteBottomUp(op->children[ci], input, plan, pattern_lookup, patterns, replaced_mark_bindings,
		                counter_bindings, child_in_cte);
	}
	// After processing children, check if this FILTER references a mark column
	// from a MARK_JOIN that was replaced by CROSS_PRODUCT + FILTER(pac_filter_eq).
	// The pac_filter_eq below already handles the filtering, so this FILTER is
	// now redundant with a dangling mark column reference. Remove it.
	if (op->type == LogicalOperatorType::LOGICAL_FILTER && !replaced_mark_bindings.empty()) {
		auto &filter = op->Cast<LogicalFilter>();
		bool has_dangling_mark = false;
		for (auto &fexpr : filter.expressions) {
			if (fexpr->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &ref = fexpr->Cast<BoundColumnRefExpression>();
				if (replaced_mark_bindings.count(HashBinding(ref.binding))) {
					has_dangling_mark = true;
					break;
				}
			}
		}
		if (has_dangling_mark && !filter.children.empty()) {
			op_ptr = std::move(filter.children[0]);
			return;
		}
	}
	LogicalOperator *plan_root = plan.get();
	// Universal col_ref type fix: after children have been processed (and populated counter_bindings),
	// fix stale col_ref types in the current operator's expressions. When PAC aggregates are converted
	// to counters, output types change (e.g. DECIMAL → LIST<FLOAT>). Expressions in all operators
	// may reference the old types. Fix them before operator-specific semantic work.
	// Skip universal type fix for ordering/limit operators — they need scalar types.
	// The terminal projection below them handles pac_noised wrapping.
	bool skip_type_fix =
	    (op->type == LogicalOperatorType::LOGICAL_ORDER_BY || op->type == LogicalOperatorType::LOGICAL_TOP_N ||
	     op->type == LogicalOperatorType::LOGICAL_LIMIT);
	if (!inside_cte_definition && !counter_bindings.empty() && !skip_type_fix) {
		auto list_type = LogicalType::LIST(PacFloatLogicalType());
		LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) {
			std::function<void(unique_ptr<Expression> &)> Fix = [&](unique_ptr<Expression> &e) {
				if (e->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &col_ref = e->Cast<BoundColumnRefExpression>();
					if (counter_bindings.count(HashBinding(col_ref.binding))) {
						col_ref.return_type = list_type;
					}
				}
				ExpressionIterator::EnumerateChildren(*e, Fix);
			};
			Fix(*expr_ptr);
		});
	}
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && !inside_cte_definition) {
		// === AGGREGATE: convert PAC aggregates to _counters, then check aggregates-over-counters ===
		auto &agg = op->Cast<LogicalAggregate>();

		// First: replace standard/PAC aggregates whose input references counter bindings
		// with _list variants. Must happen before _counters conversion below so that
		// the projection rewrite (later in this pass) sees _list outputs.
		ReplaceAggregatesOverCounters(op, input.context, counter_bindings);

		// Convert PAC aggregates to _counters variants — only the specific
		// expressions that are sources of categorical patterns.
		for (idx_t i = 0; i < agg.expressions.size(); i++) {
			auto &agg_expr = agg.expressions[i];
			if (agg_expr->type != ExpressionType::BOUND_AGGREGATE) {
				continue;
			}
			auto &bound_agg = agg_expr->Cast<BoundAggregateExpression>();
			if (!IsPacAggregate(bound_agg.function.name)) {
				continue;
			}
			// Check if this specific aggregate expression is a source of any pattern
			ColumnBinding this_binding(agg.aggregate_index, i);
			bool convert = false;
			for (auto &p : patterns) {
				for (auto &bi : p.pac_bindings) {
					if (bi.source_binding == this_binding) {
						convert = true;
						break;
					}
				}
				if (convert) {
					break;
				}
			}
			if (!convert) {
				continue;
			}
			string counters_name = GetCountersVariant(bound_agg.function.name);
			vector<unique_ptr<Expression>> children;
			for (auto &child_expr : bound_agg.children) {
				children.push_back(child_expr->Copy());
			}
			auto new_aggr = RebindAggregate(input.context, counters_name, std::move(children), bound_agg.IsDistinct());
			if (!new_aggr) { // Fallback: just rename the function in place
				bound_agg.function.name = counters_name;
				bound_agg.function.return_type = LogicalType::LIST(PacFloatLogicalType());
				agg_expr->return_type = LogicalType::LIST(PacFloatLogicalType());
			} else {
				agg.expressions[i] = std::move(new_aggr);
			}
			AddCounterBinding(counter_bindings, ColumnBinding(agg.aggregate_index, i));
			idx_t types_index = agg.groups.size() + i;
			if (types_index < agg.types.size()) {
				agg.types[types_index] = LogicalType::LIST(PacFloatLogicalType());
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION &&
	           !inside_cte_definition) { // === PROJECTION: rewrite PAC expressions ===
		auto &proj = op->Cast<LogicalProjection>();
		// Check if this projection's output is consumed by a filter/join pattern.
		// If so, skip terminal wrapping — the parent filter/join handles the rewrite.
		bool is_filter_pattern = false;
		for (auto &p : patterns) {
			if (p.parent_op && pattern_lookup.count(p.parent_op)) {
				for (auto &bi : p.pac_bindings) {
					if (bi.binding.table_index == proj.table_index) {
						is_filter_pattern = true;
						break;
					}
				}
			}
			if (is_filter_pattern) {
				break;
			}
		}
		if (is_filter_pattern) {
			// Filter-pattern projection: minimal processing only (type updates, no terminal wrapping).
			// The parent filter/join will expand these expressions and generate list_transform + pac_filter.
			for (idx_t i = 0; i < proj.expressions.size(); i++) {
				PrepareFilterPatternSlot(proj, i, counter_bindings);
				// All filter-pattern slots with counter content remain counter bindings
				auto slot_bindings = FindCounterBindings(proj.expressions[i].get(), counter_bindings);
				if (!slot_bindings.empty()) {
					auto proj_cb = ColumnBinding(proj.table_index, i);
					AddCounterBinding(counter_bindings, proj_cb,
					                  TraceCounterOrigin(proj.expressions[i], counter_bindings));
				}
			}
		} else {
			// Normal projection: full PAC rewriting with pac_noised terminal.
			bool is_terminal = stripped_scalar_wrapper || (op == plan_root);
			if (!is_terminal) {
				auto *root = plan_root;
				while (root &&
				       (root->type == LogicalOperatorType::LOGICAL_ORDER_BY ||
				        root->type == LogicalOperatorType::LOGICAL_TOP_N ||
				        root->type == LogicalOperatorType::LOGICAL_LIMIT) &&
				       !root->children.empty()) {
					root = root->children[0].get();
				}
				is_terminal = (root == op);
			}
			for (idx_t i = 0; i < proj.expressions.size(); i++) {
				if (RewriteProjectionExpression(input, proj, i, plan_root, is_terminal, counter_bindings)) {
					auto proj_cb = ColumnBinding(proj.table_index, i);
					AddCounterBinding(counter_bindings, proj_cb,
					                  TraceCounterOrigin(proj.expressions[i], counter_bindings));
				}
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_FILTER &&
	           !inside_cte_definition) { // === FILTER: rewrite expressions with pac_filter ===
		auto it = pattern_lookup.find(op);
		if (it != pattern_lookup.end()) {
			auto &filter = op->Cast<LogicalFilter>();
			for (auto expr_idx : it->second) {
				if (expr_idx >= filter.expressions.size()) {
					continue;
				}
				auto &filter_expr = filter.expressions[expr_idx];
				// Expand projection arithmetic into the filter expression so arithmetic + comparison
				// are folded into one list_transform lambda. Handles both single and multi-binding.
				ExpandProjectionRefsInExpression(filter_expr, plan_root, counter_bindings);
				auto pac_bindings = FindCounterBindings(filter_expr.get(), counter_bindings);
				if (pac_bindings.empty()) {
					continue;
				}
				// Determine wrap kind: PAC_SELECT if outer pac aggregate exists and setting enabled.
				// HAVING filters are excluded during detection, so any FILTER pattern here
				// is a genuine categorical filter (WHERE or subquery predicate).
				PacWrapKind wrap_kind = PacWrapKind::PAC_FILTER;
				ColumnBinding pattern_hash;
				for (auto &p : patterns) {
					if (p.parent_op == op && p.has_outer_pac_hash) {
						pattern_hash = p.outer_pac_hash;
						if (GetBooleanSetting(input.context, "pac_select", true)) {
							wrap_kind = PacWrapKind::PAC_SELECT;
						}
						break;
					}
				}
				auto result = RewriteExpressionWithCounters(input, pac_bindings, filter_expr.get(), counter_bindings,
				                                            wrap_kind, PacFloatLogicalType(), pattern_hash);
				if (result) {
					if (wrap_kind == PacWrapKind::PAC_SELECT) {
						// Insert projection below filter replacing hash with pac_select result
						auto new_hash =
						    InsertPacSelectProjection(input, plan, op->children[0], pattern_hash, std::move(result));
						// Replace filter condition with pac_hash <> 0 for majority-vote decision
						filter.expressions[expr_idx] = make_uniq<BoundComparisonExpression>(
						    ExpressionType::COMPARE_NOTEQUAL,
						    make_uniq<BoundColumnRefExpression>("pac_pu", LogicalType::UBIGINT, new_hash),
						    make_uniq<BoundConstantExpression>(Value::UBIGINT(0)));
					} else {
						filter.expressions[expr_idx] = std::move(result);
					}
				}
			}
		}
	} else if ((op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	            op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) &&
	           !inside_cte_definition) { // === JOIN: rewrite comparison conditions ===
		auto it = pattern_lookup.find(op);
		if (it == pattern_lookup.end()) {
			return;
		}
		// Determine wrap kind for join patterns (same logic as FILTER above)
		PacWrapKind join_wrap_kind = PacWrapKind::PAC_FILTER;
		ColumnBinding join_pattern_hash;
		for (auto &p : patterns) {
			if (p.parent_op == op && p.has_outer_pac_hash) {
				join_pattern_hash = p.outer_pac_hash;
				if (GetBooleanSetting(input.context, "pac_select", true)) {
					join_wrap_kind = PacWrapKind::PAC_SELECT;
				}
				break;
			}
		}
		auto &join = op->Cast<LogicalComparisonJoin>();
		for (auto expr_idx : it->second) {
			if (expr_idx >= join.conditions.size()) {
				continue;
			}
			auto &cond = join.conditions[expr_idx];
			// Expand projection arithmetic into join conditions
			ExpandProjectionRefsInExpression(cond.left, plan_root, counter_bindings);
			ExpandProjectionRefsInExpression(cond.right, plan_root, counter_bindings);
			auto left_bindings = FindCounterBindings(cond.left.get(), counter_bindings);
			auto right_bindings = FindCounterBindings(cond.right.get(), counter_bindings);
			bool left_is_list = !left_bindings.empty();
			bool right_is_list = !right_bindings.empty();
			if (!left_is_list && !right_is_list) {
				continue;
			}
			// Collect PAC bindings (deduplicated)
			vector<PacBindingInfo> all_bindings;
			unordered_set<uint64_t> seen_hashes;
			for (auto &b : left_bindings) {
				uint64_t h = HashBinding(b.binding);
				if (seen_hashes.insert(h).second) {
					all_bindings.push_back(b);
				}
			}
			for (auto &b : right_bindings) {
				uint64_t h = HashBinding(b.binding);
				if (seen_hashes.insert(h).second) {
					all_bindings.push_back(b);
				}
			}
			for (idx_t j = 0; j < all_bindings.size(); j++) {
				all_bindings[j].index = j;
			}
			auto comparison =
			    make_uniq<BoundComparisonExpression>(cond.comparison, cond.left->Copy(), cond.right->Copy());
			auto pac_expr = RewriteExpressionWithCounters(input, all_bindings, comparison.get(), counter_bindings,
			                                              join_wrap_kind, PacFloatLogicalType(), join_pattern_hash);
			if (pac_expr) {
				auto cross_product =
				    LogicalCrossProduct::Create(std::move(join.children[0]), std::move(join.children[1]));
				auto filter_op = make_uniq<LogicalFilter>();

				if (join_wrap_kind == PacWrapKind::PAC_SELECT) {
					// Insert projection below filter replacing hash with pac_select result
					auto new_hash =
					    InsertPacSelectProjection(input, plan, cross_product, join_pattern_hash, std::move(pac_expr));
					filter_op->expressions.push_back(make_uniq<BoundComparisonExpression>(
					    ExpressionType::COMPARE_NOTEQUAL,
					    make_uniq<BoundColumnRefExpression>("pac_pu", LogicalType::UBIGINT, new_hash),
					    make_uniq<BoundConstantExpression>(Value::UBIGINT(0))));
				} else {
					filter_op->expressions.push_back(std::move(pac_expr));
				}
				filter_op->children.push_back(std::move(cross_product));

				for (auto &p : patterns) {
					if (p.parent_op == op) {
						p.parent_op = nullptr;
					}
				}
				// Record the mark column binding so the parent FILTER
				// (which references it) can be cleaned up.
				if (join.join_type == JoinType::MARK) {
					replaced_mark_bindings.insert(HashBinding(ColumnBinding(join.mark_index, 0)));
				}
				op_ptr = std::move(filter_op);
				break;
			}
		}
	}
}

// Convert aggregates whose value child references a counter binding to _list variants.
// Catches cases ReplaceAggregatesOverCounters misses (DELIM_JOIN paths where tracing
// through the plan fails but counter_bindings has the correct information).
static void ConvertPacAggregatesToListVariants(LogicalOperator &plan, ClientContext &context,
                                               unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	for (auto &child : plan.children) {
		ConvertPacAggregatesToListVariants(*child, context, counter_bindings);
	}
	if (plan.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &agg = plan.Cast<LogicalAggregate>();
	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		if (agg.expressions[i]->type != ExpressionType::BOUND_AGGREGATE) {
			continue;
		}
		auto &bound_agg = agg.expressions[i]->Cast<BoundAggregateExpression>();
		if (bound_agg.children.empty() || IsPacListAggregate(bound_agg.function.name)) {
			continue;
		}
		idx_t value_idx =
		    (IsPacAggregate(bound_agg.function.name) || IsPacCountersAggregate(bound_agg.function.name)) &&
		            bound_agg.children.size() > 1
		        ? 1
		        : 0;
		if (!ReferencesCounterBinding(bound_agg.children[value_idx].get(), counter_bindings)) {
			continue;
		}
		if (TryRebindToListVariant(agg, i, context)) {
			AddCounterBinding(counter_bindings, ColumnBinding(agg.aggregate_index, i));
		}
	}
}

void RewriteCategoricalQuery(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// Detect categorical patterns
	vector<CategoricalPatternInfo> patterns;
	FindCategoricalPatternsInOperator(plan.get(), plan.get(), patterns, false);
	for (idx_t i = 0; i < patterns.size(); i++) {
		auto &p = patterns[i];
		for (idx_t j = 0; j < p.pac_bindings.size(); j++) {
			auto &bi = p.pac_bindings[j];
		}
	}
	if (patterns.empty()) {
		return;
	}
	// Build lightweight lookup: operator → set of expression indices
	unordered_map<LogicalOperator *, unordered_set<idx_t>> pattern_lookup;
	for (auto &p : patterns) {
		if (p.parent_op) {
			pattern_lookup[p.parent_op].insert(p.expr_index);
		}
	}
	// Bottom-up rewrite pass
	// - Aggregates: pac_noised_sum → pac_sum (counters), then aggregate-over-counters → pac_sum (list)
	// - Projections: update col_ref types, build list_transform + pac_noised/pass-through
	// - Filters: build list_transform + pac_filter
	// - Joins: rewrite conditions (two-list → CROSS_PRODUCT+FILTER, single-list → double-lambda)
	unordered_set<uint64_t> replaced_mark_bindings;
	unordered_map<uint64_t, ColumnBinding> counter_bindings;
	RewriteBottomUp(plan, input, plan, pattern_lookup, patterns, replaced_mark_bindings, counter_bindings);
	// Fix up aggregates that now consume LIST (from inner _counters) to _list variants.
	ConvertPacAggregatesToListVariants(*plan, input.context, counter_bindings);
	plan->ResolveOperatorTypes();
}

} // namespace duckdb
