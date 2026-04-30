#include "compiler/dp_elastic_compiler.hpp"
#include "utils/privacy_helpers.hpp"
#include "query_processing/pac_plan_traversal.hpp"
#include "privacy_debug.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <cmath>

namespace duckdb {

// ----------------------------------------------------------------------------
// FK chain structure — ordered from most-distant non-PU table to PU
// ----------------------------------------------------------------------------

struct DPFKChain {
	// tables[0..n-2] are non-PU; tables[n-1] is the PU
	// For single-table PU queries (no joins): tables = [pu_name], fk_cols empty
	vector<string> tables;
	// fk_cols[i] = FK column on tables[i] linking to tables[i+1]
	vector<string> fk_cols;
};

// ----------------------------------------------------------------------------
// Plan walkers
// ----------------------------------------------------------------------------

static void CollectGetNodes(LogicalOperator *op, vector<LogicalGet *> &out) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		out.push_back(&op->Cast<LogicalGet>());
	}
	for (auto &child : op->children) {
		CollectGetNodes(child.get(), out);
	}
}

// Find the unique_ptr slot holding `target` in the plan. Returns nullptr if not found.
static unique_ptr<LogicalOperator> *FindSlotForOperator(unique_ptr<LogicalOperator> &root, LogicalOperator *target) {
	if (root.get() == target) {
		return &root;
	}
	for (auto &child : root->children) {
		auto *slot = FindSlotForOperator(child, target);
		if (slot) {
			return slot;
		}
	}
	return nullptr;
}

// ----------------------------------------------------------------------------
// FK chain extraction — validates linear chain structure, detects self-joins
// ----------------------------------------------------------------------------

static DPFKChain ExtractFKChain(const PrivacyCompatibilityResult &check, const vector<LogicalGet *> &gets,
                                const vector<string> &privacy_units) {
	(void)privacy_units;

	// Self-join check: no table name may appear more than once in the plan
	std::unordered_map<string, int> name_count;
	for (auto *g : gets) {
		string name = StringUtil::Lower(g->GetTable()->name);
		name_count[name]++;
		if (name_count[name] > 1) {
			throw InvalidInputException("dp_elastic: self-joins are not supported (table '" + g->GetTable()->name +
			                            "' appears more than once)");
		}
	}

	// Single-table case: no non-PU tables → chain is just the PU itself, ES = 1
	if (check.scanned_non_pu_tables.empty()) {
		if (check.scanned_pu_tables.size() != 1) {
			throw InvalidInputException("dp_elastic: expected exactly one privacy unit table, found " +
			                            std::to_string(check.scanned_pu_tables.size()));
		}
		return {{check.scanned_pu_tables[0]}, {}};
	}

	// At most one PU may be scanned directly. Zero is fine — the PU is then inferred from
	// PRIVACY_LINK metadata via fk_paths (no explicit join required).
	if (check.scanned_pu_tables.size() > 1) {
		string pu_list;
		for (auto &pu : check.scanned_pu_tables) {
			if (!pu_list.empty()) {
				pu_list += ", ";
			}
			pu_list += "'" + pu + "'";
		}
		throw InvalidInputException("dp_elastic: query touches multiple privacy unit tables (" + pu_list +
		                            "). Only one privacy unit is supported per query.");
	}

	// Find non-PU table with the longest FK path — this is the root of the chain.
	// fk_paths contains the full path to the PU (including the PU itself) for every scanned
	// non-PU table, whether or not the PU is explicitly joined in the query.
	const vector<string> *longest_path = nullptr;
	for (auto &non_pu : check.scanned_non_pu_tables) {
		auto it = check.fk_paths.find(non_pu);
		if (it == check.fk_paths.end() || it->second.empty()) {
			throw InvalidInputException("dp_elastic: table '" + non_pu +
			                            "' has no PRIVACY_LINK path to a privacy unit table. "
			                            "Declare the link with ALTER TABLE ADD PRIVACY_LINK.");
		}
		if (!longest_path || it->second.size() > longest_path->size()) {
			longest_path = &it->second;
		}
	}
	D_ASSERT(longest_path);

	// Build a set of tables in the longest path for membership checks
	std::unordered_set<string> path_set;
	for (auto &t : *longest_path) {
		path_set.insert(StringUtil::Lower(t));
	}

	// All non-PU scanned tables must appear in the longest path (linear chain requirement)
	for (auto &non_pu : check.scanned_non_pu_tables) {
		if (path_set.find(StringUtil::Lower(non_pu)) == path_set.end()) {
			throw InvalidInputException("dp_elastic: star/diamond joins are not yet supported. Table '" + non_pu +
			                            "' is not in the linear FK chain to the privacy unit. "
			                            "All joined tables must form a single linear chain.");
		}
	}

	// Build fk_cols: for each consecutive pair in the chain, find the FK column
	DPFKChain chain;
	chain.tables = *longest_path;
	chain.fk_cols.reserve(chain.tables.size() - 1);

	for (idx_t i = 0; i + 1 < chain.tables.size(); i++) {
		const string &from_table = chain.tables[i];
		const string &to_table = chain.tables[i + 1];

		auto meta_it = check.table_metadata.find(from_table);
		if (meta_it == check.table_metadata.end()) {
			throw InternalException("dp_elastic: no metadata for table '" + from_table + "'");
		}
		bool found_fk = false;
		for (auto &fk : meta_it->second.fks) {
			if (StringUtil::Lower(fk.first) == StringUtil::Lower(to_table)) {
				if (fk.second.empty()) {
					throw InternalException("dp_elastic: empty FK column list for " + from_table + " → " + to_table);
				}
				chain.fk_cols.push_back(fk.second[0]);
				found_fk = true;
				break;
			}
		}
		if (!found_fk) {
			throw InternalException("dp_elastic: could not find FK column from '" + from_table + "' to '" + to_table +
			                        "'");
		}
	}

	return chain;
}

// ----------------------------------------------------------------------------
// Eligibility check (Phase D: allows linear FK join chains)
// ----------------------------------------------------------------------------

struct DPEligibility {
	LogicalAggregate *top_agg;
	DPFKChain fk_chain;
};

static DPEligibility CheckDPEligibility(unique_ptr<LogicalOperator> &plan, const vector<string> &privacy_units,
                                        const PrivacyCompatibilityResult &check) {
	vector<LogicalGet *> gets;
	CollectGetNodes(plan.get(), gets);

	// Validates self-joins, PU count, and linear chain structure; returns the chain
	DPFKChain fk_chain = ExtractFKChain(check, gets, privacy_units);

	vector<LogicalAggregate *> aggs;
	FindAllAggregates(plan, aggs);
	if (aggs.size() != 1) {
		throw InvalidInputException("dp_elastic: expected exactly one aggregate, found " + std::to_string(aggs.size()));
	}
	auto *agg = aggs[0];
	for (auto &expr : agg->expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			throw InvalidInputException("dp_elastic: unsupported non-aggregate expression in aggregate node");
		}
		auto &a = expr->Cast<BoundAggregateExpression>();
		if (a.IsDistinct()) {
			throw InvalidInputException("dp_elastic: DISTINCT aggregates are not supported");
		}
		const string &name = a.function.name;
		if (name != "count" && name != "count_star" && name != "sum") {
			throw InvalidInputException("dp_elastic: only COUNT and SUM are supported (got '" + name + "')");
		}
		if (name == "sum" && a.children.size() != 1) {
			throw InvalidInputException("dp_elastic: sum must have exactly one argument");
		}
		if (name == "sum" && !a.children[0]->return_type.IsNumeric()) {
			throw InvalidInputException("dp_elastic: SUM argument must be a numeric type (got '" +
			                            a.children[0]->return_type.ToString() + "')");
		}
	}
	return {agg, std::move(fk_chain)};
}

// ----------------------------------------------------------------------------
// mf_K computation — max frequency of fk_col values in table (one auxiliary query)
// ----------------------------------------------------------------------------

static double ComputeMfK(ClientContext &context, const string &table_name, const string &fk_col) {
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection conn(db);
	string query = "SELECT COALESCE(CAST(MAX(cnt) AS DOUBLE), 0.0) "
	               "FROM (SELECT COUNT(*) AS cnt FROM " +
	               table_name + " GROUP BY " + fk_col + ")";
	auto result = conn.Query(query);
	if (!result || result->HasError()) {
		throw InvalidInputException("dp_elastic: failed to compute mf_K for " + table_name + "." + fk_col +
		                            (result ? (": " + result->GetError()) : ": null result"));
	}
	if (result->RowCount() == 0) {
		// Table is empty: no rows contribute → max frequency is 0
		return 0.0;
	}
	auto val = result->GetValue(0, 0);
	return val.IsNull() ? 0.0 : val.GetValue<double>();
}

// Collect mf_K values for all FK hops (one per non-PU table in the chain)
static vector<double> CollectMfKValues(ClientContext &context, const DPFKChain &chain) {
	vector<double> mf_values;
	mf_values.reserve(chain.fk_cols.size());
	for (idx_t i = 0; i + 1 < chain.tables.size(); i++) {
		double mf_k = ComputeMfK(context, chain.tables[i], chain.fk_cols[i]);
		PRIVACY_DEBUG_PRINT("[DP_ELASTIC] mf_K(" + chain.tables[i] + "." + chain.fk_cols[i] +
		                    ") = " + std::to_string(mf_k));
		mf_values.push_back(mf_k);
	}
	return mf_values;
}

// SES_β(D) = max_{k≥0} [product_i(mf_i + k) * exp(-β * k)]
// The maximum is at k* ≈ n/β − mean(mf_i). k_max must reach at least k*.
static double ComputeSmoothElasticSensitivity(const vector<double> &mf_values, double beta) {
	double ses = 1.0;
	for (auto m : mf_values) {
		ses *= m;
	}
	// k* ≈ n/β; add margin and cap to avoid impractically long loops
	double n = static_cast<double>(mf_values.size());
	int k_max = static_cast<int>(std::min(n / beta + 200.0, 100000.0));
	k_max = std::max(k_max, 1000);
	for (int k = 1; k <= k_max; k++) {
		double decay = std::exp(-beta * static_cast<double>(k));
		if (decay < 1e-15) {
			break;
		}
		double ls_k = 1.0;
		for (auto m : mf_values) {
			ls_k *= (m + static_cast<double>(k));
		}
		double term = ls_k * decay;
		if (term > ses) {
			ses = term;
		}
	}
	return ses;
}

// Compute ES as the noise multiplier (divide by epsilon in the caller to get Laplace scale).
//   Global (no dp_delta): ES = product(mf_i), pure ε-DP
//   Smooth (dp_delta > 0): ES = 2·SES_β with β = ε/(2·ln(2/δ)), achieves (ε,δ)-DP
static double ComputeElasticSensitivity(ClientContext &context, const DPFKChain &chain, double epsilon) {
	auto mf_values = CollectMfKValues(context, chain);

	double delta = 0.0;
	if (TryGetDpDelta(context, delta) && delta > 0.0 && delta < 1.0) {
		if (delta > 0.5) {
			throw InvalidInputException("dp_elastic: dp_delta must be < 0.5 for meaningful (ε,δ)-DP (got " +
			                            std::to_string(delta) + ")");
		}
		double beta = epsilon / (2.0 * std::log(2.0 / delta));
		double ses = ComputeSmoothElasticSensitivity(mf_values, beta);
		PRIVACY_DEBUG_PRINT("[DP_ELASTIC] smooth sensitivity: beta=" + std::to_string(beta) +
		                    " SES=" + std::to_string(ses));
		return 2.0 * ses;
	}

	// Global elastic sensitivity: product of all mf_K values
	double es = 1.0;
	for (auto m : mf_values) {
		es *= m;
	}
	PRIVACY_DEBUG_PRINT("[DP_ELASTIC] global elastic sensitivity = " + std::to_string(es));
	return es;
}

// ----------------------------------------------------------------------------
// Aggregate helpers
// ----------------------------------------------------------------------------

static bool AggregateContainsSum(const LogicalAggregate *agg) {
	for (auto &expr : agg->expressions) {
		auto &a = expr->Cast<BoundAggregateExpression>();
		if (a.function.name == "sum") {
			return true;
		}
	}
	return false;
}

// Replace sum child with greatest(least(cast(v, DOUBLE), C), -C), cast back to original type
static void ClipSumInputs(OptimizerExtensionInput &input, LogicalAggregate *agg, double bound) {
	for (auto &expr : agg->expressions) {
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		if (aggr.function.name != "sum" || aggr.children.empty()) {
			continue;
		}
		auto original_type = aggr.children[0]->return_type;
		auto as_double =
		    BoundCastExpression::AddCastToType(input.context, std::move(aggr.children[0]), LogicalType::DOUBLE);
		auto upper_bound = make_uniq<BoundConstantExpression>(Value::DOUBLE(bound));
		auto lower_bound = make_uniq<BoundConstantExpression>(Value::DOUBLE(-bound));
		auto upper_clipped = input.optimizer.BindScalarFunction("least", std::move(as_double), std::move(upper_bound));
		auto clipped = input.optimizer.BindScalarFunction("greatest", std::move(upper_clipped), std::move(lower_bound));
		auto restored = BoundCastExpression::AddCastToType(input.context, std::move(clipped), original_type);
		aggr.children[0] = std::move(restored);
	}
	agg->ResolveOperatorTypes();
}

// Laplace scale = sensitivity / epsilon; sensitivity = es for COUNT, es * sum_bound for SUM
static double Sensitivity(const BoundAggregateExpression &aggr, double sum_bound, double es) {
	const string &name = aggr.function.name;
	if (name == "count" || name == "count_star") {
		return es;
	}
	if (name == "sum") {
		return es * sum_bound;
	}
	throw InternalException("dp_elastic: Sensitivity received unsupported '" + name + "'");
}

// ----------------------------------------------------------------------------
// Wrap aggregate output — inserts LogicalProjection above `agg` with
// dp_laplace_noise applied to each DP-target column, cast back to original type
// ----------------------------------------------------------------------------

static void WrapAggregateWithLaplace(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                     LogicalAggregate *agg, const vector<double> &agg_scales) {
	idx_t n_groups = agg->groups.size();
	idx_t n_aggs = agg->expressions.size();
	idx_t group_idx = agg->group_index;
	idx_t agg_idx = agg->aggregate_index;

	auto agg_types = agg->types;
	D_ASSERT(agg_types.size() == n_groups + n_aggs);

	vector<unique_ptr<Expression>> proj_exprs;
	proj_exprs.reserve(n_groups + n_aggs);

	for (idx_t gi = 0; gi < n_groups; gi++) {
		proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(agg_types[gi], ColumnBinding(group_idx, gi)));
	}

	for (idx_t ai = 0; ai < n_aggs; ai++) {
		auto agg_col_type = agg_types[n_groups + ai];
		auto col_ref = make_uniq<BoundColumnRefExpression>(agg_col_type, ColumnBinding(agg_idx, ai));
		double scale = agg_scales[ai];
		if (scale <= 0.0 || !std::isfinite(scale)) {
			proj_exprs.push_back(std::move(col_ref));
			continue;
		}
		unique_ptr<Expression> value_expr =
		    BoundCastExpression::AddCastToType(input.context, std::move(col_ref), LogicalType::DOUBLE);
		auto scale_expr = make_uniq<BoundConstantExpression>(Value::DOUBLE(scale));
		unique_ptr<Expression> noised =
		    input.optimizer.BindScalarFunction("dp_laplace_noise", std::move(value_expr), std::move(scale_expr));
		if (agg_col_type != LogicalType::DOUBLE) {
			noised = BoundCastExpression::AddCastToType(input.context, std::move(noised), agg_col_type);
		}
		proj_exprs.push_back(std::move(noised));
	}

	idx_t proj_idx = input.optimizer.binder.GenerateTableIndex();
	auto projection = make_uniq<LogicalProjection>(proj_idx, std::move(proj_exprs));

	auto *slot = FindSlotForOperator(plan, agg);
	if (!slot) {
		throw InternalException("dp_elastic: could not locate aggregate slot in plan");
	}
	auto old_agg = std::move(*slot);
	projection->children.push_back(std::move(old_agg));
	projection->ResolveOperatorTypes();
	LogicalOperator *proj_ptr = projection.get();
	*slot = std::move(projection);

	// Remap bindings above the inserted projection
	ColumnBindingReplacer replacer;
	for (idx_t gi = 0; gi < n_groups; gi++) {
		replacer.replacement_bindings.emplace_back(ColumnBinding(group_idx, gi), ColumnBinding(proj_idx, gi));
	}
	for (idx_t ai = 0; ai < n_aggs; ai++) {
		replacer.replacement_bindings.emplace_back(ColumnBinding(agg_idx, ai), ColumnBinding(proj_idx, n_groups + ai));
	}
	replacer.stop_operator = proj_ptr;
	replacer.VisitOperator(*plan);
}

// ----------------------------------------------------------------------------
// Entry point
// ----------------------------------------------------------------------------

void CompileDPElasticQuery(const PrivacyCompatibilityResult &check, OptimizerExtensionInput &input,
                           unique_ptr<LogicalOperator> &plan, const vector<string> &privacy_units,
                           const string &query_hash) {
	(void)query_hash;
	PRIVACY_DEBUG_PRINT("[DP_ELASTIC] CompileDPElasticQuery: start");

	double epsilon = GetDpEpsilon(input.context, 1.0);
	if (epsilon <= 0.0 || !std::isfinite(epsilon)) {
		throw InvalidInputException("dp_elastic: dp_epsilon must be a positive finite number (got " +
		                            std::to_string(epsilon) + ")");
	}

	plan->ResolveOperatorTypes();

	auto eligibility = CheckDPEligibility(plan, privacy_units, check);
	auto *agg = eligibility.top_agg;

	double sum_bound = 0.0;
	bool has_sum = AggregateContainsSum(agg);
	if (has_sum) {
		if (!TryGetDpSumBound(input.context, sum_bound)) {
			throw InvalidInputException(
			    "dp_elastic: dp_sum_bound must be set for SUM aggregates (SET dp_sum_bound = <C>)");
		}
		if (sum_bound <= 0.0 || !std::isfinite(sum_bound)) {
			throw InvalidInputException("dp_elastic: dp_sum_bound must be a positive finite number (got " +
			                            std::to_string(sum_bound) + ")");
		}
		ClipSumInputs(input, agg, sum_bound);
	}

	// Compute elastic sensitivity (global or smooth depending on dp_delta setting)
	double es = ComputeElasticSensitivity(input.context, eligibility.fk_chain, epsilon);

	// When privacy_noise=false the compilation pipeline still runs (clipping, FK chain)
	// but Laplace scale is zeroed → dp_laplace_noise returns the value unchanged.
	// This mirrors pac_mi=0 for PAC and enables deterministic testing.
	bool noise_enabled = IsPacNoiseEnabled(input.context, true);

	vector<double> agg_scales;
	agg_scales.reserve(agg->expressions.size());
	for (auto &expr : agg->expressions) {
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		double sens = Sensitivity(aggr, sum_bound, es);
		double scale = noise_enabled ? (sens / epsilon) : 0.0;
		agg_scales.push_back(scale);
		PRIVACY_DEBUG_PRINT("[DP_ELASTIC] agg '" + aggr.function.name + "' sensitivity=" + std::to_string(sens) +
		                    " scale=" + std::to_string(scale));
	}

	WrapAggregateWithLaplace(input, plan, agg, agg_scales);

#if PRIVACY_DEBUG
	PRIVACY_DEBUG_PRINT("=== PLAN AFTER DP_ELASTIC TRANSFORMATION ===");
	plan->Print();
#endif
}

} // namespace duckdb
