#include "aggregates/pac_aggregate.hpp"

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <random>
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif
#include <cmath>
#include <cstring>
#include <type_traits>
#include <limits>

// Every argument to pac_aggregate is the output of a query evaluated on a random subsample of the privacy unit

namespace duckdb {

// ============================================================================
// Global PacPState map for cross-aggregate p-tracking within a query
// ============================================================================
static std::mutex g_pstate_map_mutex; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
static std::unordered_map<uint64_t, std::weak_ptr<PacPState>>
    g_pstate_map; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

std::shared_ptr<PacPState> GetOrCreatePState(uint64_t query_hash) {
	std::lock_guard<std::mutex> lock(g_pstate_map_mutex);
	auto it = g_pstate_map.find(query_hash);
	if (it != g_pstate_map.end()) {
		auto sp = it->second.lock();
		if (sp) {
			return sp;
		}
	}
	auto sp = std::make_shared<PacPState>();
	g_pstate_map[query_hash] = sp;
	// Lazily clean up expired entries (cheap: just scan for expired weak_ptrs)
	for (auto iter = g_pstate_map.begin(); iter != g_pstate_map.end();) {
		if (iter->second.expired()) {
			iter = g_pstate_map.erase(iter);
		} else {
			++iter;
		}
	}
	return sp;
}

// Helper function to generate a random seed - implementation
uint64_t PacGenerateRandomSeed() {
	return std::random_device {}();
}

// Check for absence of sample diversity in PAC aggregates.
// Accumulates per-group stats into bind_data, then checks the population-level condition.
void CheckPacSampleDiversity(uint64_t key_hash, const PAC_FLOAT *buf, uint64_t update_count, const char *aggr_name,
                             PacBindData &bind_data) {
	bind_data.total_update_count += update_count;

	// Classify this group as suspicious or not
	bool suspicious = false;
	int zero_bits = 64 - pac_popcount64(key_hash);
	if (zero_bits >= 29 && zero_bits <= 35) {
		// Check if all active counters have the same value
		bool found_first = false;
		PAC_FLOAT first_val = 0;
		suspicious = true;
		for (int j = 0; j < 64; j++) {
			if ((key_hash >> j) & 1ULL) {
				if (!found_first) {
					first_val = buf[j];
					found_first = true;
				} else if (buf[j] != first_val) {
					suspicious = false;
					break;
				}
			}
		}
		if (!found_first) {
			suspicious = false;
		}
	}

	if (suspicious) {
		bind_data.suspicious_count++;
	} else {
		bind_data.nonsuspicious_count++;
	}

	// Check population-level condition
	if (bind_data.total_update_count >= PAC_SUSPICIOUS_THRESHOLD &&
	    bind_data.suspicious_count > 2 * bind_data.nonsuspicious_count) {
		throw InvalidInputException("%s detected absence of sample diversity -- which clearly is privacy unsafe",
		                            aggr_name);
	}
}

// ============================================================================
// NOTE: pac_count implementation was moved to src/pac_count.cpp / src/include/pac_count.hpp.
// The noisy-sample computation used by multiple aggregates is kept here and exported.
// ============================================================================

// Forward declaration for the internal variance helper so it can be used above
static double ComputeSecondMomentVariance(const vector<PAC_FLOAT> &values);

// Deterministic sampling helpers (use engine() bits directly so draws are identical across platforms
// for a given mt19937_64 seed). Function names follow repository style (no snake_case).
static inline uint64_t EngineNext(std::mt19937_64 &gen) {
	return gen();
}

// DeterministicUniformUnit: produce a uniform double in [0,1) using 53 bits from engine
static inline double DeterministicUniformUnit(std::mt19937_64 &gen) {
	uint64_t r = EngineNext(gen);
	uint64_t top53 = r >> 11;                              // keep top 53 bits
	constexpr double inv2pow53 = 1.0 / 9007199254740992.0; // 1 / 2^53
	return static_cast<double>(top53) * inv2pow53;
}

// DeterministicNormalSample: Box–Muller sampling using DeterministicUniformUnit
static inline double DeterministicNormalSample(std::mt19937_64 &gen, bool &has_spare, double &spare, double mean,
                                               double stddev) {
	if (has_spare) {
		has_spare = false;
		return mean + spare * stddev;
	}
	double u1 = DeterministicUniformUnit(gen);
	double u2 = DeterministicUniformUnit(gen);
	if (u1 <= 0.0) {
		u1 = std::numeric_limits<double>::min();
	}
	double r = std::sqrt(-2.0 * std::log(u1));
	double theta = 2.0 * M_PI * u2;
	double z0 = r * std::cos(theta);
	double z1 = r * std::sin(theta);
	spare = z1;
	has_spare = true;
	return mean + z0 * stddev;
}

// PacNoiseInNull: probabilistically returns true based on bit count in key_hash
// mi: controls probabilistic (mi>0) vs deterministic (mi<=0) mode
// correction: reduces NULL probability by this factor (considers correction times more non-nulls)
bool PacNoiseInNull(uint64_t key_hash, double mi, double correction, std::mt19937_64 &gen) {
	int popcount = pac_popcount64(key_hash);
	// Effective popcount considering correction factor (correction times more non-nulls)
	double effective_popcount = popcount * correction;

	if (mi <= 0.0) {
		// Deterministic mode: NULL when effective_popcount < 1
		return effective_popcount < 1.0;
	}
	// Probabilistic mode: P(NULL) = popcount(~key_hash) / (64 * correction)
	// Equivalently: NULL if popcount(~key_hash) > threshold, where threshold is in [0, 64*correction)
	uint64_t range = static_cast<uint64_t>(64.0 * correction);
	if (range == 0) {
		range = 1;
	}
	int threshold = static_cast<int>(gen() % range);
	return pac_popcount64(~key_hash) > threshold;
}

// Finalize: compute noisy sample from the 64 counters (works on double array)
// If use_deterministic_noise is true, uses platform-agnostic Box-Muller; otherwise uses std::normal_distribution
// is_null: bitmask where bit i=1 means counter i should be excluded (compacted out)
// mi: mutual information parameter for noise calculation
// correction: factor to multiply values by after compacting but before noising
// pstate: optional p-tracking state for persistent secret composition (Bayesian posterior over worlds)
// Returns: correction*yJ + noise where yJ is a randomly selected counter
PAC_FLOAT PacNoisySampleFrom64Counters(const PAC_FLOAT counters[64], double mi, double correction, std::mt19937_64 &gen,
                                       bool use_deterministic_noise, uint64_t is_null, uint64_t counter_selector,
                                       const std::shared_ptr<PacPState> &pstate) {
	D_ASSERT(~is_null != 0); // at least one bit must be valid

	// The vals array will always have 64 elements. If a counter is NULL, we push 0.
	vector<PAC_FLOAT> vals;
	vals.reserve(64);
	for (int i = 0; i < 64; i++) {
		vals.push_back(((is_null >> i) & 1) ? 0 : counters[i]);
	}

	// Apply correction factor to all values (after compacting NULLs, before noising)
	for (auto &v : vals) {
		v *= static_cast<PAC_FLOAT>(correction);
	}

	// mi <= 0 means no noise - return counter[0] directly (no RNG consumption)
	if (mi <= 0.0) {
		return vals[0];
	}
	int N = static_cast<int>(vals.size()); // N is always 64

	// Pick counter index J in [0, N-1] deterministically from counter_selector.
	int J = static_cast<int>(counter_selector % static_cast<uint64_t>(N));
	PAC_FLOAT yJ = vals[J];

	// ---- P-tracking path: use p-weighted variance and Bayesian update ----
	if (pstate) {
		std::lock_guard<std::mutex> lock(pstate->mtx);

		// get_noise_var(p, vals, b): p-weighted mean and variance over all 64 worlds.
		double w_mean = 0.0;
		for (int k = 0; k < 64; k++) {
			w_mean += static_cast<double>(vals[k]) * pstate->p[k];
		}

		double w_var = 0.0;
		for (int k = 0; k < 64; k++) {
			double d = static_cast<double>(vals[k]) - w_mean;
			w_var += d * d * pstate->p[k];
		}

		double noise_var = w_var / (2.0 * mi);

		if (noise_var <= 0.0 || !std::isfinite(noise_var)) {
			return yJ; // no variance — no noise needed
		}

		// Sample noise
		double noise;
		if (use_deterministic_noise) {
			double u1 = DeterministicUniformUnit(gen);
			double u2 = DeterministicUniformUnit(gen);
			if (u1 <= 0.0) {
				u1 = std::numeric_limits<double>::min();
			}
			double r = std::sqrt(-2.0 * std::log(u1));
			double theta = 2.0 * M_PI * u2;
			double z0 = r * std::cos(theta);
			noise = z0 * std::sqrt(noise_var);
		} else {
			std::normal_distribution<double> normal_dist(0.0, std::sqrt(noise_var));
			noise = normal_dist(gen);
		}
		double noisy_result = static_cast<double>(yJ) + noise;

		// update_p(p, vals, noisy_result, noise_var): Bayesian posterior update
		// log_p[k] = log(p[k]) + log_likelihood[k], then log-sum-exp normalize
		double log_p[64];
		double max_log = -std::numeric_limits<double>::infinity();
		for (int k = 0; k < 64; k++) {
			double diff = static_cast<double>(vals[k]) - noisy_result;
			log_p[k] = std::log(pstate->p[k] + 1e-300) + (-0.5 * diff * diff / noise_var);
			if (log_p[k] > max_log) {
				max_log = log_p[k];
			}
		}
		// Log-sum-exp normalization
		double sum_exp = 0.0;
		for (int k = 0; k < 64; k++) {
			sum_exp += std::exp(log_p[k] - max_log);
		}
		double log_sum = max_log + std::log(sum_exp);

		// Write updated p back to pstate
		for (int k = 0; k < 64; k++) {
			pstate->p[k] = std::exp(log_p[k] - log_sum);
		}

		return static_cast<PAC_FLOAT>(noisy_result);
	}

uniform_path:
	// ---- Original uniform path (no p-tracking) ----
	// Compute delta using the shared exported helper (uses mi for noise variance)
	double delta = ComputeDeltaFromValues(vals, mi);

	if (delta <= 0.0 || !std::isfinite(delta)) {
		// If there's no variance, return the selected counter value without noise.
		return yJ;
	}

	// Sample normal(0, sqrt(delta)) using either deterministic Box-Muller or std::normal_distribution
	double noise;
	if (use_deterministic_noise) {
		// Platform-agnostic deterministic Box-Muller
		double u1 = DeterministicUniformUnit(gen);
		double u2 = DeterministicUniformUnit(gen);
		if (u1 <= 0.0) {
			u1 = std::numeric_limits<double>::min();
		}
		double r = std::sqrt(-2.0 * std::log(u1));
		double theta = 2.0 * M_PI * u2;
		double z0 = r * std::cos(theta);
		noise = z0 * std::sqrt(delta);
	} else {
		// Platform-specific std::normal_distribution (faster but not deterministic across platforms)
		std::normal_distribution<double> normal_dist(0.0, std::sqrt(delta));
		noise = normal_dist(gen);
	}
	return static_cast<PAC_FLOAT>(static_cast<double>(yJ) + noise);
}

struct PacAggregateLocalState : public FunctionLocalState {
	explicit PacAggregateLocalState(uint64_t seed) : gen(seed), has_spare(false), spare(0.0) {
	}
	std::mt19937_64 gen;
	bool has_spare;
	double spare;
};

// Compute second-moment variance (not unbiased estimator)
// Internal math stays double for numerical stability
static double ComputeSecondMomentVariance(const vector<PAC_FLOAT> &values) {
	idx_t n = values.size();
	if (n <= 1) {
		return 0.0;
	}

	double mean = 0.0;
	for (auto v : values) {
		mean += static_cast<double>(v);
	}
	mean /= static_cast<double>(n);

	double var = 0.0;
	for (auto v : values) {
		double d = static_cast<double>(v) - mean;
		var += d * d;
	}
	// Use sample variance (divide by n-1) to make the estimator unbiased for finite samples
	return var / static_cast<double>(n - 1);
}

// Exported helper: compute the PAC noise variance (delta) from values and mi.
// This implements the header-declared ComputeDeltaFromValues and is reused by other files.
// Internal math stays double for numerical stability.
double ComputeDeltaFromValues(const vector<PAC_FLOAT> &values, double mi) {
	if (mi < 0.0) {
		throw InvalidInputException("ComputeDeltaFromValues: mi must be >= 0");
	}
	double sigma2 = ComputeSecondMomentVariance(values);
	double delta = sigma2 / (2.0 * mi);
	return delta;
}

unique_ptr<FunctionLocalState> PacAggregateInit(ExpressionState &state, const BoundFunctionExpression &,
                                                FunctionData *bind_data) {
	// Single source-of-truth: prefer seed supplied via bind_data (PacBindData). Do NOT read the session
	// setting here. If bind_data is not provided, fall back to non-deterministic random_device.
	uint64_t seed = std::random_device {}();
	if (bind_data) {
		seed = bind_data->Cast<PacBindData>().seed;
	}
	return make_uniq<PacAggregateLocalState>(seed);
}

// Templated implementation that reads list entries of arbitrary numeric type T for values
// and arbitrary integer-like type C for counts. We use the name PacAggregateScalar as the
// template so callers can instantiate PacAggregateScalar<T,C> directly.
template <class T, class C>
static void PacAggregateScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &vals = args.data[0];
	auto &cnts = args.data[1];
	auto &mi_vec = args.data[2];
	auto &k_vec = args.data[3];

	idx_t count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto res = FlatVector::GetData<double>(result);
	FlatVector::Validity(result).SetAllValid(count);

	auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<PacAggregateLocalState>();
	auto &gen = local.gen;

	// Get query_hash and pstate from bind data for deterministic counter selection and p-tracking
	uint64_t query_hash = 0;
	std::shared_ptr<PacPState> pstate;
	auto &bound_expr = state.expr.Cast<BoundFunctionExpression>();
	if (bound_expr.bind_info) {
		auto &bd = bound_expr.bind_info->Cast<PacBindData>();
		query_hash = bd.query_hash;
		pstate = bd.pstate;
	}

	// --- extract lists ---
	UnifiedVectorFormat vvals, vcnts;
	vals.ToUnifiedFormat(count, vvals);
	cnts.ToUnifiedFormat(count, vcnts);

	// Prepare unified format for k (integer per-row parameter) once per chunk
	UnifiedVectorFormat kvals;
	k_vec.ToUnifiedFormat(count, kvals);
	auto *kdata = UnifiedVectorFormat::GetData<int32_t>(kvals);

	for (idx_t row = 0; row < count; row++) {
		bool refuse = false;

		// --- read mi, k ---
		double mi = mi_vec.GetValue(row).GetValue<double>();
		if (mi < 0.0) {
			throw InvalidInputException("pac_aggregate: mi must be >= 0");
		}
		idx_t kidx = kvals.sel ? kvals.sel->get_index(row) : row;
		int k = kdata[kidx];

		idx_t r = vvals.sel ? vvals.sel->get_index(row) : row;
		if (!vvals.validity.RowIsValid(r) || !vcnts.validity.RowIsValid(r)) {
			result.SetValue(row, Value());
			continue;
		}

		auto *vals_entries = UnifiedVectorFormat::GetData<list_entry_t>(vvals);
		auto *cnts_entries = UnifiedVectorFormat::GetData<list_entry_t>(vcnts);

		auto ve = vals_entries[r];
		auto ce = cnts_entries[r];

		// Values and counts arrays must have the same length (one count per sample position).
		if (ve.length != ce.length) {
			throw InvalidInputException("pac_aggregate: values and counts length mismatch");
		}
		idx_t vals_len = ve.length;
		idx_t cnts_len = ce.length;

		auto &vals_child = ListVector::GetEntry(vals);
		auto &cnts_child = ListVector::GetEntry(cnts);
		vals_child.Flatten(ve.offset + ve.length);
		cnts_child.Flatten(ce.offset + ce.length);

		auto *vdata = FlatVector::GetData<T>(vals_child);
		auto *cdata = FlatVector::GetData<C>(cnts_child);

		vector<PAC_FLOAT> values;
		values.reserve(vals_len);

		int64_t max_count = 0;
		for (idx_t i = 0; i < vals_len; i++) {
			if (!FlatVector::Validity(vals_child).RowIsValid(ve.offset + i)) {
				refuse = true;
				break;
			}
			values.push_back(ToDouble<T>(vdata[ve.offset + i]));
			int64_t cnt_val = 0;
			if (i < cnts_len) {
				// Cast counts to int64 for the max_count check so BIGINT[] counts don't truncate.
				cnt_val = static_cast<int64_t>(cdata[ce.offset + i]);
			}
			max_count = std::max<int64_t>(max_count, cnt_val);
		}

		// Because we need exactly 64 worlds for p-tracking and variance, pad values up to 64 with 0.
		while (values.size() < 64) {
			values.push_back(0.0);
		}

		if (refuse || values.empty() || max_count < static_cast<int64_t>(k)) {
			result.SetValue(row, Value());
			continue;
		}

		// ---------------- PAC core ----------------

		// mi <= 0 means no noise - always return counter[0] directly
		if (mi <= 0.0) {
			res[row] = values[0];
			continue;
		}

		idx_t N = values.size();
		// 1. pick J deterministically from query_hash (same within a query)
		idx_t J = static_cast<idx_t>(query_hash % static_cast<uint64_t>(N));
		PAC_FLOAT yJ = values[J];

		// P-tracking path for pac_aggregate scalar (only when list fits in 64 worlds)
		if (pstate && N == 64) {
			std::lock_guard<std::mutex> lock(pstate->mtx);
			// pac_aggregate uses indices 0..63 directly as world indices for p-tracking.
			double w_mean = 0.0;
			for (idx_t k = 0; k < 64; k++) {
				w_mean += static_cast<double>(values[k]) * pstate->p[k];
			}
			double w_var = 0.0;
			for (idx_t k = 0; k < 64; k++) {
				double d = static_cast<double>(values[k]) - w_mean;
				w_var += d * d * pstate->p[k];
			}
			double noise_var = w_var / (2.0 * mi);
			if (noise_var > 0.0 && std::isfinite(noise_var)) {
				double noise = DeterministicNormalSample(gen, local.has_spare, local.spare, 0.0, std::sqrt(noise_var));
				double noisy_result = static_cast<double>(yJ) + noise;
				// Bayesian update
				double log_p[64];
				double max_log = -std::numeric_limits<double>::infinity();
				for (idx_t k = 0; k < 64; k++) {
					double diff = static_cast<double>(values[k]) - noisy_result;
					log_p[k] = std::log(pstate->p[k] + 1e-300) + (-0.5 * diff * diff / noise_var);
					if (log_p[k] > max_log) {
						max_log = log_p[k];
					}
				}
				double sum_exp = 0.0;
				for (idx_t k = 0; k < 64; k++) {
					sum_exp += std::exp(log_p[k] - max_log);
				}
				double log_sum = max_log + std::log(sum_exp);
				for (idx_t k = 0; k < 64; k++) {
					pstate->p[k] = std::exp(log_p[k] - log_sum);
				}
				res[row] = noisy_result;
				continue;
			}
			// Fall through to uniform path if no variance
		}

		// 2. empirical (second-moment) variance over the full list
		double delta = ComputeDeltaFromValues(values, mi);

		if (delta <= 0.0 || !std::isfinite(delta)) {
			res[row] = yJ;
			continue;
		}

		// 3. noise
		res[row] = yJ + DeterministicNormalSample(gen, local.has_spare, local.spare, 0.0, std::sqrt(delta));
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> PacAggregateBind(ClientContext &ctx, ScalarFunction &,
                                                 vector<unique_ptr<Expression>> &args) {
	// mi is per-row for pac_aggregate; store default mi in bind data but it's unused here
	auto result = make_uniq<PacBindData>(ctx, 128.0);
	return result;
}

void RegisterPacAggregateFunctions(ExtensionLoader &loader) {
	// Helper that registers one overload given the element logical types and a function pointer
	auto make_and_register = [&](const LogicalType &vals_elem, const LogicalType &cnts_elem,
	                             const scalar_function_t &fn) {
		auto fun = ScalarFunction(
		    "pac_aggregate",
		    {LogicalType::LIST(vals_elem), LogicalType::LIST(cnts_elem), LogicalType::DOUBLE, LogicalType::INTEGER},
		    LogicalType::DOUBLE, fn, PacAggregateBind, nullptr, nullptr, PacAggregateInit);
		fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
		loader.RegisterFunction(fun);
	};

	// For each values element type we register overloads where the counts element type can be any integer-like type.
	// This addresses cases like BIGINT[] counts (the user's error).
#define REG_COUNTS_FOR(V_LOG, V_CPP)                                                                                   \
	make_and_register(V_LOG, LogicalType::TINYINT, PacAggregateScalar<V_CPP, int8_t>);                                 \
	make_and_register(V_LOG, LogicalType::SMALLINT, PacAggregateScalar<V_CPP, int16_t>);                               \
	make_and_register(V_LOG, LogicalType::INTEGER, PacAggregateScalar<V_CPP, int32_t>);                                \
	make_and_register(V_LOG, LogicalType::BIGINT, PacAggregateScalar<V_CPP, int64_t>);                                 \
	make_and_register(V_LOG, LogicalType::HUGEINT, PacAggregateScalar<V_CPP, hugeint_t>);                              \
	make_and_register(V_LOG, LogicalType::UTINYINT, PacAggregateScalar<V_CPP, uint8_t>);                               \
	make_and_register(V_LOG, LogicalType::USMALLINT, PacAggregateScalar<V_CPP, uint16_t>);                             \
	make_and_register(V_LOG, LogicalType::UINTEGER, PacAggregateScalar<V_CPP, uint32_t>);                              \
	make_and_register(V_LOG, LogicalType::UBIGINT, PacAggregateScalar<V_CPP, uint64_t>);                               \
	make_and_register(V_LOG, LogicalType::UHUGEINT, PacAggregateScalar<V_CPP, uhugeint_t>);                            \
	/* Also accept floating-point counts arrays (users may accidentally pass DOUBLE[]/FLOAT[]) */                      \
	make_and_register(V_LOG, LogicalType::FLOAT, PacAggregateScalar<V_CPP, float>);                                    \
	make_and_register(V_LOG, LogicalType::DOUBLE, PacAggregateScalar<V_CPP, double>);

	// Register for all common value element types (including floats)
	REG_COUNTS_FOR(LogicalType::BOOLEAN, int8_t)
	REG_COUNTS_FOR(LogicalType::TINYINT, int8_t)
	REG_COUNTS_FOR(LogicalType::SMALLINT, int16_t)
	REG_COUNTS_FOR(LogicalType::INTEGER, int32_t)
	REG_COUNTS_FOR(LogicalType::BIGINT, int64_t)
	// HUGEINT values
	REG_COUNTS_FOR(LogicalType::HUGEINT, hugeint_t)
	// Unsigned values
	REG_COUNTS_FOR(LogicalType::UTINYINT, uint8_t)
	REG_COUNTS_FOR(LogicalType::USMALLINT, uint16_t)
	REG_COUNTS_FOR(LogicalType::UINTEGER, uint32_t)
	REG_COUNTS_FOR(LogicalType::UBIGINT, uint64_t)
	REG_COUNTS_FOR(LogicalType::UHUGEINT, uhugeint_t)
	// Floating point values: counts are still integer-like
	REG_COUNTS_FOR(LogicalType::FLOAT, float)
	REG_COUNTS_FOR(LogicalType::DOUBLE, double)

#undef REG_COUNTS_FOR
}

// ============================================================================
// pac_hash: UBIGINT -> UBIGINT with exactly 32 bits set
// ============================================================================

// 64-bit prime with exactly 32 bits set (irregular pattern), used for hashing in hash32_32
#define PAC_HASH_PRIME 0xB2833106536E95DFULL

static uint64_t hash32_32(uint64_t num) {
	for (int round = 0; round < 16; ++round) {
		uint64_t next = PAC_HASH_PRIME * (num ^ PAC_HASH_PRIME);
		uint64_t flip = static_cast<uint64_t>(static_cast<int64_t>(32 - pac_popcount64(num)) >> 63);
		num ^= flip; // conditionally negate without branching
		int pop = pac_popcount64(num);
		if (pop >= 26) {
			for (int iter = 0; iter < 10; ++iter) {
				if (pop == 32) {
					return num;
				}
				uint64_t mask = 1ULL << ((next >> (6 * iter)) & 63);
				if ((num & mask) == 0) {
					num |= mask;
					++pop;
				}
			}
		}
		num = next; // fallback to next hash if repair fails
	}
	return 0xAAAAAAAAAAAAAAAAULL; // 1010...10 pattern: exactly 32 bits set
}

static unique_ptr<FunctionData> PacHashBind(ClientContext &ctx, ScalarFunction &, vector<unique_ptr<Expression>> &) {
	double mi = GetPacMiFromSetting(ctx);
	bool hash_repair = true;
	Value hr_val;
	if (ctx.TryGetCurrentSetting("pac_hash_repair", hr_val) && !hr_val.IsNull()) {
		hash_repair = hr_val.GetValue<bool>();
	}
	return make_uniq<PacBindData>(ctx, mi, 1.0, 1.0, hash_repair);
}

static void PacHashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto count = args.size();
	auto &function = state.expr.Cast<BoundFunctionExpression>();
	uint64_t query_hash = 0;
	bool hash_repair = true;
	if (function.bind_info) {
		auto &bind_data = function.bind_info->Cast<PacBindData>();
		query_hash = bind_data.query_hash;
		hash_repair = bind_data.hash_repair;
	}
	UnaryExecutor::Execute<uint64_t, uint64_t>(input, result, count, [query_hash, hash_repair](uint64_t val) {
		uint64_t xored = val ^ query_hash;
		return hash_repair ? hash32_32(xored) : xored;
	});
}

void RegisterPacHashFunction(ExtensionLoader &loader) {
	ScalarFunction pac_hash("pac_hash", {LogicalType::UBIGINT}, LogicalType::UBIGINT, PacHashFunction, PacHashBind);
	loader.RegisterFunction(pac_hash);
}

} // namespace duckdb
