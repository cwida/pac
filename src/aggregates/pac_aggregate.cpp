#include "aggregates/pac_aggregate.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
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
// is_null: bitmask where bit i=1 means counter i should be excluded (compacted out)
// mi: mutual information parameter for noise calculation
// correction: factor to multiply values by after compacting but before noising
// pstate: optional p-tracking state for persistent secret composition (Bayesian posterior over worlds)
// Returns: correction*yJ + noise where yJ is a randomly selected counter
PAC_FLOAT PacNoisySampleFrom64Counters(const PAC_FLOAT counters[64], double mi, double correction, std::mt19937_64 &gen,
                                       uint64_t is_null, uint64_t counter_selector,
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

	// Pick counter index J in [0, 63] deterministically from counter_selector.
	int J = static_cast<int>(counter_selector % 64);
	PAC_FLOAT yJ = vals[J];

	// ---- P-tracking path: use p-weighted variance and Bayesian update ----
	if (pstate) {
		std::lock_guard<std::mutex> lock(pstate->mtx);

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
			return yJ;
		}

		std::normal_distribution<double> normal_dist(0.0, std::sqrt(noise_var));
		double noisy_result = static_cast<double>(yJ) + normal_dist(gen);

		// Bayesian posterior update: log_p[k] = log(p[k]) + log_likelihood[k], then log-sum-exp normalize
		double log_p[64];
		double max_log = -std::numeric_limits<double>::infinity();
		for (int k = 0; k < 64; k++) {
			double diff = static_cast<double>(vals[k]) - noisy_result;
			log_p[k] = std::log(pstate->p[k] + 1e-300) + (-0.5 * diff * diff / noise_var);
			if (log_p[k] > max_log) {
				max_log = log_p[k];
			}
		}
		double sum_exp = 0.0;
		for (int k = 0; k < 64; k++) {
			sum_exp += std::exp(log_p[k] - max_log);
		}
		double log_sum = max_log + std::log(sum_exp);
		for (int k = 0; k < 64; k++) {
			pstate->p[k] = std::exp(log_p[k] - log_sum);
		}

		return static_cast<PAC_FLOAT>(noisy_result);
	}

	// ---- Uniform path (no p-tracking) ----
	double delta = ComputeDeltaFromValues(vals, mi);

	if (delta <= 0.0 || !std::isfinite(delta)) {
		return yJ;
	}

	std::normal_distribution<double> normal_dist(0.0, std::sqrt(delta));
	return static_cast<PAC_FLOAT>(static_cast<double>(yJ) + normal_dist(gen));
}

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
	CreateScalarFunctionInfo info(pac_hash);
	FunctionDescription desc;
	desc.description = "[INTERNAL] Hashes a privacy unit key for PAC subsample assignment.";
	info.descriptions.push_back(std::move(desc));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
