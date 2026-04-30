#include "aggregates/dp_laplace_noise.hpp"
#include "aggregates/pac_aggregate.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <cmath>
#include <random>

namespace duckdb {

static void DpLaplaceNoiseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	Value seed_val;
	uint64_t seed = (context.TryGetCurrentSetting("privacy_seed", seed_val) && !seed_val.IsNull())
	                    ? uint64_t(seed_val.GetValue<int64_t>())
	                    : uint64_t(std::random_device {}());
	// If a seed was explicitly set, keep the run deterministic; otherwise vary per active query.
	if (!seed_val.IsNull()) {
		// keep deterministic
	} else {
		seed ^= PAC_MAGIC_HASH * static_cast<uint64_t>(context.ActiveTransaction().GetActiveQuery());
	}

	auto count = args.size();
	BinaryExecutor::Execute<double, double, double>(
	    args.data[0], args.data[1], result, count, [&](double value, double scale) -> double {
		    if (scale <= 0.0 || !std::isfinite(scale)) {
			    return value;
		    }
		    // Per-row seed mixing — stable across vectorized batches as long as inputs repeat.
		    std::mt19937_64 gen(seed ^ (PAC_MAGIC_HASH * static_cast<uint64_t>(std::hash<double> {}(value) ^
		                                                                       std::hash<double> {}(scale))));
		    std::uniform_real_distribution<double> uni(-0.5, 0.5);
		    double u = uni(gen);
		    // Inverse CDF of Laplace(0, scale): -scale * sgn(u) * ln(1 - 2|u|)
		    double sign = (u < 0.0) ? -1.0 : 1.0;
		    double noise = -scale * sign * std::log(std::max(1e-300, 1.0 - 2.0 * std::abs(u)));
		    return value + noise;
	    });
}

void RegisterDpLaplaceNoiseFunction(ExtensionLoader &loader) {
	ScalarFunction fn("dp_laplace_noise", {LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                  DpLaplaceNoiseFunction);
	CreateScalarFunctionInfo info(fn);
	FunctionDescription desc;
	desc.description = "Adds Laplace(0, scale) noise to a value for elastic-sensitivity DP.";
	info.descriptions.push_back(std::move(desc));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
