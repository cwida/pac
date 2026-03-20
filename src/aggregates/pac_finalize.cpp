#include "aggregates/pac_aggregate.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/function/cast/default_casts.hpp"

#include <random>

namespace duckdb {

// ============================================================================
// pac_finalize(LIST<FLOAT>) -> FLOAT
// Takes 64 subsample counters and returns a noised scalar value.
// Counters already include 2x correction from pac_sum/pac_count finalize.
// ============================================================================
static void PacFinalizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	double mi = GetPacMiFromSetting(context);

	Value pac_seed_val;
	uint64_t seed = (context.TryGetCurrentSetting("pac_seed", pac_seed_val) && !pac_seed_val.IsNull())
	                    ? uint64_t(pac_seed_val.GetValue<int64_t>())
	                    : 42;
	if (mi != 0.0) {
		seed ^= PAC_MAGIC_HASH * static_cast<uint64_t>(context.ActiveTransaction().GetActiveQuery());
	}
	uint64_t query_hash = (seed * PAC_MAGIC_HASH) ^ PAC_MAGIC_HASH;

	auto &list_vec = args.data[0];
	idx_t count = args.size();

	UnifiedVectorFormat list_data;
	list_vec.ToUnifiedFormat(count, list_data);

	auto result_data = FlatVector::GetData<PAC_FLOAT>(result);
	auto &result_validity = FlatVector::Validity(result);

	auto &child_vec = ListVector::GetEntry(list_vec);
	UnifiedVectorFormat child_data;
	child_vec.ToUnifiedFormat(ListVector::GetListSize(list_vec), child_data);
	auto child_values = UnifiedVectorFormat::GetData<PAC_FLOAT>(child_data);

	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);

	for (idx_t i = 0; i < count; i++) {
		auto list_idx = list_data.sel->get_index(i);

		if (!list_data.validity.RowIsValid(list_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto &entry = list_entries[list_idx];
		if (entry.length != 64) {
			result_validity.SetInvalid(i);
			continue;
		}

		PAC_FLOAT counters[64];
		for (idx_t j = 0; j < 64; j++) {
			auto child_idx = child_data.sel->get_index(entry.offset + j);
			counters[j] = child_data.validity.RowIsValid(child_idx) ? child_values[child_idx] : 0;
		}

		// RNG seeded per row using seed + list offset (stable across DataChunk batches)
		std::mt19937_64 gen(seed ^ (entry.offset * PAC_MAGIC_HASH));

		result_data[i] = PacNoisySampleFrom64Counters(counters, mi, 1.0, gen, 0ULL, query_hash, nullptr);
	}
}

void RegisterPacFinalizeFunction(ExtensionLoader &loader) {
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	auto scalar_type = PacFloatLogicalType();
	ScalarFunction pac_finalize("pac_finalize", {list_type}, scalar_type, PacFinalizeFunction);
	loader.RegisterFunction(pac_finalize);

	// Register implicit casts LIST<FLOAT> → FLOAT and LIST<FLOAT> → DOUBLE so the binder
	// can resolve comparisons (WHERE total > 100) and function calls (SUM(total)) on
	// derived_pu counter columns. The cast performs pac_finalize at execution time.
	auto pac_finalize_cast = [](Vector &source, Vector &result, idx_t count, CastParameters &parameters) -> bool {
		double mi = 0.0;
		uint64_t seed = 42;
		uint64_t query_hash = (seed * PAC_MAGIC_HASH) ^ PAC_MAGIC_HASH;

		UnifiedVectorFormat list_data;
		source.ToUnifiedFormat(count, list_data);

		auto &child_vec = ListVector::GetEntry(source);
		UnifiedVectorFormat child_data;
		child_vec.ToUnifiedFormat(ListVector::GetListSize(source), child_data);
		auto child_values = UnifiedVectorFormat::GetData<PAC_FLOAT>(child_data);
		auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);

		// Write to a PAC_FLOAT temp buffer, then cast to the result type if needed
		vector<PAC_FLOAT> tmp(count);
		auto &result_validity = FlatVector::Validity(result);

		for (idx_t i = 0; i < count; i++) {
			auto list_idx = list_data.sel->get_index(i);
			if (!list_data.validity.RowIsValid(list_idx)) {
				result_validity.SetInvalid(i);
				continue;
			}
			auto &entry = list_entries[list_idx];
			if (entry.length != 64) {
				result_validity.SetInvalid(i);
				continue;
			}
			PAC_FLOAT counters[64];
			for (idx_t j = 0; j < 64; j++) {
				auto child_idx = child_data.sel->get_index(entry.offset + j);
				counters[j] = child_data.validity.RowIsValid(child_idx) ? child_values[child_idx] : 0;
			}
			std::mt19937_64 gen(seed ^ (entry.offset * PAC_MAGIC_HASH));
			tmp[i] = PacNoisySampleFrom64Counters(counters, mi, 1.0, gen, 0ULL, query_hash, nullptr);
		}

		// Copy to result — handles both FLOAT and DOUBLE result types
		if (result.GetType().id() == LogicalTypeId::FLOAT) {
			auto result_data = FlatVector::GetData<float>(result);
			for (idx_t i = 0; i < count; i++) {
				result_data[i] = static_cast<float>(tmp[i]);
			}
		} else {
			auto result_data = FlatVector::GetData<double>(result);
			for (idx_t i = 0; i < count; i++) {
				result_data[i] = static_cast<double>(tmp[i]);
			}
		}
		return true;
	};
	loader.RegisterCastFunction(list_type, LogicalType::FLOAT, BoundCastInfo(pac_finalize_cast), 200);
	loader.RegisterCastFunction(list_type, LogicalType::DOUBLE, BoundCastInfo(pac_finalize_cast), 200);
}

} // namespace duckdb
