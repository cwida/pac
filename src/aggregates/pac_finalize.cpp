#include "aggregates/pac_aggregate.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <random>

namespace duckdb {

// ============================================================================
// pac_finalize bind data — stable world picking (no ActiveQuery mixing)
// ============================================================================
struct PacFinalizeBindData : public FunctionData {
	double mi;
	uint64_t seed;
	uint64_t query_hash;

	explicit PacFinalizeBindData(ClientContext &ctx) {
		mi = GetPacMiFromSetting(ctx);

		Value pac_seed_val;
		if (ctx.TryGetCurrentSetting("pac_seed", pac_seed_val) && !pac_seed_val.IsNull()) {
			seed = uint64_t(pac_seed_val.GetValue<int64_t>());
		} else {
			seed = 42;
		}
		// Stable world: do NOT mix in ActiveQuery — same counters always produce same noise
		query_hash = (seed * PAC_MAGIC_HASH) ^ PAC_MAGIC_HASH;
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<PacFinalizeBindData>(*this);
		return copy;
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<PacFinalizeBindData>();
		return mi == o.mi && seed == o.seed && query_hash == o.query_hash;
	}
};

static unique_ptr<FunctionData> PacFinalizeBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<PacFinalizeBindData>(context);
}

// ============================================================================
// pac_finalize(LIST<DOUBLE>) -> DOUBLE
// Takes 64 subsample counters and returns a noised scalar value.
// Counters already include 2x correction from pac_sum/pac_count finalize.
// ============================================================================
static void PacFinalizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// Check context availability first
	auto &context = state.GetContext();
	double mi = GetPacMiFromSetting(context);
	Value pac_seed_val;
	uint64_t seed = (context.TryGetCurrentSetting("pac_seed", pac_seed_val) && !pac_seed_val.IsNull())
	                    ? uint64_t(pac_seed_val.GetValue<int64_t>())
	                    : 42;
	// Stable world: no ActiveQuery mixing
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

		// Extract 64 counters
		PAC_FLOAT counters[64];
		for (idx_t j = 0; j < 64; j++) {
			auto child_idx = child_data.sel->get_index(entry.offset + j);
			counters[j] = child_data.validity.RowIsValid(child_idx) ? child_values[child_idx] : 0;
		}

		// Seed RNG deterministically per row using seed + row index
		std::mt19937_64 gen(seed ^ (i * PAC_MAGIC_HASH));

		// correction = 1.0 (already baked into counters), is_null = 0 (all counters valid), no pstate
		result_data[i] = PacNoisySampleFrom64Counters(counters, mi, 1.0, gen, 0ULL, query_hash, nullptr);
	}
}

void RegisterPacFinalizeFunction(ExtensionLoader &loader) {
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	ScalarFunction pac_finalize("pac_finalize", {list_type}, PacFloatLogicalType(), PacFinalizeFunction,
	                            PacFinalizeBind);
	loader.RegisterFunction(pac_finalize);
}

} // namespace duckdb
