#include "aggregates/pac_distinct.hpp"
#include <cstring>

namespace duckdb {

// ============================================================================
// Common bind function
// ============================================================================
static unique_ptr<FunctionData> PacDistinctBind(ClientContext &ctx, AggregateFunction &func,
                                                vector<unique_ptr<Expression>> &args) {
	idx_t corr_idx = args.size(); // sentinel: no correction
	if (args.size() >= 2) {
		auto &last = args.back();
		if (last->IsFoldable() && (last->return_type.IsNumeric() || last->return_type.id() == LogicalTypeId::UNKNOWN)) {
			corr_idx = args.size() - 1;
		}
	}
	return MakePacBindData(ctx, args, corr_idx, func.name.c_str());
}

// ============================================================================
// PAC_COUNT_DISTINCT
// pac_count_distinct(UBIGINT key_hash, UBIGINT value_hash) → BIGINT
//
// Tracks a map of value_hash → OR(key_hashes). In finalize, each distinct value
// contributes to counter[j] if bit j of its accumulated key_hash is set.
// ============================================================================

static idx_t PacCountDistinctStateSize(const AggregateFunction &) {
	return sizeof(PacCountDistinctState);
}

static void PacCountDistinctInitialize(const AggregateFunction &, data_ptr_t state_ptr) {
	auto &state = *reinterpret_cast<PacCountDistinctState *>(state_ptr);
	state.distinct_map = nullptr;
	state.key_hash = 0;
	state.update_count = 0;
}

static void PacCountDistinctUpdate(Vector inputs[], AggregateInputData &aggr, idx_t input_count, data_ptr_t state_ptr,
                                   idx_t count) {
	auto &state = *reinterpret_cast<PacCountDistinctState *>(state_ptr);
	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<uint64_t>(value_data);

	if (!state.distinct_map) {
		state.distinct_map = new PacFlatMap<uint64_t>();
	}

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		if (hash_data.validity.RowIsValid(h_idx) && value_data.validity.RowIsValid(v_idx)) {
			state.key_hash |= hashes[h_idx];
			state.update_count++;
			*state.distinct_map->GetOrCreate(values[v_idx]) |= hashes[h_idx];
		}
	}
}

static void PacCountDistinctScatterUpdate(Vector inputs[], AggregateInputData &aggr, idx_t input_count, Vector &states,
                                          idx_t count) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<uint64_t>(value_data);
	auto state_p = UnifiedVectorFormat::GetData<PacCountDistinctState *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		if (hash_data.validity.RowIsValid(h_idx) && value_data.validity.RowIsValid(v_idx)) {
			auto &state = *state_p[sdata.sel->get_index(i)];
			if (!state.distinct_map) {
				state.distinct_map = new PacFlatMap<uint64_t>();
			}
			state.key_hash |= hashes[h_idx];
			state.update_count++;
			*state.distinct_map->GetOrCreate(values[v_idx]) |= hashes[h_idx];
		}
	}
}

static void PacCountDistinctCombine(Vector &src, Vector &dst, AggregateInputData &aggr, idx_t count) {
	auto sa = FlatVector::GetData<PacCountDistinctState *>(src);
	auto da = FlatVector::GetData<PacCountDistinctState *>(dst);
	for (idx_t i = 0; i < count; i++) {
		if (!sa[i]->distinct_map) {
			continue;
		}
		da[i]->key_hash |= sa[i]->key_hash;
		da[i]->update_count += sa[i]->update_count;
		if (!da[i]->distinct_map) {
			// Steal src's map
			da[i]->distinct_map = sa[i]->distinct_map;
			sa[i]->distinct_map = nullptr;
		} else {
			sa[i]->distinct_map->ForEach(
			    [&](uint64_t vh, uint64_t kh) { *da[i]->distinct_map->GetOrCreate(vh) |= kh; });
			delete sa[i]->distinct_map;
			sa[i]->distinct_map = nullptr;
		}
	}
}

static void PacCountDistinctDestroy(Vector &states, AggregateInputData &, idx_t count) {
	auto state_p = FlatVector::GetData<PacCountDistinctState *>(states);
	for (idx_t i = 0; i < count; i++) {
		delete state_p[i]->distinct_map;
		state_p[i]->distinct_map = nullptr;
	}
}

static void PacCountDistinctFinalize(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                     idx_t offset) {
	auto aggs = FlatVector::GetData<PacCountDistinctState *>(states);
	auto data = FlatVector::GetData<int64_t>(result);
	auto &result_mask = FlatVector::Validity(result);
	auto &bind = input.bind_data->Cast<PacBindData>();
	std::mt19937_64 gen(bind.seed);
	double mi = bind.mi;
	double correction = bind.correction;
	uint64_t query_hash = bind.query_hash;
	auto pstate = bind.pstate;
	PAC_FLOAT buf[64];

	for (idx_t i = 0; i < count; i++) {
		auto &state = *aggs[i];
		uint64_t key_hash = state.key_hash;

		if (PacNoiseInNull(key_hash, mi, correction, gen)) {
			result_mask.SetInvalid(offset + i);
			continue;
		}

		// Build 64 counters: for each distinct value, add 1 to counter[j] if bit j of its key_hash is set
		memset(buf, 0, sizeof(buf));
		if (state.distinct_map) {
			state.distinct_map->ForEach([&](uint64_t vh, uint64_t kh) {
				for (int j = 0; j < 64; j++) {
					buf[j] += static_cast<PAC_FLOAT>((kh >> j) & 1);
				}
			});
		}

		CheckPacSampleDiversity(key_hash, buf, state.update_count, "pac_count_distinct",
		                        input.bind_data->Cast<PacBindData>());

		data[offset + i] = static_cast<int64_t>(
		    PacNoisySampleFrom64Counters(buf, mi, correction, gen, true, ~key_hash, query_hash, pstate) * 2.0);
	}
}

void RegisterPacCountDistinctFunctions(ExtensionLoader &loader) {
	AggregateFunctionSet fcn_set("pac_count_distinct");

	auto func = AggregateFunction("pac_count_distinct", {LogicalType::UBIGINT, LogicalType::UBIGINT},
	                              LogicalType::BIGINT, PacCountDistinctStateSize, PacCountDistinctInitialize,
	                              PacCountDistinctScatterUpdate, PacCountDistinctCombine, PacCountDistinctFinalize,
	                              FunctionNullHandling::SPECIAL_HANDLING, PacCountDistinctUpdate, PacDistinctBind);
	func.destructor = PacCountDistinctDestroy;
	fcn_set.AddFunction(func);

	loader.RegisterFunction(fcn_set);
}

// ============================================================================
// PAC_SUM_DISTINCT / PAC_AVG_DISTINCT
// pac_sum_distinct(UBIGINT key_hash, DOUBLE value) → DOUBLE
// pac_avg_distinct(UBIGINT key_hash, DOUBLE value) → DOUBLE
//
// Tracks a map of bit_cast(value) → (OR(key_hashes), value). In finalize,
// each distinct value contributes its value to counter[j] if bit j is set.
// pac_avg_distinct divides each counter by its distinct count.
// ============================================================================

static idx_t PacSumDistinctStateSize(const AggregateFunction &) {
	return sizeof(PacSumDistinctState);
}

static void PacSumDistinctInitialize(const AggregateFunction &, data_ptr_t state_ptr) {
	auto &state = *reinterpret_cast<PacSumDistinctState *>(state_ptr);
	state.distinct_map = nullptr;
	state.key_hash = 0;
	state.update_count = 0;
}

// Helper: bit-cast double to uint64_t for use as map key (preserves equality semantics)
static inline uint64_t DoubleBits(double v) {
	uint64_t bits;
	memcpy(&bits, &v, sizeof(bits));
	return bits;
}

static void PacSumDistinctUpdate(Vector inputs[], AggregateInputData &aggr, idx_t input_count, data_ptr_t state_ptr,
                                 idx_t count) {
	auto &state = *reinterpret_cast<PacSumDistinctState *>(state_ptr);
	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<double>(value_data);

	if (!state.distinct_map) {
		state.distinct_map = new PacFlatMap<std::pair<uint64_t, double>>();
	}

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		if (hash_data.validity.RowIsValid(h_idx) && value_data.validity.RowIsValid(v_idx)) {
			state.key_hash |= hashes[h_idx];
			state.update_count++;
			uint64_t val_key = DoubleBits(values[v_idx]);
			auto *entry = state.distinct_map->GetOrCreate(val_key);
			entry->first |= hashes[h_idx];
			entry->second = values[v_idx];
		}
	}
}

static void PacSumDistinctScatterUpdate(Vector inputs[], AggregateInputData &aggr, idx_t input_count, Vector &states,
                                        idx_t count) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<double>(value_data);
	auto state_p = UnifiedVectorFormat::GetData<PacSumDistinctState *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		if (hash_data.validity.RowIsValid(h_idx) && value_data.validity.RowIsValid(v_idx)) {
			auto &state = *state_p[sdata.sel->get_index(i)];
			if (!state.distinct_map) {
				state.distinct_map = new PacFlatMap<std::pair<uint64_t, double>>();
			}
			state.key_hash |= hashes[h_idx];
			state.update_count++;
			uint64_t val_key = DoubleBits(values[v_idx]);
			auto *entry = state.distinct_map->GetOrCreate(val_key);
			entry->first |= hashes[h_idx];
			entry->second = values[v_idx];
		}
	}
}

static void PacSumDistinctCombine(Vector &src, Vector &dst, AggregateInputData &aggr, idx_t count) {
	auto sa = FlatVector::GetData<PacSumDistinctState *>(src);
	auto da = FlatVector::GetData<PacSumDistinctState *>(dst);
	for (idx_t i = 0; i < count; i++) {
		if (!sa[i]->distinct_map) {
			continue;
		}
		da[i]->key_hash |= sa[i]->key_hash;
		da[i]->update_count += sa[i]->update_count;
		if (!da[i]->distinct_map) {
			da[i]->distinct_map = sa[i]->distinct_map;
			sa[i]->distinct_map = nullptr;
		} else {
			sa[i]->distinct_map->ForEach([&](uint64_t vk, const std::pair<uint64_t, double> &pair) {
				auto *entry = da[i]->distinct_map->GetOrCreate(vk);
				entry->first |= pair.first;
				entry->second = pair.second;
			});
			delete sa[i]->distinct_map;
			sa[i]->distinct_map = nullptr;
		}
	}
}

static void PacSumDistinctDestroy(Vector &states, AggregateInputData &, idx_t count) {
	auto state_p = FlatVector::GetData<PacSumDistinctState *>(states);
	for (idx_t i = 0; i < count; i++) {
		delete state_p[i]->distinct_map;
		state_p[i]->distinct_map = nullptr;
	}
}

template <bool DIVIDE_BY_COUNT>
static void PacSumDistinctFinalizeInternal(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                           idx_t offset) {
	auto aggs = FlatVector::GetData<PacSumDistinctState *>(states);
	auto data = FlatVector::GetData<double>(result);
	auto &result_mask = FlatVector::Validity(result);
	auto &bind = input.bind_data->Cast<PacBindData>();
	std::mt19937_64 gen(bind.seed);
	double mi = bind.mi;
	double correction = bind.correction;
	uint64_t query_hash = bind.query_hash;
	auto pstate = bind.pstate;
	PAC_FLOAT buf[64];

	for (idx_t i = 0; i < count; i++) {
		auto &state = *aggs[i];
		uint64_t key_hash = state.key_hash;

		if (PacNoiseInNull(key_hash, mi, correction, gen)) {
			result_mask.SetInvalid(offset + i);
			continue;
		}

		// Build 64 counters from distinct map
		memset(buf, 0, sizeof(buf));
		PAC_FLOAT count_buf[64];
		if (DIVIDE_BY_COUNT) {
			memset(count_buf, 0, sizeof(count_buf));
		}

		if (state.distinct_map) {
			state.distinct_map->ForEach([&](uint64_t vk, const std::pair<uint64_t, double> &pair) {
				uint64_t kh = pair.first;
				double val = pair.second;
				for (int j = 0; j < 64; j++) {
					if ((kh >> j) & 1) {
						buf[j] += static_cast<PAC_FLOAT>(val);
						if (DIVIDE_BY_COUNT) {
							count_buf[j] += 1.0f;
						}
					}
				}
			});
		}

		if (DIVIDE_BY_COUNT) {
			// AVG: divide sum by count for each counter
			for (int j = 0; j < 64; j++) {
				if (count_buf[j] > 0) {
					buf[j] /= count_buf[j];
				}
			}
		}

		const char *name = DIVIDE_BY_COUNT ? "pac_avg_distinct" : "pac_sum_distinct";
		CheckPacSampleDiversity(key_hash, buf, state.update_count, name, input.bind_data->Cast<PacBindData>());

		data[offset + i] = static_cast<double>(
		    PacNoisySampleFrom64Counters(buf, mi, correction, gen, true, ~key_hash, query_hash, pstate) * 2.0);
	}
}

static void PacSumDistinctFinalize(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                   idx_t offset) {
	PacSumDistinctFinalizeInternal<false>(states, input, result, count, offset);
}

static void PacAvgDistinctFinalize(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                   idx_t offset) {
	PacSumDistinctFinalizeInternal<true>(states, input, result, count, offset);
}

void RegisterPacSumDistinctFunctions(ExtensionLoader &loader) {
	AggregateFunctionSet fcn_set("pac_sum_distinct");

	auto func = AggregateFunction("pac_sum_distinct", {LogicalType::UBIGINT, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                              PacSumDistinctStateSize, PacSumDistinctInitialize, PacSumDistinctScatterUpdate,
	                              PacSumDistinctCombine, PacSumDistinctFinalize, FunctionNullHandling::SPECIAL_HANDLING,
	                              PacSumDistinctUpdate, PacDistinctBind);
	func.destructor = PacSumDistinctDestroy;
	fcn_set.AddFunction(func);

	loader.RegisterFunction(fcn_set);
}

void RegisterPacAvgDistinctFunctions(ExtensionLoader &loader) {
	AggregateFunctionSet fcn_set("pac_avg_distinct");

	auto func = AggregateFunction("pac_avg_distinct", {LogicalType::UBIGINT, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                              PacSumDistinctStateSize, PacSumDistinctInitialize, PacSumDistinctScatterUpdate,
	                              PacSumDistinctCombine, PacAvgDistinctFinalize, FunctionNullHandling::SPECIAL_HANDLING,
	                              PacSumDistinctUpdate, PacDistinctBind);
	func.destructor = PacSumDistinctDestroy;
	fcn_set.AddFunction(func);

	loader.RegisterFunction(fcn_set);
}

} // namespace duckdb
