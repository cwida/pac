#include "aggregates/pac_clip_min_max.hpp"
#include "categorical/pac_categorical.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <cmath>

namespace duckdb {

// ============================================================================
// Inner state update: always unsigned (caller provides abs value)
// ============================================================================
template <bool IS_MAX>
AUTOVECTORIZE inline void PacClipMinMaxUpdateOneInternal(PacClipMinMaxIntState<IS_MAX> &state, uint64_t key_hash,
                                                         uint64_t value, ArenaAllocator &allocator) {
	state.key_hash |= key_hash;

	int level = PacClipMinMaxIntState<IS_MAX>::GetLevel(value);
	int shift = level << 1;
	uint8_t shifted_val = static_cast<uint8_t>((value >> shift) & 0xFF);

	state.EnsureLevelAllocated(allocator, level);
	uint64_t *buf = state.levels[level];

	// Set bitmap bit (always, even if BOUNDOPT skips the extreme update)
	buf[PCMM_SWAR] |= (1ULL << (key_hash >> 58));

	// BOUNDOPT: skip expensive SIMD update if value can't improve any extreme at this level
	if (!PAC_IS_BETTER(shifted_val, state.level_bounds[level])) {
		return;
	}
	state.UpdateExtreme(buf, shifted_val, key_hash);

	// Periodically recompute bound
	if ((state.update_count & (BOUND_RECOMPUTE_INTERVAL - 1)) == 0) {
		state.RecomputeBound(level);
	}
}

// ============================================================================
// Route signed value to pos or neg state (two-sided, like clip_sum)
// ============================================================================
template <bool IS_MAX>
inline void PacClipMinMaxRouteValue(PacClipMinMaxStateWrapper<IS_MAX> &wrapper,
                                    PacClipMinMaxIntState<IS_MAX> *pos_state, uint64_t hash, int64_t value,
                                    ArenaAllocator &a) {
	if (value < 0) {
		auto *neg = wrapper.EnsureNegState(a);
		PacClipMinMaxUpdateOneInternal<!IS_MAX>(*neg, hash, static_cast<uint64_t>(-value), a);
		neg->update_count++;
	} else {
		PacClipMinMaxUpdateOneInternal<IS_MAX>(*pos_state, hash, static_cast<uint64_t>(value), a);
		pos_state->update_count++;
	}
}

// ============================================================================
// Buffered update (two-sided: SIGNED routes to pos/neg, !SIGNED always pos)
// ============================================================================
template <bool IS_MAX, bool SIGNED, typename ValueT>
AUTOVECTORIZE inline void PacClipMinMaxUpdateOne(PacClipMinMaxStateWrapper<IS_MAX> &agg, uint64_t key_hash,
                                                 ValueT value, ArenaAllocator &a) {
	uint64_t cnt = agg.n_buffered & PacClipMinMaxStateWrapper<IS_MAX>::BUF_MASK;
	if (DUCKDB_UNLIKELY(cnt == PacClipMinMaxStateWrapper<IS_MAX>::BUF_SIZE)) {
		auto *dst_state = agg.EnsureState(a);
		for (int i = 0; i < PacClipMinMaxStateWrapper<IS_MAX>::BUF_SIZE; i++) {
			if (SIGNED) {
				PacClipMinMaxRouteValue<IS_MAX>(agg, dst_state, agg.hash_buf[i], agg.val_buf[i], a);
			} else {
				PacClipMinMaxUpdateOneInternal<IS_MAX>(*dst_state, agg.hash_buf[i],
				                                       static_cast<uint64_t>(agg.val_buf[i]), a);
				dst_state->update_count++;
			}
		}
		if (SIGNED) {
			PacClipMinMaxRouteValue<IS_MAX>(agg, dst_state, key_hash, static_cast<int64_t>(value), a);
		} else {
			PacClipMinMaxUpdateOneInternal<IS_MAX>(*dst_state, key_hash, static_cast<uint64_t>(value), a);
			dst_state->update_count++;
		}
		agg.n_buffered &= ~PacClipMinMaxStateWrapper<IS_MAX>::BUF_MASK;
	} else {
		agg.val_buf[cnt] = static_cast<int64_t>(value);
		agg.hash_buf[cnt] = key_hash;
		agg.n_buffered++;
	}
}

// ============================================================================
// Buffer flush
// ============================================================================
template <bool IS_MAX, bool SIGNED>
inline void PacClipMinMaxFlushBuffer(PacClipMinMaxStateWrapper<IS_MAX> &src, PacClipMinMaxStateWrapper<IS_MAX> &dst,
                                     ArenaAllocator &a) {
	uint64_t cnt = src.n_buffered & PacClipMinMaxStateWrapper<IS_MAX>::BUF_MASK;
	if (cnt > 0) {
		auto *dst_state = dst.EnsureState(a);
		for (uint64_t i = 0; i < cnt; i++) {
			if (SIGNED) {
				PacClipMinMaxRouteValue<IS_MAX>(dst, dst_state, src.hash_buf[i], src.val_buf[i], a);
			} else {
				PacClipMinMaxUpdateOneInternal<IS_MAX>(*dst_state, src.hash_buf[i],
				                                       static_cast<uint64_t>(src.val_buf[i]), a);
				dst_state->update_count++;
			}
		}
		src.n_buffered &= ~PacClipMinMaxStateWrapper<IS_MAX>::BUF_MASK;
	}
}

// ============================================================================
// Vectorized Update and ScatterUpdate
// ============================================================================
template <bool IS_MAX, bool SIGNED, class VALUE_TYPE, class INPUT_TYPE>
static void PacClipMinMaxUpdate(Vector inputs[], PacClipMinMaxStateWrapper<IS_MAX> &state, idx_t count,
                                ArenaAllocator &allocator) {
	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);

	if (hash_data.validity.AllValid() && value_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			PacClipMinMaxUpdateOne<IS_MAX, SIGNED>(state, hashes[h_idx],
			                                       ConvertValue<VALUE_TYPE>::convert(values[v_idx]), allocator);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
				continue;
			}
			PacClipMinMaxUpdateOne<IS_MAX, SIGNED>(state, hashes[h_idx],
			                                       ConvertValue<VALUE_TYPE>::convert(values[v_idx]), allocator);
		}
	}
}

template <bool IS_MAX, bool SIGNED, class VALUE_TYPE, class INPUT_TYPE>
static void PacClipMinMaxScatterUpdate(Vector inputs[], Vector &states, idx_t count, ArenaAllocator &allocator) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);

	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipMinMaxStateWrapper<IS_MAX> *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		PacClipMinMaxUpdateOne<IS_MAX, SIGNED>(*state, hashes[h_idx], ConvertValue<VALUE_TYPE>::convert(values[v_idx]),
		                                       allocator);
	}
}

// ============================================================================
// X-macro: generate Update/ScatterUpdate for integer types
// ============================================================================
#define PCMM_INT_TYPES_SIGNED                                                                                          \
	X(TinyInt, int64_t, int8_t, true)                                                                                  \
	X(SmallInt, int64_t, int16_t, true)                                                                                \
	X(Integer, int64_t, int32_t, true)                                                                                 \
	X(BigInt, int64_t, int64_t, true)

#define PCMM_INT_TYPES_UNSIGNED                                                                                        \
	X(UTinyInt, uint64_t, uint8_t, false)                                                                              \
	X(USmallInt, uint64_t, uint16_t, false)                                                                            \
	X(UInteger, uint64_t, uint32_t, false)                                                                             \
	X(UBigInt, uint64_t, uint64_t, false)

// Generate for IS_MAX=true (MAX)
#define X(NAME, VALUE_T, INPUT_T, SIGNED_VAL)                                                                          \
	static void PacClipMaxUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p,           \
	                                   idx_t count) {                                                                  \
		auto &state = *reinterpret_cast<PacClipMinMaxStateWrapper<true> *>(state_p);                                   \
		PacClipMinMaxUpdate<true, SIGNED_VAL, VALUE_T, INPUT_T>(inputs, state, count, aggr.allocator);                 \
	}                                                                                                                  \
	static void PacClipMaxScatterUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states,        \
	                                          idx_t count) {                                                           \
		PacClipMinMaxScatterUpdate<true, SIGNED_VAL, VALUE_T, INPUT_T>(inputs, states, count, aggr.allocator);         \
	}
PCMM_INT_TYPES_SIGNED
PCMM_INT_TYPES_UNSIGNED
#undef X

// Generate for IS_MAX=false (MIN)
#define X(NAME, VALUE_T, INPUT_T, SIGNED_VAL)                                                                          \
	static void PacClipMinUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p,           \
	                                   idx_t count) {                                                                  \
		auto &state = *reinterpret_cast<PacClipMinMaxStateWrapper<false> *>(state_p);                                  \
		PacClipMinMaxUpdate<false, SIGNED_VAL, VALUE_T, INPUT_T>(inputs, state, count, aggr.allocator);                \
	}                                                                                                                  \
	static void PacClipMinScatterUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states,        \
	                                          idx_t count) {                                                           \
		PacClipMinMaxScatterUpdate<false, SIGNED_VAL, VALUE_T, INPUT_T>(inputs, states, count, aggr.allocator);        \
	}
PCMM_INT_TYPES_SIGNED
PCMM_INT_TYPES_UNSIGNED
#undef X

// ============================================================================
// Float/double update: scale to int64, route through signed path
// ============================================================================
template <bool IS_MAX, typename FLOAT_TYPE, int SHIFT>
static void PacClipMinMaxUpdateFloat(Vector inputs[], PacClipMinMaxStateWrapper<IS_MAX> &state, idx_t count,
                                     ArenaAllocator &allocator) {
	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<FLOAT_TYPE>(value_data);

	if (hash_data.validity.AllValid() && value_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			PacClipMinMaxUpdateOne<IS_MAX, true>(state, hashes[h_idx],
			                                     ScaleFloatToInt64<FLOAT_TYPE, SHIFT>(values[v_idx]), allocator);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
				continue;
			}
			PacClipMinMaxUpdateOne<IS_MAX, true>(state, hashes[h_idx],
			                                     ScaleFloatToInt64<FLOAT_TYPE, SHIFT>(values[v_idx]), allocator);
		}
	}
}

template <bool IS_MAX, typename FLOAT_TYPE, int SHIFT>
static void PacClipMinMaxScatterUpdateFloat(Vector inputs[], Vector &states, idx_t count, ArenaAllocator &allocator) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<FLOAT_TYPE>(value_data);
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipMinMaxStateWrapper<IS_MAX> *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		PacClipMinMaxUpdateOne<IS_MAX, true>(*state, hashes[h_idx], ScaleFloatToInt64<FLOAT_TYPE, SHIFT>(values[v_idx]),
		                                     allocator);
	}
}

// X-macro: generate float/double Update/ScatterUpdate for MAX and MIN
#define PCMM_FLOAT_TYPES                                                                                               \
	XF(SingleFloat, float, CLIP_FLOAT_SHIFT)                                                                           \
	XF(SingleDouble, double, CLIP_DOUBLE_SHIFT)

#define XF(NAME, FLOAT_T, SHIFT_VAL)                                                                                   \
	static void PacClipMaxUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p,           \
	                                   idx_t count) {                                                                  \
		auto &state = *reinterpret_cast<PacClipMinMaxStateWrapper<true> *>(state_p);                                   \
		PacClipMinMaxUpdateFloat<true, FLOAT_T, SHIFT_VAL>(inputs, state, count, aggr.allocator);                      \
	}                                                                                                                  \
	static void PacClipMaxScatterUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states,        \
	                                          idx_t count) {                                                           \
		PacClipMinMaxScatterUpdateFloat<true, FLOAT_T, SHIFT_VAL>(inputs, states, count, aggr.allocator);              \
	}                                                                                                                  \
	static void PacClipMinUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p,           \
	                                   idx_t count) {                                                                  \
		auto &state = *reinterpret_cast<PacClipMinMaxStateWrapper<false> *>(state_p);                                  \
		PacClipMinMaxUpdateFloat<false, FLOAT_T, SHIFT_VAL>(inputs, state, count, aggr.allocator);                     \
	}                                                                                                                  \
	static void PacClipMinScatterUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states,        \
	                                          idx_t count) {                                                           \
		PacClipMinMaxScatterUpdateFloat<false, FLOAT_T, SHIFT_VAL>(inputs, states, count, aggr.allocator);             \
	}
PCMM_FLOAT_TYPES
#undef XF

// ============================================================================
// Combine
// ============================================================================
template <bool IS_MAX>
static void PacClipMinMaxCombineInt(Vector &src, Vector &dst, idx_t count, ArenaAllocator &allocator) {
	auto src_wrapper = FlatVector::GetData<PacClipMinMaxStateWrapper<IS_MAX> *>(src);
	auto dst_wrapper = FlatVector::GetData<PacClipMinMaxStateWrapper<IS_MAX> *>(dst);

	for (idx_t i = 0; i < count; i++) {
		// Flush src's buffer into dst (always signed — values stored as int64 in buffer)
		PacClipMinMaxFlushBuffer<IS_MAX, true>(*src_wrapper[i], *dst_wrapper[i], allocator);

		auto *s = src_wrapper[i]->GetState();
		if (s) {
			auto *d = dst_wrapper[i]->EnsureState(allocator);
			d->CombineFrom(s, allocator);
		}
		// Combine neg states
		auto *s_neg = src_wrapper[i]->GetNegState();
		if (s_neg) {
			auto *d_neg = dst_wrapper[i]->GetNegState();
			if (!d_neg) {
				dst_wrapper[i]->neg_state = s_neg; // steal
			} else {
				d_neg->CombineFrom(s_neg, allocator);
			}
		}
	}
}

static void PacClipMaxCombine(Vector &src, Vector &dst, AggregateInputData &aggr, idx_t count) {
	PacClipMinMaxCombineInt<true>(src, dst, count, aggr.allocator);
}
static void PacClipMinCombine(Vector &src, Vector &dst, AggregateInputData &aggr, idx_t count) {
	PacClipMinMaxCombineInt<false>(src, dst, count, aggr.allocator);
}

// PacClipBindData is defined in pac_clip_aggr.hpp

// ============================================================================
// Finalize: noised scalar output
// ============================================================================
template <bool IS_MAX, class ACC_TYPE>
static void PacClipMinMaxFinalize(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                  idx_t offset) {
	auto state_ptrs = FlatVector::GetData<PacClipMinMaxStateWrapper<IS_MAX> *>(states);
	auto data = FlatVector::GetData<ACC_TYPE>(result);
	auto &result_mask = FlatVector::Validity(result);
	auto &bind = static_cast<PacClipBindData &>(*input.bind_data);
	double mi = bind.mi;
	double correction = bind.correction;
	uint64_t query_hash = bind.query_hash;
	auto pstate = bind.pstate;
	int clip_support = bind.clip_support_threshold;
	bool clip_scale = bind.clip_scale;

	for (idx_t i = 0; i < count; i++) {
		PacClipMinMaxFlushBuffer<IS_MAX, true>(*state_ptrs[i], *state_ptrs[i], input.allocator);

		PAC_FLOAT buf[64] = {0};
		auto *pos = state_ptrs[i]->GetState();
		auto *neg = state_ptrs[i]->GetNegState();
		if (!pos && !neg) {
			result_mask.SetInvalid(offset + i);
			continue;
		}
		uint64_t key_hash = (pos ? pos->key_hash : 0) | (neg ? neg->key_hash : 0);
		std::mt19937_64 gen(bind.seed);
		if (PacNoiseInNull(key_hash, mi, correction, gen)) {
			result_mask.SetInvalid(offset + i);
			continue;
		}
		uint64_t update_count = 0;
		if (pos) {
			pos->GetTotals(buf, clip_support, clip_scale);
			update_count = pos->update_count;
		}
		// Merge neg state: negate absolute extremes back to negative values
		if (neg) {
			PAC_FLOAT neg_buf[64] = {0};
			neg->GetTotals(neg_buf, clip_support, clip_scale);
			for (int j = 0; j < 64; j++) {
				// Only merge if neg had a surviving contribution (not fully clipped)
				if (neg_buf[j] != 0) {
					PAC_FLOAT neg_val = -neg_buf[j];
					if (IS_MAX) {
						buf[j] = std::max(buf[j], neg_val);
					} else {
						buf[j] = std::min(buf[j], neg_val);
					}
				}
			}
			update_count += neg->update_count;
		}
		CheckPacSampleDiversity(key_hash, buf, update_count, IS_MAX ? "pac_noised_clip_max" : "pac_noised_clip_min",
		                        bind);
		double noise_var = 0.0;
		PAC_FLOAT result_val =
		    PacNoisySampleFrom64Counters(buf, mi, correction, gen, ~key_hash, query_hash, pstate, &noise_var);
		result_val /= static_cast<PAC_FLOAT>(bind.float_scale);
		// Utility NULLing: no variance scaling for min/max (no 2x compensation)
		double utility_threshold = bind.utility_threshold;
		if (PacUtilityNull(static_cast<double>(result_val), noise_var, utility_threshold, gen)) {
			result_mask.SetInvalid(offset + i);
			continue;
		}
		data[offset + i] = FromDouble<ACC_TYPE>(result_val);
	}
}

// Noised finalize instantiations — return type matches input type
// Integer inputs: return same type as non-clip min/max (the type itself)
// For clip variants, noised output returns the value type. We use templates to handle all types.

// Helper to deduce return type from value type. For integers, the noised clip min/max
// returns the same type. For float/double, returns float/double.
// X-macro: generate noised finalize wrappers for all output types × MAX/MIN
#define PCMM_FINALIZE_TYPES                                                                                            \
	XFIN(BigInt, int64_t)                                                                                              \
	XFIN(Float, float)                                                                                                 \
	XFIN(Double, double)                                                                                               \
	XFIN(HugeInt, hugeint_t)

#define XFIN(NAME, ACC_T)                                                                                              \
	static void PacClipMaxNoisedFinalize##NAME(Vector &s, AggregateInputData &i, Vector &r, idx_t c, idx_t o) {        \
		PacClipMinMaxFinalize<true, ACC_T>(s, i, r, c, o);                                                             \
	}                                                                                                                  \
	static void PacClipMinNoisedFinalize##NAME(Vector &s, AggregateInputData &i, Vector &r, idx_t c, idx_t o) {        \
		PacClipMinMaxFinalize<false, ACC_T>(s, i, r, c, o);                                                            \
	}
PCMM_FINALIZE_TYPES
#undef XFIN

// ============================================================================
// Counters finalize (LIST<FLOAT> output)
// ============================================================================
template <bool IS_MAX>
static void PacClipMinMaxFinalizeCounters(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                          idx_t offset) {
	auto state_ptrs = FlatVector::GetData<PacClipMinMaxStateWrapper<IS_MAX> *>(states);
	auto &bind = static_cast<PacClipBindData &>(*input.bind_data);
	int clip_support = bind.clip_support_threshold;
	double correction = bind.correction;
	double float_scale = bind.float_scale;
	bool clip_scale = bind.clip_scale;

	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &child_vec = ListVector::GetEntry(result);

	idx_t total_elements = count * 64;
	ListVector::Reserve(result, total_elements);
	ListVector::SetListSize(result, total_elements);

	auto child_data = FlatVector::GetData<PAC_FLOAT>(child_vec);

	for (idx_t i = 0; i < count; i++) {
		PacClipMinMaxFlushBuffer<IS_MAX, true>(*state_ptrs[i], *state_ptrs[i], input.allocator);

		list_entries[offset + i].offset = i * 64;
		list_entries[offset + i].length = 64;

		PAC_FLOAT buf[64] = {0};
		uint64_t key_hash = 0;
		uint64_t update_count = 0;

		auto *pos = state_ptrs[i]->GetState();
		auto *neg = state_ptrs[i]->GetNegState();
		if (pos) {
			key_hash = pos->key_hash;
			update_count = pos->update_count;
			pos->GetTotals(buf, clip_support, clip_scale);
		}
		if (neg) {
			PAC_FLOAT neg_buf[64] = {0};
			neg->GetTotals(neg_buf, clip_support, clip_scale);
			key_hash |= neg->key_hash;
			for (int j = 0; j < 64; j++) {
				if (neg_buf[j] != 0) {
					PAC_FLOAT neg_val = -neg_buf[j];
					if (IS_MAX) {
						buf[j] = std::max(buf[j], neg_val);
					} else {
						buf[j] = std::min(buf[j], neg_val);
					}
				}
			}
			update_count += neg->update_count;
		}
		CheckPacSampleDiversity(key_hash, buf, update_count, IS_MAX ? "pac_clip_max" : "pac_clip_min", bind);

		idx_t base = i * 64;
		for (int j = 0; j < 64; j++) {
			if ((key_hash >> j) & 1ULL) {
				child_data[base + j] = static_cast<PAC_FLOAT>(buf[j] * correction / float_scale);
			} else {
				child_data[base + j] = 0.0;
			}
		}
	}
}

static void PacClipMaxFinalizeCounters(Vector &s, AggregateInputData &i, Vector &r, idx_t c, idx_t o) {
	PacClipMinMaxFinalizeCounters<true>(s, i, r, c, o);
}
static void PacClipMinFinalizeCounters(Vector &s, AggregateInputData &i, Vector &r, idx_t c, idx_t o) {
	PacClipMinMaxFinalizeCounters<false>(s, i, r, c, o);
}

// ============================================================================
// State size / init / bind
// ============================================================================
template <bool IS_MAX>
static idx_t PacClipMinMaxStateSize(const AggregateFunction &) {
	return sizeof(PacClipMinMaxStateWrapper<IS_MAX>);
}

template <bool IS_MAX>
static void PacClipMinMaxInitialize(const AggregateFunction &, data_ptr_t state_p) {
	memset(state_p, 0, sizeof(PacClipMinMaxStateWrapper<IS_MAX>));
}

// PacClipBind, PacClipBindFloat, PacClipBindDouble are defined in pac_clip_aggr.hpp

// ============================================================================
// DECIMAL support: dispatch by physical type
// ============================================================================
template <bool IS_MAX>
static AggregateFunction GetPacClipMinMaxNoisedAggregate(PhysicalType type) {
	const char *name = IS_MAX ? "pac_noised_clip_max" : "pac_noised_clip_min";
	auto finalize = IS_MAX ? PacClipMaxNoisedFinalizeBigInt : PacClipMinNoisedFinalizeBigInt;
	auto combine = IS_MAX ? PacClipMaxCombine : PacClipMinCombine;
	auto state_size = PacClipMinMaxStateSize<IS_MAX>;
	auto init = PacClipMinMaxInitialize<IS_MAX>;

	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::SMALLINT}, LogicalType::HUGEINT, state_size,
		                         init, IS_MAX ? PacClipMaxScatterUpdateSmallInt : PacClipMinScatterUpdateSmallInt,
		                         combine, finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
		                         IS_MAX ? PacClipMaxUpdateSmallInt : PacClipMinUpdateSmallInt);
	case PhysicalType::INT32:
		return AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::INTEGER}, LogicalType::HUGEINT, state_size,
		                         init, IS_MAX ? PacClipMaxScatterUpdateInteger : PacClipMinScatterUpdateInteger,
		                         combine, finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
		                         IS_MAX ? PacClipMaxUpdateInteger : PacClipMinUpdateInteger);
	case PhysicalType::INT64:
		return AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::BIGINT}, LogicalType::HUGEINT, state_size,
		                         init, IS_MAX ? PacClipMaxScatterUpdateBigInt : PacClipMinScatterUpdateBigInt, combine,
		                         finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
		                         IS_MAX ? PacClipMaxUpdateBigInt : PacClipMinUpdateBigInt);
	case PhysicalType::INT128:
		return AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::HUGEINT}, LogicalType::HUGEINT, state_size,
		                         init, IS_MAX ? PacClipMaxScatterUpdateBigInt : PacClipMinScatterUpdateBigInt, combine,
		                         IS_MAX ? PacClipMaxNoisedFinalizeHugeInt : PacClipMinNoisedFinalizeHugeInt,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING,
		                         IS_MAX ? PacClipMaxUpdateBigInt : PacClipMinUpdateBigInt);
	default:
		throw InternalException("pac_noised_clip_min/max: unsupported decimal physical type");
	}
}

template <bool IS_MAX>
static unique_ptr<FunctionData> BindDecimalPacNoisedClipMinMax(ClientContext &ctx, AggregateFunction &function,
                                                               vector<unique_ptr<Expression>> &args) {
	auto decimal_type = args[1]->return_type;
	function = GetPacClipMinMaxNoisedAggregate<IS_MAX>(decimal_type.InternalType());
	function.name = IS_MAX ? "pac_noised_clip_max" : "pac_noised_clip_min";
	function.arguments[1] = decimal_type;
	function.return_type = LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, DecimalType::GetScale(decimal_type));
	return PacClipBind(ctx, function, args);
}

// ============================================================================
// Registration helpers
// ============================================================================
template <bool IS_MAX>
static void AddClipMinMaxCountersFcn(AggregateFunctionSet &set, const string &name, const LogicalType &value_type,
                                     aggregate_update_t scatter, aggregate_finalize_t finalize,
                                     aggregate_simple_update_t update) {
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type}, list_type,
	                                  PacClipMinMaxStateSize<IS_MAX>, PacClipMinMaxInitialize<IS_MAX>, scatter,
	                                  IS_MAX ? PacClipMaxCombine : PacClipMinCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipBind));
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type, LogicalType::DOUBLE}, list_type,
	                                  PacClipMinMaxStateSize<IS_MAX>, PacClipMinMaxInitialize<IS_MAX>, scatter,
	                                  IS_MAX ? PacClipMaxCombine : PacClipMinCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipBind));
}

template <bool IS_MAX>
static void AddNoisedClipMinMaxFcn(AggregateFunctionSet &set, const string &name, const LogicalType &value_type,
                                   const LogicalType &result_type, aggregate_update_t scatter,
                                   aggregate_finalize_t finalize, aggregate_simple_update_t update) {
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type}, result_type,
	                                  PacClipMinMaxStateSize<IS_MAX>, PacClipMinMaxInitialize<IS_MAX>, scatter,
	                                  IS_MAX ? PacClipMaxCombine : PacClipMinCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipBind));
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type, LogicalType::DOUBLE}, result_type,
	                                  PacClipMinMaxStateSize<IS_MAX>, PacClipMinMaxInitialize<IS_MAX>, scatter,
	                                  IS_MAX ? PacClipMaxCombine : PacClipMinCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipBind));
}

// Helper to register all type overloads
template <bool IS_MAX>
static void RegisterClipMinMaxTypeOverloads(AggregateFunctionSet &set, const string &name, bool counters) {
	auto counters_finalize = IS_MAX ? PacClipMaxFinalizeCounters : PacClipMinFinalizeCounters;
	auto noised_finalize = IS_MAX ? PacClipMaxNoisedFinalizeBigInt : PacClipMinNoisedFinalizeBigInt;

	if (counters) {
		// Counters (LIST<FLOAT>) variants — signed types
		AddClipMinMaxCountersFcn<IS_MAX>(set, name, LogicalType::TINYINT,
		                                 IS_MAX ? PacClipMaxScatterUpdateTinyInt : PacClipMinScatterUpdateTinyInt,
		                                 counters_finalize, IS_MAX ? PacClipMaxUpdateTinyInt : PacClipMinUpdateTinyInt);
		AddClipMinMaxCountersFcn<IS_MAX>(set, name, LogicalType::BOOLEAN,
		                                 IS_MAX ? PacClipMaxScatterUpdateTinyInt : PacClipMinScatterUpdateTinyInt,
		                                 counters_finalize, IS_MAX ? PacClipMaxUpdateTinyInt : PacClipMinUpdateTinyInt);
		AddClipMinMaxCountersFcn<IS_MAX>(set, name, LogicalType::SMALLINT,
		                                 IS_MAX ? PacClipMaxScatterUpdateSmallInt : PacClipMinScatterUpdateSmallInt,
		                                 counters_finalize,
		                                 IS_MAX ? PacClipMaxUpdateSmallInt : PacClipMinUpdateSmallInt);
		AddClipMinMaxCountersFcn<IS_MAX>(set, name, LogicalType::INTEGER,
		                                 IS_MAX ? PacClipMaxScatterUpdateInteger : PacClipMinScatterUpdateInteger,
		                                 counters_finalize, IS_MAX ? PacClipMaxUpdateInteger : PacClipMinUpdateInteger);
		AddClipMinMaxCountersFcn<IS_MAX>(set, name, LogicalType::BIGINT,
		                                 IS_MAX ? PacClipMaxScatterUpdateBigInt : PacClipMinScatterUpdateBigInt,
		                                 counters_finalize, IS_MAX ? PacClipMaxUpdateBigInt : PacClipMinUpdateBigInt);
		// Unsigned types
		AddClipMinMaxCountersFcn<IS_MAX>(set, name, LogicalType::UTINYINT,
		                                 IS_MAX ? PacClipMaxScatterUpdateUTinyInt : PacClipMinScatterUpdateUTinyInt,
		                                 counters_finalize,
		                                 IS_MAX ? PacClipMaxUpdateUTinyInt : PacClipMinUpdateUTinyInt);
		AddClipMinMaxCountersFcn<IS_MAX>(set, name, LogicalType::USMALLINT,
		                                 IS_MAX ? PacClipMaxScatterUpdateUSmallInt : PacClipMinScatterUpdateUSmallInt,
		                                 counters_finalize,
		                                 IS_MAX ? PacClipMaxUpdateUSmallInt : PacClipMinUpdateUSmallInt);
		AddClipMinMaxCountersFcn<IS_MAX>(set, name, LogicalType::UINTEGER,
		                                 IS_MAX ? PacClipMaxScatterUpdateUInteger : PacClipMinScatterUpdateUInteger,
		                                 counters_finalize,
		                                 IS_MAX ? PacClipMaxUpdateUInteger : PacClipMinUpdateUInteger);
		AddClipMinMaxCountersFcn<IS_MAX>(set, name, LogicalType::UBIGINT,
		                                 IS_MAX ? PacClipMaxScatterUpdateUBigInt : PacClipMinScatterUpdateUBigInt,
		                                 counters_finalize, IS_MAX ? PacClipMaxUpdateUBigInt : PacClipMinUpdateUBigInt);
	} else {
		// Noised (scalar) variants — signed types
		AddNoisedClipMinMaxFcn<IS_MAX>(set, name, LogicalType::TINYINT, LogicalType::BIGINT,
		                               IS_MAX ? PacClipMaxScatterUpdateTinyInt : PacClipMinScatterUpdateTinyInt,
		                               noised_finalize, IS_MAX ? PacClipMaxUpdateTinyInt : PacClipMinUpdateTinyInt);
		AddNoisedClipMinMaxFcn<IS_MAX>(set, name, LogicalType::BOOLEAN, LogicalType::BIGINT,
		                               IS_MAX ? PacClipMaxScatterUpdateTinyInt : PacClipMinScatterUpdateTinyInt,
		                               noised_finalize, IS_MAX ? PacClipMaxUpdateTinyInt : PacClipMinUpdateTinyInt);
		AddNoisedClipMinMaxFcn<IS_MAX>(set, name, LogicalType::SMALLINT, LogicalType::BIGINT,
		                               IS_MAX ? PacClipMaxScatterUpdateSmallInt : PacClipMinScatterUpdateSmallInt,
		                               noised_finalize, IS_MAX ? PacClipMaxUpdateSmallInt : PacClipMinUpdateSmallInt);
		AddNoisedClipMinMaxFcn<IS_MAX>(set, name, LogicalType::INTEGER, LogicalType::BIGINT,
		                               IS_MAX ? PacClipMaxScatterUpdateInteger : PacClipMinScatterUpdateInteger,
		                               noised_finalize, IS_MAX ? PacClipMaxUpdateInteger : PacClipMinUpdateInteger);
		AddNoisedClipMinMaxFcn<IS_MAX>(set, name, LogicalType::BIGINT, LogicalType::BIGINT,
		                               IS_MAX ? PacClipMaxScatterUpdateBigInt : PacClipMinScatterUpdateBigInt,
		                               noised_finalize, IS_MAX ? PacClipMaxUpdateBigInt : PacClipMinUpdateBigInt);
		// Unsigned types
		AddNoisedClipMinMaxFcn<IS_MAX>(set, name, LogicalType::UTINYINT, LogicalType::BIGINT,
		                               IS_MAX ? PacClipMaxScatterUpdateUTinyInt : PacClipMinScatterUpdateUTinyInt,
		                               noised_finalize, IS_MAX ? PacClipMaxUpdateUTinyInt : PacClipMinUpdateUTinyInt);
		AddNoisedClipMinMaxFcn<IS_MAX>(set, name, LogicalType::USMALLINT, LogicalType::BIGINT,
		                               IS_MAX ? PacClipMaxScatterUpdateUSmallInt : PacClipMinScatterUpdateUSmallInt,
		                               noised_finalize, IS_MAX ? PacClipMaxUpdateUSmallInt : PacClipMinUpdateUSmallInt);
		AddNoisedClipMinMaxFcn<IS_MAX>(set, name, LogicalType::UINTEGER, LogicalType::BIGINT,
		                               IS_MAX ? PacClipMaxScatterUpdateUInteger : PacClipMinScatterUpdateUInteger,
		                               noised_finalize, IS_MAX ? PacClipMaxUpdateUInteger : PacClipMinUpdateUInteger);
		AddNoisedClipMinMaxFcn<IS_MAX>(set, name, LogicalType::UBIGINT, LogicalType::BIGINT,
		                               IS_MAX ? PacClipMaxScatterUpdateUBigInt : PacClipMinScatterUpdateUBigInt,
		                               noised_finalize, IS_MAX ? PacClipMaxUpdateUBigInt : PacClipMinUpdateUBigInt);
	}
}

// ============================================================================
// Add float/double overloads to a function set
// ============================================================================
template <bool IS_MAX>
static void AddFloatDoubleOverloads(AggregateFunctionSet &set, const string &name, bool counters) {
	auto combine = IS_MAX ? PacClipMaxCombine : PacClipMinCombine;
	auto state_size = PacClipMinMaxStateSize<IS_MAX>;
	auto init = PacClipMinMaxInitialize<IS_MAX>;

	if (counters) {
		auto finalize = IS_MAX ? PacClipMaxFinalizeCounters : PacClipMinFinalizeCounters;
		auto list_type = LogicalType::LIST(PacFloatLogicalType());

		// FLOAT
		set.AddFunction(
		    AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::FLOAT}, list_type, state_size, init,
		                      IS_MAX ? PacClipMaxScatterUpdateSingleFloat : PacClipMinScatterUpdateSingleFloat, combine,
		                      finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
		                      IS_MAX ? PacClipMaxUpdateSingleFloat : PacClipMinUpdateSingleFloat, PacClipBindFloat));
		set.AddFunction(AggregateFunction(
		    name, {LogicalType::UBIGINT, LogicalType::FLOAT, LogicalType::DOUBLE}, list_type, state_size, init,
		    IS_MAX ? PacClipMaxScatterUpdateSingleFloat : PacClipMinScatterUpdateSingleFloat, combine, finalize,
		    FunctionNullHandling::DEFAULT_NULL_HANDLING,
		    IS_MAX ? PacClipMaxUpdateSingleFloat : PacClipMinUpdateSingleFloat, PacClipBindFloat));

		// DOUBLE
		set.AddFunction(
		    AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::DOUBLE}, list_type, state_size, init,
		                      IS_MAX ? PacClipMaxScatterUpdateSingleDouble : PacClipMinScatterUpdateSingleDouble,
		                      combine, finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
		                      IS_MAX ? PacClipMaxUpdateSingleDouble : PacClipMinUpdateSingleDouble, PacClipBindDouble));
		set.AddFunction(AggregateFunction(
		    name, {LogicalType::UBIGINT, LogicalType::DOUBLE, LogicalType::DOUBLE}, list_type, state_size, init,
		    IS_MAX ? PacClipMaxScatterUpdateSingleDouble : PacClipMinScatterUpdateSingleDouble, combine, finalize,
		    FunctionNullHandling::DEFAULT_NULL_HANDLING,
		    IS_MAX ? PacClipMaxUpdateSingleDouble : PacClipMinUpdateSingleDouble, PacClipBindDouble));
	} else {
		auto float_finalize = IS_MAX ? PacClipMaxNoisedFinalizeFloat : PacClipMinNoisedFinalizeFloat;
		auto double_finalize = IS_MAX ? PacClipMaxNoisedFinalizeDouble : PacClipMinNoisedFinalizeDouble;

		// FLOAT → FLOAT
		set.AddFunction(
		    AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::FLOAT}, LogicalType::FLOAT, state_size, init,
		                      IS_MAX ? PacClipMaxScatterUpdateSingleFloat : PacClipMinScatterUpdateSingleFloat, combine,
		                      float_finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
		                      IS_MAX ? PacClipMaxUpdateSingleFloat : PacClipMinUpdateSingleFloat, PacClipBindFloat));
		set.AddFunction(AggregateFunction(
		    name, {LogicalType::UBIGINT, LogicalType::FLOAT, LogicalType::DOUBLE}, LogicalType::FLOAT, state_size, init,
		    IS_MAX ? PacClipMaxScatterUpdateSingleFloat : PacClipMinScatterUpdateSingleFloat, combine, float_finalize,
		    FunctionNullHandling::DEFAULT_NULL_HANDLING,
		    IS_MAX ? PacClipMaxUpdateSingleFloat : PacClipMinUpdateSingleFloat, PacClipBindFloat));

		// DOUBLE → DOUBLE
		set.AddFunction(
		    AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::DOUBLE}, LogicalType::DOUBLE, state_size, init,
		                      IS_MAX ? PacClipMaxScatterUpdateSingleDouble : PacClipMinScatterUpdateSingleDouble,
		                      combine, double_finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
		                      IS_MAX ? PacClipMaxUpdateSingleDouble : PacClipMinUpdateSingleDouble, PacClipBindDouble));
		set.AddFunction(AggregateFunction(
		    name, {LogicalType::UBIGINT, LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE, state_size,
		    init, IS_MAX ? PacClipMaxScatterUpdateSingleDouble : PacClipMinScatterUpdateSingleDouble, combine,
		    double_finalize, FunctionNullHandling::DEFAULT_NULL_HANDLING,
		    IS_MAX ? PacClipMaxUpdateSingleDouble : PacClipMinUpdateSingleDouble, PacClipBindDouble));
	}
}

// ============================================================================
// Registration: templated helpers to avoid duplicating MIN/MAX registration
// ============================================================================
template <bool IS_MAX>
static void RegisterPacClipMinMaxCountersFunctions(ExtensionLoader &loader) {
	const string name = IS_MAX ? "pac_clip_max" : "pac_clip_min";
	const string short_name = IS_MAX ? "clip_max" : "clip_min";
	AggregateFunctionSet fcn_set(name);
	RegisterClipMinMaxTypeOverloads<IS_MAX>(fcn_set, name, true);

	// DECIMAL overloads
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	fcn_set.AddFunction(AggregateFunction({LogicalType::UBIGINT, LogicalTypeId::DECIMAL}, list_type, nullptr, nullptr,
	                                      nullptr, nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                                      nullptr, BindDecimalPacNoisedClipMinMax<IS_MAX>));

	AddFloatDoubleOverloads<IS_MAX>(fcn_set, name, true);
	AddPacListAggregateOverload(fcn_set, short_name);

	CreateAggregateFunctionInfo info(fcn_set);
	FunctionDescription desc;
	desc.description = IS_MAX ? "[INTERNAL] Returns 64 PAC subsample max values with per-level clipping as LIST."
	                          : "[INTERNAL] Returns 64 PAC subsample min values with per-level clipping as LIST.";
	info.descriptions.push_back(std::move(desc));
	loader.RegisterFunction(std::move(info));
}

template <bool IS_MAX>
static void RegisterPacNoisedClipMinMaxFunctions(ExtensionLoader &loader) {
	const string name = IS_MAX ? "pac_noised_clip_max" : "pac_noised_clip_min";
	AggregateFunctionSet fcn_set(name);
	RegisterClipMinMaxTypeOverloads<IS_MAX>(fcn_set, name, false);

	// DECIMAL overloads
	fcn_set.AddFunction(AggregateFunction(
	    {LogicalType::UBIGINT, LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr, nullptr,
	    nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, BindDecimalPacNoisedClipMinMax<IS_MAX>));
	fcn_set.AddFunction(AggregateFunction({LogicalType::UBIGINT, LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                      LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                      FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                                      BindDecimalPacNoisedClipMinMax<IS_MAX>));

	AddFloatDoubleOverloads<IS_MAX>(fcn_set, name, false);

	CreateAggregateFunctionInfo info(fcn_set);
	FunctionDescription desc;
	desc.description = IS_MAX ? "Privacy-preserving MAX with per-level clipping and noising."
	                          : "Privacy-preserving MIN with per-level clipping and noising.";
	info.descriptions.push_back(std::move(desc));
	loader.RegisterFunction(std::move(info));
}

// Public registration functions (called from pac_extension.cpp)
void RegisterPacClipMinFunctions(ExtensionLoader &loader) {
	RegisterPacClipMinMaxCountersFunctions<false>(loader);
}
void RegisterPacClipMaxFunctions(ExtensionLoader &loader) {
	RegisterPacClipMinMaxCountersFunctions<true>(loader);
}
void RegisterPacNoisedClipMinFunctions(ExtensionLoader &loader) {
	RegisterPacNoisedClipMinMaxFunctions<false>(loader);
}
void RegisterPacNoisedClipMaxFunctions(ExtensionLoader &loader) {
	RegisterPacNoisedClipMinMaxFunctions<true>(loader);
}

} // namespace duckdb
