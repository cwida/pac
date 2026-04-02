#include "aggregates/pac_clip_sum.hpp"
#include "categorical/pac_categorical.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <cmath>

namespace duckdb {

// ============================================================================
// Inner state update: add one unsigned value to the state
// ============================================================================
template <int NL = CLIP_NUM_LEVELS_64>
AUTOVECTORIZE inline void PacClipSumUpdateOneInternal(PacClipSumIntState<NL> &state, uint64_t key_hash, uint64_t value,
                                                      ArenaAllocator &allocator) {
	state.key_hash |= key_hash;

	int level = PacClipSumIntState<NL>::GetLevel(value);
	uint64_t shift = level << 1;
	uint16_t shifted_val = static_cast<uint16_t>(value >> shift); // max 255 (8 bits)

	state.EnsureLevelAllocated(allocator, level);
	uint64_t *buf = state.levels[level];

	// Set bitmap bit
	buf[17] |= (1ULL << (key_hash >> 58));

	// Update exact_count (may cascade top 4 bits to overflow)
	state.AddToExactCount(buf, shifted_val, allocator);

	// Add to SWAR counters
	Pac2AddToTotalsSWAR16(buf, shifted_val, key_hash);
}

// Overload for hugeint_t
template <int NL = CLIP_NUM_LEVELS_64>
AUTOVECTORIZE inline void PacClipSumUpdateOneInternal(PacClipSumIntState<NL> &state, uint64_t key_hash, hugeint_t value,
                                                      ArenaAllocator &allocator) {
	state.key_hash |= key_hash;

	uint64_t upper, lower;
	if (value.upper < 0) {
		hugeint_t abs_val = -value;
		upper = static_cast<uint64_t>(abs_val.upper);
		lower = abs_val.lower;
	} else {
		upper = static_cast<uint64_t>(value.upper);
		lower = value.lower;
	}

	int level = PacClipSumIntState<NL>::GetLevel128(upper, lower);
	uint64_t shift = level << 1;

	// Shift the 128-bit value right by shift bits, take lower 8 bits
	uint16_t shifted_val;
	if (shift >= 64) {
		shifted_val = static_cast<uint16_t>(upper >> (shift - 64));
	} else if (shift > 0) {
		shifted_val = static_cast<uint16_t>((lower >> shift) | (upper << (64 - shift)));
	} else {
		shifted_val = static_cast<uint16_t>(lower);
	}
	shifted_val &= 0xFF; // max 255

	state.EnsureLevelAllocated(allocator, level);
	uint64_t *buf = state.levels[level];
	buf[17] |= (1ULL << (key_hash >> 58));
	state.AddToExactCount(buf, shifted_val, allocator);
	Pac2AddToTotalsSWAR16(buf, shifted_val, key_hash);
}

// ============================================================================
// Value routing: two-sided (pos/neg) dispatch
// ============================================================================
// Route a uint64_t value — when SIGNED, the bits represent a signed int64_t (two's complement)
template <int NL = CLIP_NUM_LEVELS_64, bool SIGNED = true>
inline void PacClipSumRouteValue(PacClipSumStateWrapper<NL> &wrapper, PacClipSumIntState<NL> *pos_state, uint64_t hash,
                                 uint64_t value, ArenaAllocator &a) {
	if (DUCKDB_LIKELY(hash)) {
		int64_t sval = static_cast<int64_t>(value); // reinterpret bits as signed
		if (SIGNED && sval < 0) {
			auto *neg = wrapper.EnsureNegState(a);
			PacClipSumUpdateOneInternal(*neg, hash, static_cast<uint64_t>(-sval), a);
			neg->update_count++;
		} else {
			PacClipSumUpdateOneInternal(*pos_state, hash, value, a);
			pos_state->update_count++;
		}
	}
}

// Overload for hugeint routing (signed)
template <int NL = CLIP_NUM_LEVELS_64>
inline void PacClipSumRouteHugeint(PacClipSumStateWrapper<NL> &wrapper, PacClipSumIntState<NL> *pos_state,
                                   uint64_t hash, hugeint_t value, ArenaAllocator &a, bool is_signed) {
	if (DUCKDB_LIKELY(hash)) {
		if (is_signed && value.upper < 0) {
			auto *neg = wrapper.EnsureNegState(a);
			hugeint_t abs_val = -value;
			uint64_t upper = static_cast<uint64_t>(abs_val.upper);
			uint64_t lower = abs_val.lower;
			int level = PacClipSumIntState<NL>::GetLevel128(upper, lower);
			uint64_t shift = level << 1;
			uint16_t shifted_val;
			if (shift >= 64) {
				shifted_val = static_cast<uint16_t>(upper >> (shift - 64));
			} else if (shift > 0) {
				shifted_val = static_cast<uint16_t>((lower >> shift) | (upper << (64 - shift)));
			} else {
				shifted_val = static_cast<uint16_t>(lower);
			}
			shifted_val &= 0xFF;
			neg->key_hash |= hash;
			neg->EnsureLevelAllocated(a, level);
			uint64_t *lbuf = neg->levels[level];
			lbuf[17] |= (1ULL << (hash >> 58));
			neg->AddToExactCount(lbuf, shifted_val, a);
			Pac2AddToTotalsSWAR16(lbuf, shifted_val, hash);
			neg->update_count++;
		} else {
			PacClipSumUpdateOneInternal(*pos_state, hash, value, a);
			pos_state->update_count++;
		}
	}
}

// ============================================================================
// Buffer flush
// ============================================================================
template <int NL = CLIP_NUM_LEVELS_64, bool SIGNED = true>
inline void PacClipSumFlushBuffer(PacClipSumStateWrapper<NL> &src, PacClipSumStateWrapper<NL> &dst, ArenaAllocator &a) {
	uint64_t cnt = src.n_buffered & PacClipSumStateWrapper<NL>::BUF_MASK;
	if (cnt > 0) {
		auto *dst_state = dst.EnsureState(a);
		for (uint64_t i = 0; i < cnt; i++) {
			PacClipSumRouteValue<NL, SIGNED>(dst, dst_state, src.hash_buf[i], src.val_buf[i], a);
		}
		src.n_buffered &= ~PacClipSumStateWrapper<NL>::BUF_MASK;
	}
}

// ============================================================================
// Buffered update
// ============================================================================
template <int NL = CLIP_NUM_LEVELS_64, bool SIGNED = true, typename ValueT = uint64_t>
AUTOVECTORIZE inline void PacClipSumUpdateOne(PacClipSumStateWrapper<NL> &agg, uint64_t key_hash, ValueT value,
                                              ArenaAllocator &a) {
	uint64_t cnt = agg.n_buffered & PacClipSumStateWrapper<NL>::BUF_MASK;
	if (DUCKDB_UNLIKELY(cnt == PacClipSumStateWrapper<NL>::BUF_SIZE)) {
		auto *dst_state = agg.EnsureState(a);
		for (int i = 0; i < PacClipSumStateWrapper<NL>::BUF_SIZE; i++) {
			PacClipSumRouteValue<NL, SIGNED>(agg, dst_state, agg.hash_buf[i], agg.val_buf[i], a);
		}
		PacClipSumRouteValue<NL, SIGNED>(agg, dst_state, key_hash, static_cast<uint64_t>(value), a);
		agg.n_buffered &= ~PacClipSumStateWrapper<NL>::BUF_MASK;
	} else {
		agg.val_buf[cnt] = static_cast<uint64_t>(value);
		agg.hash_buf[cnt] = key_hash;
		agg.n_buffered++;
	}
}

// Hugeint buffered update — bypass buffer, update directly
template <int NL = CLIP_NUM_LEVELS_64, bool SIGNED = true>
inline void PacClipSumUpdateOne(PacClipSumStateWrapper<NL> &agg, uint64_t key_hash, hugeint_t value,
                                ArenaAllocator &a) {
	PacClipSumFlushBuffer<NL, SIGNED>(agg, agg, a); // flush any buffered values first
	auto *state = agg.EnsureState(a);
	PacClipSumRouteHugeint(agg, state, key_hash, value, a, SIGNED);
}

// ============================================================================
// Vectorized Update and ScatterUpdate
// ============================================================================
template <int NL = CLIP_NUM_LEVELS_64, bool SIGNED = true, class VALUE_TYPE = uint64_t, class INPUT_TYPE = uint64_t>
static void PacClipSumUpdate(Vector inputs[], PacClipSumStateWrapper<NL> &state, idx_t count,
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
			PacClipSumUpdateOne<NL, SIGNED>(state, hashes[h_idx], ConvertValue<VALUE_TYPE>::convert(values[v_idx]),
			                                allocator);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
				continue;
			}
			PacClipSumUpdateOne<NL, SIGNED>(state, hashes[h_idx], ConvertValue<VALUE_TYPE>::convert(values[v_idx]),
			                                allocator);
		}
	}
}

template <int NL = CLIP_NUM_LEVELS_64, bool SIGNED = true, class VALUE_TYPE = uint64_t, class INPUT_TYPE = uint64_t>
static void PacClipSumScatterUpdate(Vector inputs[], Vector &states, idx_t count, ArenaAllocator &allocator) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);

	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipSumStateWrapper<NL> *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		PacClipSumUpdateOne<NL, SIGNED>(*state, hashes[h_idx], ConvertValue<VALUE_TYPE>::convert(values[v_idx]),
		                                allocator);
	}
}

// ============================================================================
// X-macro: generate Update/ScatterUpdate for integer types
// ============================================================================
#define CLIP_SUM_INT_TYPES_SIGNED                                                                                      \
	X(TinyInt, int64_t, int8_t, true)                                                                                  \
	X(SmallInt, int64_t, int16_t, true)                                                                                \
	X(Integer, int64_t, int32_t, true)                                                                                 \
	X(BigInt, int64_t, int64_t, true)

#define CLIP_SUM_INT_TYPES_UNSIGNED                                                                                    \
	X(UTinyInt, uint64_t, uint8_t, false)                                                                              \
	X(USmallInt, uint64_t, uint16_t, false)                                                                            \
	X(UInteger, uint64_t, uint32_t, false)                                                                             \
	X(UBigInt, uint64_t, uint64_t, false)

#define X(NAME, VALUE_T, INPUT_T, SIGNED)                                                                              \
	static void PacClipSumUpdate##NAME(Vector input[], AggregateInputData &agg, idx_t, data_ptr_t state_p,             \
	                                   idx_t cnt) {                                                                    \
		auto &state = *reinterpret_cast<PacClipSumStateWrapper<> *>(state_p);                                          \
		PacClipSumUpdate<CLIP_NUM_LEVELS_64, SIGNED, VALUE_T, INPUT_T>(input, state, cnt, agg.allocator);              \
	}                                                                                                                  \
	static void PacClipSumScatterUpdate##NAME(Vector input[], AggregateInputData &agg, idx_t, Vector &sts,             \
	                                          idx_t cnt) {                                                             \
		PacClipSumScatterUpdate<CLIP_NUM_LEVELS_64, SIGNED, VALUE_T, INPUT_T>(input, sts, cnt, agg.allocator);         \
	}
CLIP_SUM_INT_TYPES_SIGNED
CLIP_SUM_INT_TYPES_UNSIGNED
#undef X

// HugeInt update (signed, via hugeint routing — 128-bit needs full 62 levels)
static void PacClipSumUpdateHugeInt(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p, idx_t count) {
	auto &state = *reinterpret_cast<PacClipSumStateWrapper<CLIP_NUM_LEVELS> *>(state_p);
	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<hugeint_t>(value_data);
	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		PacClipSumUpdateOne<CLIP_NUM_LEVELS, true>(state, hashes[h_idx], values[v_idx], aggr.allocator);
	}
}
static void PacClipSumScatterUpdateHugeInt(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states,
                                           idx_t count) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<hugeint_t>(value_data);
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipSumStateWrapper<CLIP_NUM_LEVELS> *>(sdata);
	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		PacClipSumUpdateOne<CLIP_NUM_LEVELS, true>(*state, hashes[h_idx], values[v_idx], aggr.allocator);
	}
}

// UHugeInt update (unsigned, convert to hugeint for routing — 128-bit needs full 62 levels)
static void PacClipSumUpdateUHugeInt(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p,
                                     idx_t count) {
	auto &state = *reinterpret_cast<PacClipSumStateWrapper<CLIP_NUM_LEVELS> *>(state_p);
	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<uhugeint_t>(value_data);
	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		// uhugeint_t is always positive; treat as 128-bit unsigned
		auto &v = values[v_idx];
		auto *pos_state = state.EnsureState(aggr.allocator);
		if (DUCKDB_LIKELY(hashes[h_idx])) {
			uint64_t upper = static_cast<uint64_t>(v.upper);
			uint64_t lower = v.lower;
			int level = PacClipSumIntState<CLIP_NUM_LEVELS>::GetLevel128(upper, lower);
			uint64_t shift = level << 1;
			uint16_t shifted_val;
			if (shift >= 64) {
				shifted_val = static_cast<uint16_t>(upper >> (shift - 64));
			} else if (shift > 0) {
				shifted_val = static_cast<uint16_t>((lower >> shift) | (upper << (64 - shift)));
			} else {
				shifted_val = static_cast<uint16_t>(lower);
			}
			shifted_val &= 0xFF;
			pos_state->key_hash |= hashes[h_idx];
			pos_state->EnsureLevelAllocated(aggr.allocator, level);
			uint64_t *buf = pos_state->levels[level];
			buf[17] |= (1ULL << (hashes[h_idx] >> 58));
			pos_state->AddToExactCount(buf, shifted_val, aggr.allocator);
			Pac2AddToTotalsSWAR16(buf, shifted_val, hashes[h_idx]);
			pos_state->update_count++;
		}
	}
}
static void PacClipSumScatterUpdateUHugeInt(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states,
                                            idx_t count) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<uhugeint_t>(value_data);
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipSumStateWrapper<CLIP_NUM_LEVELS> *>(sdata);
	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		auto &v = values[v_idx];
		auto *pos_state = state->EnsureState(aggr.allocator);
		if (DUCKDB_LIKELY(hashes[h_idx])) {
			uint64_t upper = static_cast<uint64_t>(v.upper);
			uint64_t lower = v.lower;
			int level = PacClipSumIntState<CLIP_NUM_LEVELS>::GetLevel128(upper, lower);
			uint64_t shift = level << 1;
			uint16_t shifted_val;
			if (shift >= 64) {
				shifted_val = static_cast<uint16_t>(upper >> (shift - 64));
			} else if (shift > 0) {
				shifted_val = static_cast<uint16_t>((lower >> shift) | (upper << (64 - shift)));
			} else {
				shifted_val = static_cast<uint16_t>(lower);
			}
			shifted_val &= 0xFF;
			pos_state->key_hash |= hashes[h_idx];
			pos_state->EnsureLevelAllocated(aggr.allocator, level);
			uint64_t *buf = pos_state->levels[level];
			buf[17] |= (1ULL << (hashes[h_idx] >> 58));
			pos_state->AddToExactCount(buf, shifted_val, aggr.allocator);
			Pac2AddToTotalsSWAR16(buf, shifted_val, hashes[h_idx]);
			pos_state->update_count++;
		}
	}
}

// ============================================================================
// Float/Double update: scale to int64, route through signed path
// ============================================================================
template <typename FLOAT_TYPE, int SHIFT>
static void PacClipSumUpdateFloat(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p, idx_t count) {
	auto &state = *reinterpret_cast<PacClipSumStateWrapper<> *>(state_p);
	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<FLOAT_TYPE>(value_data);

	if (hash_data.validity.AllValid() && value_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			PacClipSumUpdateOne<CLIP_NUM_LEVELS_64, true>(
			    state, hashes[h_idx], ScaleFloatToInt64<FLOAT_TYPE, SHIFT>(values[v_idx]), aggr.allocator);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
				continue;
			}
			PacClipSumUpdateOne<CLIP_NUM_LEVELS_64, true>(
			    state, hashes[h_idx], ScaleFloatToInt64<FLOAT_TYPE, SHIFT>(values[v_idx]), aggr.allocator);
		}
	}
}

template <typename FLOAT_TYPE, int SHIFT>
static void PacClipSumScatterUpdateFloat(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states,
                                         idx_t count) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<FLOAT_TYPE>(value_data);
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipSumStateWrapper<> *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		PacClipSumUpdateOne<CLIP_NUM_LEVELS_64, true>(
		    *state, hashes[h_idx], ScaleFloatToInt64<FLOAT_TYPE, SHIFT>(values[v_idx]), aggr.allocator);
	}
}

// Instantiate float/double update functions
static void PacClipSumUpdateSingleFloat(Vector inputs[], AggregateInputData &aggr, idx_t n, data_ptr_t state_p,
                                        idx_t count) {
	PacClipSumUpdateFloat<float, CLIP_FLOAT_SHIFT>(inputs, aggr, n, state_p, count);
}
static void PacClipSumScatterUpdateSingleFloat(Vector inputs[], AggregateInputData &aggr, idx_t n, Vector &states,
                                               idx_t count) {
	PacClipSumScatterUpdateFloat<float, CLIP_FLOAT_SHIFT>(inputs, aggr, n, states, count);
}
static void PacClipSumUpdateSingleDouble(Vector inputs[], AggregateInputData &aggr, idx_t n, data_ptr_t state_p,
                                         idx_t count) {
	PacClipSumUpdateFloat<double, CLIP_DOUBLE_SHIFT>(inputs, aggr, n, state_p, count);
}
static void PacClipSumScatterUpdateSingleDouble(Vector inputs[], AggregateInputData &aggr, idx_t n, Vector &states,
                                                idx_t count) {
	PacClipSumScatterUpdateFloat<double, CLIP_DOUBLE_SHIFT>(inputs, aggr, n, states, count);
}

// ============================================================================
// Combine
// ============================================================================
template <int NL = CLIP_NUM_LEVELS_64>
AUTOVECTORIZE static void PacClipSumCombineInt(Vector &src, Vector &dst, idx_t count, ArenaAllocator &allocator) {
	auto src_wrapper = FlatVector::GetData<PacClipSumStateWrapper<NL> *>(src);
	auto dst_wrapper = FlatVector::GetData<PacClipSumStateWrapper<NL> *>(dst);

	for (idx_t i = 0; i < count; i++) {
		// Flush src's buffer into dst
		PacClipSumFlushBuffer<NL, true>(*src_wrapper[i], *dst_wrapper[i], allocator);

		auto *s = src_wrapper[i]->GetState();
		if (!s) {
			continue;
		}
		auto *d = dst_wrapper[i]->EnsureState(allocator);
		d->CombineFrom(s, allocator);

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

static void PacClipSumCombine(Vector &src, Vector &dst, AggregateInputData &aggr, idx_t count) {
	PacClipSumCombineInt<>(src, dst, count, aggr.allocator);
}
static void PacClipSumCombine128(Vector &src, Vector &dst, AggregateInputData &aggr, idx_t count) {
	PacClipSumCombineInt<CLIP_NUM_LEVELS>(src, dst, count, aggr.allocator);
}

// PacClipBindData is defined in pac_clip_aggr.hpp

// ============================================================================
// Finalize
// ============================================================================
template <int NL = CLIP_NUM_LEVELS_64, class ACC_TYPE = hugeint_t, bool SIGNED = true>
static void PacClipSumFinalize(Vector &states, AggregateInputData &input, Vector &result, idx_t count, idx_t offset) {
	auto state_ptrs = FlatVector::GetData<PacClipSumStateWrapper<NL> *>(states);
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
		PacClipSumFlushBuffer<NL, SIGNED>(*state_ptrs[i], *state_ptrs[i], input.allocator);

		PAC_FLOAT buf[64] = {0};
		auto *pos = state_ptrs[i]->GetState();
		if (!pos) {
			result_mask.SetInvalid(offset + i);
			continue;
		}
		uint64_t key_hash = pos->key_hash;
		std::mt19937_64 gen(bind.seed);
		if (PacNoiseInNull(key_hash, mi, correction, gen)) {
			result_mask.SetInvalid(offset + i);
			continue;
		}

		// Non-mutating: just read totals with clip_support filtering
		pos->GetTotals(buf, clip_support, clip_scale);
		uint64_t update_count = pos->update_count;

		// Subtract neg state
		auto *neg = state_ptrs[i]->GetNegState();
		if (neg) {
			PAC_FLOAT neg_buf[64] = {0};
			neg->GetTotals(neg_buf, clip_support, clip_scale);
			key_hash |= neg->key_hash;
			for (int j = 0; j < 64; j++) {
				buf[j] -= neg_buf[j];
			}
			update_count += neg->update_count;
		}

		CheckPacSampleDiversity(key_hash, buf, update_count, "pac_clip_sum", bind);
		double noise_var = 0.0;
		PAC_FLOAT result_val =
		    PacNoisySampleFrom64Counters(buf, mi, correction, gen, ~key_hash, query_hash, pstate, &noise_var);
		result_val *= PAC_FLOAT(2.0);                           // 2x compensation for ~50% sampling
		result_val /= static_cast<PAC_FLOAT>(bind.float_scale); // undo float→int64 scaling (1.0 for integers)
		// Utility NULLing: noise variance scales by 4x (2x compensation means 4x on variance)
		double utility_threshold = bind.utility_threshold;
		if (PacUtilityNull(static_cast<double>(result_val), noise_var * 4.0, utility_threshold, gen)) {
			result_mask.SetInvalid(offset + i);
			continue;
		}
		data[offset + i] = FromDouble<ACC_TYPE>(result_val);
	}
}

// Instantiate noised finalize (scalar output for pac_noised_clip_sum)
// 64-bit types
static void PacClipSumNoisedFinalizeSigned(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                           idx_t offset) {
	PacClipSumFinalize<CLIP_NUM_LEVELS_64, hugeint_t, true>(states, input, result, count, offset);
}
static void PacClipSumNoisedFinalizeUnsigned(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                             idx_t offset) {
	PacClipSumFinalize<CLIP_NUM_LEVELS_64, hugeint_t, false>(states, input, result, count, offset);
}
// 128-bit types
static void PacClipSumNoisedFinalizeSigned128(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                              idx_t offset) {
	PacClipSumFinalize<CLIP_NUM_LEVELS, hugeint_t, true>(states, input, result, count, offset);
}
static void PacClipSumNoisedFinalizeUnsigned128(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                                idx_t offset) {
	PacClipSumFinalize<CLIP_NUM_LEVELS, hugeint_t, false>(states, input, result, count, offset);
}
// BIGINT output variant — used for count→sum conversion where the original returned BIGINT
static void PacClipSumNoisedFinalizeBigInt(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                           idx_t offset) {
	PacClipSumFinalize<CLIP_NUM_LEVELS_64, int64_t, true>(states, input, result, count, offset);
}
// Float/double output variants
static void PacClipSumNoisedFinalizeFloat(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                          idx_t offset) {
	PacClipSumFinalize<CLIP_NUM_LEVELS_64, float, true>(states, input, result, count, offset);
}
static void PacClipSumNoisedFinalizeDouble(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                           idx_t offset) {
	PacClipSumFinalize<CLIP_NUM_LEVELS_64, double, true>(states, input, result, count, offset);
}

// ============================================================================
// Counters finalize (LIST<FLOAT> output for pac_clip_sum)
// ============================================================================
template <int NL = CLIP_NUM_LEVELS_64, bool SIGNED = true>
static void PacClipSumFinalizeCounters(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                       idx_t offset) {
	auto state_ptrs = FlatVector::GetData<PacClipSumStateWrapper<NL> *>(states);
	auto &bind = static_cast<PacClipBindData &>(*input.bind_data);
	int clip_support = bind.clip_support_threshold;
	double correction = bind.correction;
	double float_scale = bind.float_scale;
	bool clip_scale = bind.clip_scale;

	// Result is LIST<FLOAT>
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &child_vec = ListVector::GetEntry(result);

	idx_t total_elements = count * 64;
	ListVector::Reserve(result, total_elements);
	ListVector::SetListSize(result, total_elements);

	auto child_data = FlatVector::GetData<PAC_FLOAT>(child_vec);

	for (idx_t i = 0; i < count; i++) {
		PacClipSumFlushBuffer<NL, SIGNED>(*state_ptrs[i], *state_ptrs[i], input.allocator);

		list_entries[offset + i].offset = i * 64;
		list_entries[offset + i].length = 64;

		PAC_FLOAT buf[64] = {0};
		uint64_t key_hash = 0;
		uint64_t update_count = 0;

		auto *pos = state_ptrs[i]->GetState();
		if (pos) {
			key_hash = pos->key_hash;
			update_count = pos->update_count;
			pos->GetTotals(buf, clip_support, clip_scale);

			auto *neg = state_ptrs[i]->GetNegState();
			if (neg) {
				PAC_FLOAT neg_buf[64] = {0};
				neg->GetTotals(neg_buf, clip_support, clip_scale);
				key_hash |= neg->key_hash;
				for (int j = 0; j < 64; j++) {
					buf[j] -= neg_buf[j];
				}
				update_count += neg->update_count;
			}
		}

		CheckPacSampleDiversity(key_hash, buf, update_count, "pac_clip_sum", bind);

		idx_t base = i * 64;
		for (int j = 0; j < 64; j++) {
			if ((key_hash >> j) & 1ULL) {
				child_data[base + j] = static_cast<PAC_FLOAT>(buf[j] * 2.0 * correction / float_scale);
			} else {
				child_data[base + j] = 0.0;
			}
		}
	}
}

// 64-bit counters finalize
static void PacClipSumFinalizeCountersSigned(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                             idx_t offset) {
	PacClipSumFinalizeCounters<CLIP_NUM_LEVELS_64, true>(states, input, result, count, offset);
}
static void PacClipSumFinalizeCountersUnsigned(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                               idx_t offset) {
	PacClipSumFinalizeCounters<CLIP_NUM_LEVELS_64, false>(states, input, result, count, offset);
}
// 128-bit counters finalize
static void PacClipSumFinalizeCountersSigned128(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                                idx_t offset) {
	PacClipSumFinalizeCounters<CLIP_NUM_LEVELS, true>(states, input, result, count, offset);
}
static void PacClipSumFinalizeCountersUnsigned128(Vector &states, AggregateInputData &input, Vector &result,
                                                  idx_t count, idx_t offset) {
	PacClipSumFinalizeCounters<CLIP_NUM_LEVELS, false>(states, input, result, count, offset);
}

// ============================================================================
// State size / init / bind
// ============================================================================
static idx_t PacClipSumStateSize(const AggregateFunction &) {
	return sizeof(PacClipSumStateWrapper<>);
}
static idx_t PacClipSumStateSize128(const AggregateFunction &) {
	return sizeof(PacClipSumStateWrapper<CLIP_NUM_LEVELS>);
}

static void PacClipSumInitialize(const AggregateFunction &, data_ptr_t state_p) {
	memset(state_p, 0, sizeof(PacClipSumStateWrapper<>));
}
static void PacClipSumInitialize128(const AggregateFunction &, data_ptr_t state_p) {
	memset(state_p, 0, sizeof(PacClipSumStateWrapper<CLIP_NUM_LEVELS>));
}

// PacClipBind, PacClipBindFloat, PacClipBindDouble are defined in pac_clip_aggr.hpp

// ============================================================================
// DECIMAL support: dispatch by physical type, same pattern as pac_noised_sum
// ============================================================================
static AggregateFunction GetPacClipSumNoisedAggregate(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction("pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::SMALLINT},
		                         LogicalType::HUGEINT, PacClipSumStateSize, PacClipSumInitialize,
		                         PacClipSumScatterUpdateSmallInt, PacClipSumCombine, PacClipSumNoisedFinalizeSigned,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSmallInt);
	case PhysicalType::INT32:
		return AggregateFunction("pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::INTEGER},
		                         LogicalType::HUGEINT, PacClipSumStateSize, PacClipSumInitialize,
		                         PacClipSumScatterUpdateInteger, PacClipSumCombine, PacClipSumNoisedFinalizeSigned,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateInteger);
	case PhysicalType::INT64:
		return AggregateFunction("pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::BIGINT},
		                         LogicalType::HUGEINT, PacClipSumStateSize, PacClipSumInitialize,
		                         PacClipSumScatterUpdateBigInt, PacClipSumCombine, PacClipSumNoisedFinalizeSigned,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateBigInt);
	case PhysicalType::INT128:
		return AggregateFunction(
		    "pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::HUGEINT}, LogicalType::HUGEINT,
		    PacClipSumStateSize128, PacClipSumInitialize128, PacClipSumScatterUpdateHugeInt, PacClipSumCombine128,
		    PacClipSumNoisedFinalizeSigned128, FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateHugeInt);
	default:
		throw InternalException("pac_noised_clip_sum: unsupported decimal physical type");
	}
}

static AggregateFunction GetPacClipSumCountersAggregate(PhysicalType type) {
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction("pac_clip_sum", {LogicalType::UBIGINT, LogicalType::SMALLINT}, list_type,
		                         PacClipSumStateSize, PacClipSumInitialize, PacClipSumScatterUpdateSmallInt,
		                         PacClipSumCombine, PacClipSumFinalizeCountersSigned,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSmallInt);
	case PhysicalType::INT32:
		return AggregateFunction("pac_clip_sum", {LogicalType::UBIGINT, LogicalType::INTEGER}, list_type,
		                         PacClipSumStateSize, PacClipSumInitialize, PacClipSumScatterUpdateInteger,
		                         PacClipSumCombine, PacClipSumFinalizeCountersSigned,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateInteger);
	case PhysicalType::INT64:
		return AggregateFunction("pac_clip_sum", {LogicalType::UBIGINT, LogicalType::BIGINT}, list_type,
		                         PacClipSumStateSize, PacClipSumInitialize, PacClipSumScatterUpdateBigInt,
		                         PacClipSumCombine, PacClipSumFinalizeCountersSigned,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateBigInt);
	case PhysicalType::INT128:
		return AggregateFunction("pac_clip_sum", {LogicalType::UBIGINT, LogicalType::HUGEINT}, list_type,
		                         PacClipSumStateSize128, PacClipSumInitialize128, PacClipSumScatterUpdateHugeInt,
		                         PacClipSumCombine128, PacClipSumFinalizeCountersSigned128,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateHugeInt);
	default:
		throw InternalException("pac_clip_sum: unsupported decimal physical type");
	}
}

static unique_ptr<FunctionData> BindDecimalPacNoisedClipSum(ClientContext &ctx, AggregateFunction &function,
                                                            vector<unique_ptr<Expression>> &args) {
	auto decimal_type = args[1]->return_type;
	function = GetPacClipSumNoisedAggregate(decimal_type.InternalType());
	function.name = "pac_noised_clip_sum";
	function.arguments[1] = decimal_type;
	function.return_type = LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, DecimalType::GetScale(decimal_type));
	return PacClipBind(ctx, function, args);
}

static unique_ptr<FunctionData> BindDecimalPacClipSum(ClientContext &ctx, AggregateFunction &function,
                                                      vector<unique_ptr<Expression>> &args) {
	auto decimal_type = args[1]->return_type;
	function = GetPacClipSumCountersAggregate(decimal_type.InternalType());
	function.name = "pac_clip_sum";
	function.arguments[1] = decimal_type;
	// counters always return LIST<FLOAT>, no DECIMAL return type needed
	return PacClipBind(ctx, function, args);
}

// ============================================================================
// Registration helpers
// ============================================================================
static void AddClipSumCountersFcn(AggregateFunctionSet &set, const string &name, const LogicalType &value_type,
                                  aggregate_update_t scatter, aggregate_finalize_t finalize,
                                  aggregate_simple_update_t update) {
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type}, list_type, PacClipSumStateSize,
	                                  PacClipSumInitialize, scatter, PacClipSumCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipBind));
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type, LogicalType::DOUBLE}, list_type,
	                                  PacClipSumStateSize, PacClipSumInitialize, scatter, PacClipSumCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipBind));
}

static void AddNoisedClipSumFcn(AggregateFunctionSet &set, const string &name, const LogicalType &value_type,
                                const LogicalType &result_type, aggregate_update_t scatter,
                                aggregate_finalize_t finalize, aggregate_simple_update_t update) {
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type}, result_type, PacClipSumStateSize,
	                                  PacClipSumInitialize, scatter, PacClipSumCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipBind));
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type, LogicalType::DOUBLE}, result_type,
	                                  PacClipSumStateSize, PacClipSumInitialize, scatter, PacClipSumCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipBind));
}

// Helper to register all type overloads for a clip sum function set
static void RegisterClipSumTypeOverloads(AggregateFunctionSet &set, const string &name, bool counters) {
	if (counters) {
		// Counters (LIST<FLOAT>) variants
		AddClipSumCountersFcn(set, name, LogicalType::TINYINT, PacClipSumScatterUpdateTinyInt,
		                      PacClipSumFinalizeCountersSigned, PacClipSumUpdateTinyInt);
		AddClipSumCountersFcn(set, name, LogicalType::BOOLEAN, PacClipSumScatterUpdateTinyInt,
		                      PacClipSumFinalizeCountersSigned, PacClipSumUpdateTinyInt);
		AddClipSumCountersFcn(set, name, LogicalType::SMALLINT, PacClipSumScatterUpdateSmallInt,
		                      PacClipSumFinalizeCountersSigned, PacClipSumUpdateSmallInt);
		AddClipSumCountersFcn(set, name, LogicalType::INTEGER, PacClipSumScatterUpdateInteger,
		                      PacClipSumFinalizeCountersSigned, PacClipSumUpdateInteger);
		AddClipSumCountersFcn(set, name, LogicalType::BIGINT, PacClipSumScatterUpdateBigInt,
		                      PacClipSumFinalizeCountersSigned, PacClipSumUpdateBigInt);
		AddClipSumCountersFcn(set, name, LogicalType::UTINYINT, PacClipSumScatterUpdateUTinyInt,
		                      PacClipSumFinalizeCountersUnsigned, PacClipSumUpdateUTinyInt);
		AddClipSumCountersFcn(set, name, LogicalType::USMALLINT, PacClipSumScatterUpdateUSmallInt,
		                      PacClipSumFinalizeCountersUnsigned, PacClipSumUpdateUSmallInt);
		AddClipSumCountersFcn(set, name, LogicalType::UINTEGER, PacClipSumScatterUpdateUInteger,
		                      PacClipSumFinalizeCountersUnsigned, PacClipSumUpdateUInteger);
		AddClipSumCountersFcn(set, name, LogicalType::UBIGINT, PacClipSumScatterUpdateUBigInt,
		                      PacClipSumFinalizeCountersUnsigned, PacClipSumUpdateUBigInt);
		// HUGEINT/UHUGEINT: use 128-bit state (62 levels)
		{
			auto lt = LogicalType::LIST(PacFloatLogicalType());
			set.AddFunction(AggregateFunction(
			    name, {LogicalType::UBIGINT, LogicalType::HUGEINT}, lt, PacClipSumStateSize128, PacClipSumInitialize128,
			    PacClipSumScatterUpdateHugeInt, PacClipSumCombine128, PacClipSumFinalizeCountersSigned128,
			    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateHugeInt, PacClipBind));
			set.AddFunction(
			    AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::UHUGEINT}, lt, PacClipSumStateSize128,
			                      PacClipSumInitialize128, PacClipSumScatterUpdateUHugeInt, PacClipSumCombine128,
			                      PacClipSumFinalizeCountersUnsigned128, FunctionNullHandling::DEFAULT_NULL_HANDLING,
			                      PacClipSumUpdateUHugeInt, PacClipBind));
		}
	} else {
		// Noised (scalar HUGEINT) variants
		AddNoisedClipSumFcn(set, name, LogicalType::TINYINT, LogicalType::HUGEINT, PacClipSumScatterUpdateTinyInt,
		                    PacClipSumNoisedFinalizeSigned, PacClipSumUpdateTinyInt);
		AddNoisedClipSumFcn(set, name, LogicalType::BOOLEAN, LogicalType::HUGEINT, PacClipSumScatterUpdateTinyInt,
		                    PacClipSumNoisedFinalizeSigned, PacClipSumUpdateTinyInt);
		AddNoisedClipSumFcn(set, name, LogicalType::SMALLINT, LogicalType::HUGEINT, PacClipSumScatterUpdateSmallInt,
		                    PacClipSumNoisedFinalizeSigned, PacClipSumUpdateSmallInt);
		AddNoisedClipSumFcn(set, name, LogicalType::INTEGER, LogicalType::HUGEINT, PacClipSumScatterUpdateInteger,
		                    PacClipSumNoisedFinalizeSigned, PacClipSumUpdateInteger);
		AddNoisedClipSumFcn(set, name, LogicalType::BIGINT, LogicalType::HUGEINT, PacClipSumScatterUpdateBigInt,
		                    PacClipSumNoisedFinalizeSigned, PacClipSumUpdateBigInt);
		AddNoisedClipSumFcn(set, name, LogicalType::UTINYINT, LogicalType::HUGEINT, PacClipSumScatterUpdateUTinyInt,
		                    PacClipSumNoisedFinalizeUnsigned, PacClipSumUpdateUTinyInt);
		AddNoisedClipSumFcn(set, name, LogicalType::USMALLINT, LogicalType::HUGEINT, PacClipSumScatterUpdateUSmallInt,
		                    PacClipSumNoisedFinalizeUnsigned, PacClipSumUpdateUSmallInt);
		AddNoisedClipSumFcn(set, name, LogicalType::UINTEGER, LogicalType::HUGEINT, PacClipSumScatterUpdateUInteger,
		                    PacClipSumNoisedFinalizeUnsigned, PacClipSumUpdateUInteger);
		AddNoisedClipSumFcn(set, name, LogicalType::UBIGINT, LogicalType::HUGEINT, PacClipSumScatterUpdateUBigInt,
		                    PacClipSumNoisedFinalizeUnsigned, PacClipSumUpdateUBigInt);
		// HUGEINT/UHUGEINT: use 128-bit state (62 levels)
		set.AddFunction(
		    AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::HUGEINT}, LogicalType::HUGEINT,
		                      PacClipSumStateSize128, PacClipSumInitialize128, PacClipSumScatterUpdateHugeInt,
		                      PacClipSumCombine128, PacClipSumNoisedFinalizeSigned128,
		                      FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateHugeInt, PacClipBind));
		set.AddFunction(
		    AggregateFunction(name, {LogicalType::UBIGINT, LogicalType::UHUGEINT}, LogicalType::HUGEINT,
		                      PacClipSumStateSize128, PacClipSumInitialize128, PacClipSumScatterUpdateUHugeInt,
		                      PacClipSumCombine128, PacClipSumNoisedFinalizeUnsigned128,
		                      FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateUHugeInt, PacClipBind));
	}
}

// ============================================================================
// Registration: pac_clip_sum (counters, LIST<FLOAT>)
// ============================================================================
void RegisterPacClipSumFunctions(ExtensionLoader &loader) {
	AggregateFunctionSet fcn_set("pac_clip_sum");
	RegisterClipSumTypeOverloads(fcn_set, "pac_clip_sum", true);

	// DECIMAL overloads
	auto list_type = LogicalType::LIST(PacFloatLogicalType());
	fcn_set.AddFunction(AggregateFunction({LogicalType::UBIGINT, LogicalTypeId::DECIMAL}, list_type, nullptr, nullptr,
	                                      nullptr, nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                                      nullptr, BindDecimalPacClipSum));
	fcn_set.AddFunction(AggregateFunction({LogicalType::UBIGINT, LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                      list_type, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                      FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, BindDecimalPacClipSum));

	// FLOAT/DOUBLE overloads (scale to int64 internally)
	fcn_set.AddFunction(AggregateFunction(
	    "pac_clip_sum", {LogicalType::UBIGINT, LogicalType::FLOAT}, list_type, PacClipSumStateSize,
	    PacClipSumInitialize, PacClipSumScatterUpdateSingleFloat, PacClipSumCombine, PacClipSumFinalizeCountersSigned,
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleFloat, PacClipBindFloat));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_clip_sum", {LogicalType::UBIGINT, LogicalType::FLOAT, LogicalType::DOUBLE}, list_type, PacClipSumStateSize,
	    PacClipSumInitialize, PacClipSumScatterUpdateSingleFloat, PacClipSumCombine, PacClipSumFinalizeCountersSigned,
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleFloat, PacClipBindFloat));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_clip_sum", {LogicalType::UBIGINT, LogicalType::DOUBLE}, list_type, PacClipSumStateSize,
	    PacClipSumInitialize, PacClipSumScatterUpdateSingleDouble, PacClipSumCombine, PacClipSumFinalizeCountersSigned,
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleDouble, PacClipBindDouble));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_clip_sum", {LogicalType::UBIGINT, LogicalType::DOUBLE, LogicalType::DOUBLE}, list_type,
	    PacClipSumStateSize, PacClipSumInitialize, PacClipSumScatterUpdateSingleDouble, PacClipSumCombine,
	    PacClipSumFinalizeCountersSigned, FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleDouble,
	    PacClipBindDouble));

	// Add list aggregate overload (LIST<FLOAT> → LIST<FLOAT>) for categorical/subquery
	AddPacListAggregateOverload(fcn_set, "clip_sum");

	CreateAggregateFunctionInfo info(fcn_set);
	FunctionDescription desc;
	desc.description = "[INTERNAL] Returns 64 PAC subsample counters with per-level clipping as LIST.";
	desc.examples = {"SELECT c_mktsegment, pac_clip_sum(pac_hash(hash(c_custkey)), c_acctbal) FROM customer GROUP BY "
	                 "c_mktsegment"};
	info.descriptions.push_back(std::move(desc));
	loader.RegisterFunction(std::move(info));
}

// ============================================================================
// Registration: pac_noised_clip_sum (fused noised, scalar HUGEINT)
// ============================================================================
void RegisterPacNoisedClipSumFunctions(ExtensionLoader &loader) {
	AggregateFunctionSet fcn_set("pac_noised_clip_sum");
	RegisterClipSumTypeOverloads(fcn_set, "pac_noised_clip_sum", false);

	// DECIMAL overloads
	fcn_set.AddFunction(AggregateFunction(
	    {LogicalType::UBIGINT, LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr, nullptr,
	    nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, BindDecimalPacNoisedClipSum));
	fcn_set.AddFunction(AggregateFunction(
	    {LogicalType::UBIGINT, LogicalTypeId::DECIMAL, LogicalType::DOUBLE}, LogicalTypeId::DECIMAL, nullptr, nullptr,
	    nullptr, nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, BindDecimalPacNoisedClipSum));

	// FLOAT/DOUBLE overloads (return FLOAT/DOUBLE respectively)
	fcn_set.AddFunction(AggregateFunction(
	    "pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::FLOAT}, LogicalType::FLOAT, PacClipSumStateSize,
	    PacClipSumInitialize, PacClipSumScatterUpdateSingleFloat, PacClipSumCombine, PacClipSumNoisedFinalizeFloat,
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleFloat, PacClipBindFloat));
	fcn_set.AddFunction(
	    AggregateFunction("pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::FLOAT, LogicalType::DOUBLE},
	                      LogicalType::FLOAT, PacClipSumStateSize, PacClipSumInitialize,
	                      PacClipSumScatterUpdateSingleFloat, PacClipSumCombine, PacClipSumNoisedFinalizeFloat,
	                      FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleFloat, PacClipBindFloat));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::DOUBLE}, LogicalType::DOUBLE, PacClipSumStateSize,
	    PacClipSumInitialize, PacClipSumScatterUpdateSingleDouble, PacClipSumCombine, PacClipSumNoisedFinalizeDouble,
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleDouble, PacClipBindDouble));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	    PacClipSumStateSize, PacClipSumInitialize, PacClipSumScatterUpdateSingleDouble, PacClipSumCombine,
	    PacClipSumNoisedFinalizeDouble, FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleDouble,
	    PacClipBindDouble));

	CreateAggregateFunctionInfo info(fcn_set);
	FunctionDescription desc;
	desc.description = "Privacy-preserving SUM with per-level clipping and noising. Supports 128-bit.";
	desc.examples = {"SELECT c_mktsegment, pac_noised_clip_sum(pac_hash(hash(c_custkey)), c_acctbal) FROM customer "
	                 "GROUP BY c_mktsegment"};
	info.descriptions.push_back(std::move(desc));
	loader.RegisterFunction(std::move(info));
}

// ============================================================================
// Registration: pac_noised_clip_sumcount (sum-of-counts, BIGINT → BIGINT)
// Used when count→sum conversion needs to preserve BIGINT return type.
// ============================================================================
void RegisterPacNoisedClipSumCountFunctions(ExtensionLoader &loader) {
	AggregateFunctionSet fcn_set("pac_noised_clip_sumcount");
	// Only BIGINT input → BIGINT output (counts are always BIGINT)
	AddNoisedClipSumFcn(fcn_set, "pac_noised_clip_sumcount", LogicalType::BIGINT, LogicalType::BIGINT,
	                    PacClipSumScatterUpdateBigInt, PacClipSumNoisedFinalizeBigInt, PacClipSumUpdateBigInt);
	CreateAggregateFunctionInfo info(fcn_set);
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
