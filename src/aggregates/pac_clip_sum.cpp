#include "aggregates/pac_clip_sum.hpp"
#include "categorical/pac_categorical.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <cmath>

namespace duckdb {

// ============================================================================
// Inner state update: add one unsigned value to the state
// ============================================================================
AUTOVECTORIZE inline void PacClipSumUpdateOneInternal(PacClipSumIntState &state, uint64_t key_hash, uint64_t value,
                                                      ArenaAllocator &allocator) {
	state.key_hash |= key_hash;

	int level = PacClipSumIntState::GetLevel(value);
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
AUTOVECTORIZE inline void PacClipSumUpdateOneInternal(PacClipSumIntState &state, uint64_t key_hash, hugeint_t value,
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

	int level = PacClipSumIntState::GetLevel128(upper, lower);
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
template <bool SIGNED>
inline void PacClipSumRouteValue(PacClipSumStateWrapper &wrapper, PacClipSumIntState *pos_state, uint64_t hash,
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
inline void PacClipSumRouteHugeint(PacClipSumStateWrapper &wrapper, PacClipSumIntState *pos_state, uint64_t hash,
                                   hugeint_t value, ArenaAllocator &a, bool is_signed) {
	if (DUCKDB_LIKELY(hash)) {
		if (is_signed && value.upper < 0) {
			auto *neg = wrapper.EnsureNegState(a);
			hugeint_t abs_val = -value;
			uint64_t upper = static_cast<uint64_t>(abs_val.upper);
			uint64_t lower = abs_val.lower;
			int level = PacClipSumIntState::GetLevel128(upper, lower);
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
template <bool SIGNED>
inline void PacClipSumFlushBuffer(PacClipSumStateWrapper &src, PacClipSumStateWrapper &dst, ArenaAllocator &a) {
	uint64_t cnt = src.n_buffered & PacClipSumStateWrapper::BUF_MASK;
	if (cnt > 0) {
		auto *dst_state = dst.EnsureState(a);
		for (uint64_t i = 0; i < cnt; i++) {
			PacClipSumRouteValue<SIGNED>(dst, dst_state, src.hash_buf[i], src.val_buf[i], a);
		}
		src.n_buffered &= ~PacClipSumStateWrapper::BUF_MASK;
	}
}

// ============================================================================
// Buffered update
// ============================================================================
template <bool SIGNED, typename ValueT>
AUTOVECTORIZE inline void PacClipSumUpdateOne(PacClipSumStateWrapper &agg, uint64_t key_hash, ValueT value,
                                              ArenaAllocator &a) {
	uint64_t cnt = agg.n_buffered & PacClipSumStateWrapper::BUF_MASK;
	if (DUCKDB_UNLIKELY(cnt == PacClipSumStateWrapper::BUF_SIZE)) {
		auto *dst_state = agg.EnsureState(a);
		for (int i = 0; i < PacClipSumStateWrapper::BUF_SIZE; i++) {
			PacClipSumRouteValue<SIGNED>(agg, dst_state, agg.hash_buf[i], agg.val_buf[i], a);
		}
		PacClipSumRouteValue<SIGNED>(agg, dst_state, key_hash, static_cast<uint64_t>(value), a);
		agg.n_buffered &= ~PacClipSumStateWrapper::BUF_MASK;
	} else {
		agg.val_buf[cnt] = static_cast<uint64_t>(value);
		agg.hash_buf[cnt] = key_hash;
		agg.n_buffered++;
	}
}

// Hugeint buffered update — bypass buffer, update directly
template <bool SIGNED>
inline void PacClipSumUpdateOne(PacClipSumStateWrapper &agg, uint64_t key_hash, hugeint_t value, ArenaAllocator &a) {
	PacClipSumFlushBuffer<SIGNED>(agg, agg, a); // flush any buffered values first
	auto *state = agg.EnsureState(a);
	PacClipSumRouteHugeint(agg, state, key_hash, value, a, SIGNED);
}

// ============================================================================
// Vectorized Update and ScatterUpdate
// ============================================================================
template <bool SIGNED, class VALUE_TYPE, class INPUT_TYPE>
static void PacClipSumUpdate(Vector inputs[], PacClipSumStateWrapper &state, idx_t count, ArenaAllocator &allocator) {
	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);

	if (hash_data.validity.AllValid() && value_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			PacClipSumUpdateOne<SIGNED>(state, hashes[h_idx], ConvertValue<VALUE_TYPE>::convert(values[v_idx]),
			                            allocator);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
				continue;
			}
			PacClipSumUpdateOne<SIGNED>(state, hashes[h_idx], ConvertValue<VALUE_TYPE>::convert(values[v_idx]),
			                            allocator);
		}
	}
}

template <bool SIGNED, class VALUE_TYPE, class INPUT_TYPE>
static void PacClipSumScatterUpdate(Vector inputs[], Vector &states, idx_t count, ArenaAllocator &allocator) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);

	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipSumStateWrapper *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		PacClipSumUpdateOne<SIGNED>(*state, hashes[h_idx], ConvertValue<VALUE_TYPE>::convert(values[v_idx]), allocator);
	}
}

// ============================================================================
// X-macro: generate Update/ScatterUpdate for integer types
// ============================================================================
#define PAC2_INT_TYPES_SIGNED                                                                                          \
	X(TinyInt, int64_t, int8_t, true)                                                                                  \
	X(SmallInt, int64_t, int16_t, true)                                                                                \
	X(Integer, int64_t, int32_t, true)                                                                                 \
	X(BigInt, int64_t, int64_t, true)

#define PAC2_INT_TYPES_UNSIGNED                                                                                        \
	X(UTinyInt, uint64_t, uint8_t, false)                                                                              \
	X(USmallInt, uint64_t, uint16_t, false)                                                                            \
	X(UInteger, uint64_t, uint32_t, false)                                                                             \
	X(UBigInt, uint64_t, uint64_t, false)

#define X(NAME, VALUE_T, INPUT_T, SIGNED)                                                                              \
	static void PacClipSumUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p,           \
	                                   idx_t count) {                                                                  \
		auto &state = *reinterpret_cast<PacClipSumStateWrapper *>(state_p);                                            \
		PacClipSumUpdate<SIGNED, VALUE_T, INPUT_T>(inputs, state, count, aggr.allocator);                              \
	}                                                                                                                  \
	static void PacClipSumScatterUpdate##NAME(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states,        \
	                                          idx_t count) {                                                           \
		PacClipSumScatterUpdate<SIGNED, VALUE_T, INPUT_T>(inputs, states, count, aggr.allocator);                      \
	}
PAC2_INT_TYPES_SIGNED
PAC2_INT_TYPES_UNSIGNED
#undef X

// HugeInt update (signed, via hugeint routing)
static void PacClipSumUpdateHugeInt(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p, idx_t count) {
	auto &state = *reinterpret_cast<PacClipSumStateWrapper *>(state_p);
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
		PacClipSumUpdateOne<true>(state, hashes[h_idx], values[v_idx], aggr.allocator);
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
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipSumStateWrapper *>(sdata);
	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		PacClipSumUpdateOne<true>(*state, hashes[h_idx], values[v_idx], aggr.allocator);
	}
}

// UHugeInt update (unsigned, convert to hugeint for routing)
static void PacClipSumUpdateUHugeInt(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p,
                                     idx_t count) {
	auto &state = *reinterpret_cast<PacClipSumStateWrapper *>(state_p);
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
			int level = PacClipSumIntState::GetLevel128(upper, lower);
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
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipSumStateWrapper *>(sdata);
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
			int level = PacClipSumIntState::GetLevel128(upper, lower);
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
	auto &state = *reinterpret_cast<PacClipSumStateWrapper *>(state_p);
	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<FLOAT_TYPE>(value_data);

	if (hash_data.validity.AllValid() && value_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			PacClipSumUpdateOne<true>(state, hashes[h_idx], ScaleFloatToInt64<FLOAT_TYPE, SHIFT>(values[v_idx]),
			                          aggr.allocator);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
				continue;
			}
			PacClipSumUpdateOne<true>(state, hashes[h_idx], ScaleFloatToInt64<FLOAT_TYPE, SHIFT>(values[v_idx]),
			                          aggr.allocator);
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
	auto state_ptrs = UnifiedVectorFormat::GetData<PacClipSumStateWrapper *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue;
		}
		PacClipSumUpdateOne<true>(*state, hashes[h_idx], ScaleFloatToInt64<FLOAT_TYPE, SHIFT>(values[v_idx]),
		                          aggr.allocator);
	}
}

// Instantiate float/double update functions
static void PacClipSumUpdateSingleFloat(Vector inputs[], AggregateInputData &aggr, idx_t n, data_ptr_t state_p,
                                        idx_t count) {
	PacClipSumUpdateFloat<float, PAC2_FLOAT_SHIFT>(inputs, aggr, n, state_p, count);
}
static void PacClipSumScatterUpdateSingleFloat(Vector inputs[], AggregateInputData &aggr, idx_t n, Vector &states,
                                               idx_t count) {
	PacClipSumScatterUpdateFloat<float, PAC2_FLOAT_SHIFT>(inputs, aggr, n, states, count);
}
static void PacClipSumUpdateSingleDouble(Vector inputs[], AggregateInputData &aggr, idx_t n, data_ptr_t state_p,
                                         idx_t count) {
	PacClipSumUpdateFloat<double, PAC2_DOUBLE_SHIFT>(inputs, aggr, n, state_p, count);
}
static void PacClipSumScatterUpdateSingleDouble(Vector inputs[], AggregateInputData &aggr, idx_t n, Vector &states,
                                                idx_t count) {
	PacClipSumScatterUpdateFloat<double, PAC2_DOUBLE_SHIFT>(inputs, aggr, n, states, count);
}

// ============================================================================
// Combine
// ============================================================================
AUTOVECTORIZE static void PacClipSumCombineInt(Vector &src, Vector &dst, idx_t count, ArenaAllocator &allocator) {
	auto src_wrapper = FlatVector::GetData<PacClipSumStateWrapper *>(src);
	auto dst_wrapper = FlatVector::GetData<PacClipSumStateWrapper *>(dst);

	for (idx_t i = 0; i < count; i++) {
		// Flush src's buffer into dst
		PacClipSumFlushBuffer<true>(*src_wrapper[i], *dst_wrapper[i], allocator);

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
	PacClipSumCombineInt(src, dst, count, aggr.allocator);
}

// ============================================================================
// Bind data with clip_support threshold
// ============================================================================
struct PacClipSumBindData : public PacBindData {
	int clip_support_threshold; // levels with fewer estimated distinct contributors are zeroed out
	double float_scale;         // scale factor for float/double→int64 conversion (1.0 for integer types)

	PacClipSumBindData(ClientContext &ctx, double mi_val, double correction_val, int clip_support,
	                   double float_scale_val = 1.0)
	    : PacBindData(ctx, mi_val, correction_val, 1.0), clip_support_threshold(clip_support),
	      float_scale(float_scale_val) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<PacClipSumBindData>(*this);
		copy->total_update_count = 0;
		copy->suspicious_count = 0;
		copy->nonsuspicious_count = 0;
		return copy;
	}
	bool Equals(const FunctionData &other) const override {
		if (!PacBindData::Equals(other)) {
			return false;
		}
		auto *o = dynamic_cast<const PacClipSumBindData *>(&other);
		return o && clip_support_threshold == o->clip_support_threshold && float_scale == o->float_scale;
	}
};

// ============================================================================
// Finalize
// ============================================================================
template <class ACC_TYPE, bool SIGNED>
static void PacClipSumFinalize(Vector &states, AggregateInputData &input, Vector &result, idx_t count, idx_t offset) {
	auto state_ptrs = FlatVector::GetData<PacClipSumStateWrapper *>(states);
	auto data = FlatVector::GetData<ACC_TYPE>(result);
	auto &result_mask = FlatVector::Validity(result);
	auto &bind = static_cast<PacClipSumBindData &>(*input.bind_data);
	double mi = bind.mi;
	double correction = bind.correction;
	uint64_t query_hash = bind.query_hash;
	auto pstate = bind.pstate;
	int clip_support = bind.clip_support_threshold;

	for (idx_t i = 0; i < count; i++) {
		PacClipSumFlushBuffer<SIGNED>(*state_ptrs[i], *state_ptrs[i], input.allocator);

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
		pos->GetTotals(buf, clip_support);
		uint64_t update_count = pos->update_count;

		// Subtract neg state
		auto *neg = state_ptrs[i]->GetNegState();
		if (neg) {
			PAC_FLOAT neg_buf[64] = {0};
			neg->GetTotals(neg_buf, clip_support);
			key_hash |= neg->key_hash;
			for (int j = 0; j < 64; j++) {
				buf[j] -= neg_buf[j];
			}
			update_count += neg->update_count;
		}

		CheckPacSampleDiversity(key_hash, buf, update_count, "pac_clip_sum", bind);
		PAC_FLOAT result_val = PacNoisySampleFrom64Counters(buf, mi, correction, gen, ~key_hash, query_hash, pstate);
		result_val *= PAC_FLOAT(2.0);                           // 2x compensation for ~50% sampling
		result_val /= static_cast<PAC_FLOAT>(bind.float_scale); // undo float→int64 scaling (1.0 for integers)
		data[offset + i] = FromDouble<ACC_TYPE>(result_val);
	}
}

// Instantiate noised finalize (scalar output for pac_noised_clip_sum)
static void PacClipSumNoisedFinalizeSigned(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                           idx_t offset) {
	PacClipSumFinalize<hugeint_t, true>(states, input, result, count, offset);
}
static void PacClipSumNoisedFinalizeUnsigned(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                             idx_t offset) {
	PacClipSumFinalize<hugeint_t, false>(states, input, result, count, offset);
}
// BIGINT output variant — used for count→sum conversion where the original returned BIGINT
static void PacClipSumNoisedFinalizeBigInt(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                           idx_t offset) {
	PacClipSumFinalize<int64_t, true>(states, input, result, count, offset);
}
// Float/double output variants
static void PacClipSumNoisedFinalizeFloat(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                          idx_t offset) {
	PacClipSumFinalize<float, true>(states, input, result, count, offset);
}
static void PacClipSumNoisedFinalizeDouble(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                           idx_t offset) {
	PacClipSumFinalize<double, true>(states, input, result, count, offset);
}

// ============================================================================
// Counters finalize (LIST<FLOAT> output for pac_clip_sum)
// ============================================================================
template <bool SIGNED>
static void PacClipSumFinalizeCounters(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                       idx_t offset) {
	auto state_ptrs = FlatVector::GetData<PacClipSumStateWrapper *>(states);
	auto &bind = static_cast<PacClipSumBindData &>(*input.bind_data);
	int clip_support = bind.clip_support_threshold;
	double correction = bind.correction;
	double float_scale = bind.float_scale;

	// Result is LIST<FLOAT>
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &child_vec = ListVector::GetEntry(result);

	idx_t total_elements = count * 64;
	ListVector::Reserve(result, total_elements);
	ListVector::SetListSize(result, total_elements);

	auto child_data = FlatVector::GetData<PAC_FLOAT>(child_vec);

	for (idx_t i = 0; i < count; i++) {
		PacClipSumFlushBuffer<SIGNED>(*state_ptrs[i], *state_ptrs[i], input.allocator);

		list_entries[offset + i].offset = i * 64;
		list_entries[offset + i].length = 64;

		PAC_FLOAT buf[64] = {0};
		uint64_t key_hash = 0;
		uint64_t update_count = 0;

		auto *pos = state_ptrs[i]->GetState();
		if (pos) {
			key_hash = pos->key_hash;
			update_count = pos->update_count;
			pos->GetTotals(buf, clip_support);

			auto *neg = state_ptrs[i]->GetNegState();
			if (neg) {
				PAC_FLOAT neg_buf[64] = {0};
				neg->GetTotals(neg_buf, clip_support);
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

static void PacClipSumFinalizeCountersSigned(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                             idx_t offset) {
	PacClipSumFinalizeCounters<true>(states, input, result, count, offset);
}
static void PacClipSumFinalizeCountersUnsigned(Vector &states, AggregateInputData &input, Vector &result, idx_t count,
                                               idx_t offset) {
	PacClipSumFinalizeCounters<false>(states, input, result, count, offset);
}

// ============================================================================
// State size / init / bind
// ============================================================================
static idx_t PacClipSumStateSize(const AggregateFunction &) {
	return sizeof(PacClipSumStateWrapper);
}

static void PacClipSumInitialize(const AggregateFunction &, data_ptr_t state_p) {
	memset(state_p, 0, sizeof(PacClipSumStateWrapper));
}

static unique_ptr<FunctionData> PacClipSumBind(ClientContext &ctx, AggregateFunction &,
                                               vector<unique_ptr<Expression>> &args) {
	double mi = GetPacMiFromSetting(ctx);
	double correction = 1.0;
	if (2 < args.size()) {
		if (!args[2]->IsFoldable()) {
			throw InvalidInputException("pac_clip_sum: correction parameter must be a constant");
		}
		auto val = ExpressionExecutor::EvaluateScalar(ctx, *args[2]);
		correction = val.GetValue<double>();
		if (correction < 0.0) {
			throw InvalidInputException("pac_clip_sum: correction must be >= 0");
		}
	}
	// Read pac_clip_support threshold
	int clip_support = 0;
	Value dc_val;
	if (ctx.TryGetCurrentSetting("pac_clip_support", dc_val) && !dc_val.IsNull()) {
		clip_support = static_cast<int>(dc_val.GetValue<int64_t>());
	}
	return make_uniq<PacClipSumBindData>(ctx, mi, correction, clip_support);
}

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
		return AggregateFunction("pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::HUGEINT},
		                         LogicalType::HUGEINT, PacClipSumStateSize, PacClipSumInitialize,
		                         PacClipSumScatterUpdateHugeInt, PacClipSumCombine, PacClipSumNoisedFinalizeSigned,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateHugeInt);
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
		                         PacClipSumStateSize, PacClipSumInitialize, PacClipSumScatterUpdateHugeInt,
		                         PacClipSumCombine, PacClipSumFinalizeCountersSigned,
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
	return PacClipSumBind(ctx, function, args);
}

static unique_ptr<FunctionData> BindDecimalPacClipSum(ClientContext &ctx, AggregateFunction &function,
                                                      vector<unique_ptr<Expression>> &args) {
	auto decimal_type = args[1]->return_type;
	function = GetPacClipSumCountersAggregate(decimal_type.InternalType());
	function.name = "pac_clip_sum";
	function.arguments[1] = decimal_type;
	// counters always return LIST<FLOAT>, no DECIMAL return type needed
	return PacClipSumBind(ctx, function, args);
}

// Float/double bind: thin wrappers around PacClipSumBind logic with float_scale
static unique_ptr<FunctionData> PacClipSumBindFloat(ClientContext &ctx, AggregateFunction &f,
                                                    vector<unique_ptr<Expression>> &args) {
	auto result = PacClipSumBind(ctx, f, args);
	static_cast<PacClipSumBindData &>(*result).float_scale = PAC2_FLOAT_SCALE;
	return result;
}
static unique_ptr<FunctionData> PacClipSumBindDouble(ClientContext &ctx, AggregateFunction &f,
                                                     vector<unique_ptr<Expression>> &args) {
	auto result = PacClipSumBind(ctx, f, args);
	static_cast<PacClipSumBindData &>(*result).float_scale = PAC2_DOUBLE_SCALE;
	return result;
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
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipSumBind));
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type, LogicalType::DOUBLE}, list_type,
	                                  PacClipSumStateSize, PacClipSumInitialize, scatter, PacClipSumCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipSumBind));
}

static void AddNoisedClipSumFcn(AggregateFunctionSet &set, const string &name, const LogicalType &value_type,
                                const LogicalType &result_type, aggregate_update_t scatter,
                                aggregate_finalize_t finalize, aggregate_simple_update_t update) {
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type}, result_type, PacClipSumStateSize,
	                                  PacClipSumInitialize, scatter, PacClipSumCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipSumBind));
	set.AddFunction(AggregateFunction(name, {LogicalType::UBIGINT, value_type, LogicalType::DOUBLE}, result_type,
	                                  PacClipSumStateSize, PacClipSumInitialize, scatter, PacClipSumCombine, finalize,
	                                  FunctionNullHandling::DEFAULT_NULL_HANDLING, update, PacClipSumBind));
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
		AddClipSumCountersFcn(set, name, LogicalType::HUGEINT, PacClipSumScatterUpdateHugeInt,
		                      PacClipSumFinalizeCountersSigned, PacClipSumUpdateHugeInt);
		AddClipSumCountersFcn(set, name, LogicalType::UHUGEINT, PacClipSumScatterUpdateUHugeInt,
		                      PacClipSumFinalizeCountersUnsigned, PacClipSumUpdateUHugeInt);
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
		AddNoisedClipSumFcn(set, name, LogicalType::HUGEINT, LogicalType::HUGEINT, PacClipSumScatterUpdateHugeInt,
		                    PacClipSumNoisedFinalizeSigned, PacClipSumUpdateHugeInt);
		AddNoisedClipSumFcn(set, name, LogicalType::UHUGEINT, LogicalType::HUGEINT, PacClipSumScatterUpdateUHugeInt,
		                    PacClipSumNoisedFinalizeUnsigned, PacClipSumUpdateUHugeInt);
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
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleFloat, PacClipSumBindFloat));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_clip_sum", {LogicalType::UBIGINT, LogicalType::FLOAT, LogicalType::DOUBLE}, list_type, PacClipSumStateSize,
	    PacClipSumInitialize, PacClipSumScatterUpdateSingleFloat, PacClipSumCombine, PacClipSumFinalizeCountersSigned,
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleFloat, PacClipSumBindFloat));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_clip_sum", {LogicalType::UBIGINT, LogicalType::DOUBLE}, list_type, PacClipSumStateSize,
	    PacClipSumInitialize, PacClipSumScatterUpdateSingleDouble, PacClipSumCombine, PacClipSumFinalizeCountersSigned,
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleDouble, PacClipSumBindDouble));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_clip_sum", {LogicalType::UBIGINT, LogicalType::DOUBLE, LogicalType::DOUBLE}, list_type,
	    PacClipSumStateSize, PacClipSumInitialize, PacClipSumScatterUpdateSingleDouble, PacClipSumCombine,
	    PacClipSumFinalizeCountersSigned, FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleDouble,
	    PacClipSumBindDouble));

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
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleFloat, PacClipSumBindFloat));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::FLOAT, LogicalType::DOUBLE}, LogicalType::FLOAT,
	    PacClipSumStateSize, PacClipSumInitialize, PacClipSumScatterUpdateSingleFloat, PacClipSumCombine,
	    PacClipSumNoisedFinalizeFloat, FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleFloat,
	    PacClipSumBindFloat));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::DOUBLE}, LogicalType::DOUBLE, PacClipSumStateSize,
	    PacClipSumInitialize, PacClipSumScatterUpdateSingleDouble, PacClipSumCombine, PacClipSumNoisedFinalizeDouble,
	    FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleDouble, PacClipSumBindDouble));
	fcn_set.AddFunction(AggregateFunction(
	    "pac_noised_clip_sum", {LogicalType::UBIGINT, LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	    PacClipSumStateSize, PacClipSumInitialize, PacClipSumScatterUpdateSingleDouble, PacClipSumCombine,
	    PacClipSumNoisedFinalizeDouble, FunctionNullHandling::DEFAULT_NULL_HANDLING, PacClipSumUpdateSingleDouble,
	    PacClipSumBindDouble));

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
