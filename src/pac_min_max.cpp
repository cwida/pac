#include "include/pac_min_max.hpp"

// Macro to recompute global bound at current level
// BOUND_T: type to cast the result to (State::TMAX for int, VALUE_TYPE for float)
// EXT_T: element type of the extremes array
// EXTREMES: the extremes array
#ifdef PAC_MINMAX_NOBOUNDOPT
#define BOUND_RECOMPUTE(BOUND_T, EXT_T, EXTREMES)
#else
#define BOUND_RECOMPUTE(BOUND_T, EXT_T, EXTREMES)                                                                      \
	if (++state.update_count == BOUND_RECOMPUTE_INTERVAL) {                                                            \
		state.update_count = 0;                                                                                        \
		state.global_bound = static_cast<BOUND_T>(ComputeGlobalBound<EXT_T, EXT_T, IS_MAX>(EXTREMES));                 \
	}
#endif

namespace duckdb {

// ============================================================================
// Update functions - process one value into the 64 extremes
// ============================================================================

// Integer update: cascading version with MAXWIDTH support
template <bool SIGNED, bool IS_MAX, int MAXWIDTH, typename VALUE_TYPE>
AUTOVECTORIZE static inline void PacMinMaxUpdateOne(PacMinMaxIntState<SIGNED, IS_MAX, MAXWIDTH> &state,
                                                    uint64_t key_hash, VALUE_TYPE value, ArenaAllocator &allocator) {
	using State = PacMinMaxIntState<SIGNED, IS_MAX, MAXWIDTH>;
	using T8 = typename State::T8;
	using T16 = typename State::T16;
	using T32 = typename State::T32;
	using T64 = typename State::T64;
	using T128 = typename State::T128;
	using TMAX = typename State::TMAX;
#ifdef PAC_MINMAX_NONCASCADING
	if (!state.initialized) {
		state.Initialize();
	}
#ifndef PAC_MINMAX_NOBOUNDOPT
	// Compare at TMAX level - global_bound is already stored as TMAX
	if (!PAC_IS_BETTER(static_cast<TMAX>(value), state.global_bound)) {
		return; // early out using minmax optimization
	}
#endif
	// non-cascading mode: aggregate at TMAX (widest type for this MAXWIDTH)
	UpdateExtremes<TMAX, IS_MAX>(state.extremes, key_hash, static_cast<TMAX>(value));
	BOUND_RECOMPUTE(TMAX, TMAX, state.extremes);
#else
	// cascading mode allocates levels in increasing width and upgrades upon need only
	if (!state.initialized) {
		state.AllocateFirstLevel(allocator);
	}
	// Upgrade level if new value doesn't fit
	if (MAXWIDTH >= 16 && !State::FitsIn8(value) && state.current_level == 8) {
		state.UpgradeTo16();
	}
	if (MAXWIDTH >= 32 && !State::FitsIn16(value) && state.current_level == 16) {
		state.UpgradeTo32();
	}
	if (MAXWIDTH >= 64 && !State::FitsIn32(value) && state.current_level == 32) {
		state.UpgradeTo64();
	}
	if (MAXWIDTH >= 128 && !State::FitsIn64(value) && state.current_level == 64) {
		state.UpgradeTo128();
	}
#ifndef PAC_MINMAX_NOBOUNDOPT
	// Compare value against global_bound (stored as TMAX, upcast value for comparison)
	if (!PAC_IS_BETTER(static_cast<TMAX>(value), state.global_bound)) {
		return; // early out
	}
#endif
	// Update at current level
	if (state.current_level == 8) {
		UpdateExtremesSIMD<T8, int8_t, IS_MAX, 0x0101010101010101ULL, 8>(state.extremes8, key_hash,
		                                                                 static_cast<T8>(value));
		BOUND_RECOMPUTE(TMAX, T8, state.extremes8);
	}
	if (MAXWIDTH >= 16 && state.current_level == 16) {
		UpdateExtremesSIMD<T16, int16_t, IS_MAX, 0x0001000100010001ULL, 16>(state.extremes16, key_hash,
		                                                                    static_cast<T16>(value));
		BOUND_RECOMPUTE(TMAX, T16, state.extremes16);
	}
	if (MAXWIDTH >= 32 && state.current_level == 32) {
		UpdateExtremesSIMD<T32, int32_t, IS_MAX, 0x0000000100000001ULL, 32>(state.extremes32, key_hash,
		                                                                    static_cast<T32>(value));
		BOUND_RECOMPUTE(TMAX, T32, state.extremes32);
	}
	if (MAXWIDTH >= 64 && state.current_level == 64) {
		UpdateExtremes<T64, IS_MAX>(state.extremes64, key_hash, static_cast<T64>(value));
		BOUND_RECOMPUTE(TMAX, T64, state.extremes64);
	}
	if (MAXWIDTH >= 128 && state.current_level == 128) {
		UpdateExtremes<T128, IS_MAX>(state.extremes128, key_hash, static_cast<T128>(value));
		BOUND_RECOMPUTE(TMAX, T128, state.extremes128);
	}
#endif
}

// Float/Double update - templated with VALUE_TYPE and MAXWIDTH
template <bool IS_MAX, typename VALUE_TYPE, int MAXWIDTH = 64>
AUTOVECTORIZE static inline void PacMinMaxUpdateOneFloat(PacMinMaxFloatState<IS_MAX, VALUE_TYPE, MAXWIDTH> &state,
                                                         uint64_t key_hash, VALUE_TYPE value,
                                                         ArenaAllocator &allocator) {
	using State = PacMinMaxFloatState<IS_MAX, VALUE_TYPE, MAXWIDTH>;
#ifdef PAC_MINMAX_NONCASCADING
	if (!state.initialized)
		state.Initialize();
#ifndef PAC_MINMAX_NOBOUNDOPT
	// Compare against global_bound (stored as VALUE_TYPE)
	if (!PAC_IS_BETTER(value, state.global_bound)) {
		return; // early out
	}
#endif
	// non-cascading mode: aggregate at VALUE_TYPE
	UpdateExtremes<VALUE_TYPE, IS_MAX>(state.extremes, key_hash, value);
	BOUND_RECOMPUTE(VALUE_TYPE, VALUE_TYPE, state.extremes);
#else
	// cascading mode allocates levels in increasing width and upgrades upon need only
	if (!state.initialized) {
		state.AllocateFirstLevel(allocator);
	}
	// Upgrade to double precision if value doesn't fit in float
	if (MAXWIDTH >= 64 && !State::FitsInFloat(value) && state.current_level == 32) {
		state.UpgradeToDoublePrecision();
	}
#ifndef PAC_MINMAX_NOBOUNDOPT
	// Compare against global_bound (stored as VALUE_TYPE, upcast value for comparison)
	if (!PAC_IS_BETTER(static_cast<VALUE_TYPE>(value), state.global_bound)) {
		return; // early out
	}
#endif
	if (state.current_level == 32) {
		UpdateExtremes<float, IS_MAX>(state.extremes32, key_hash, static_cast<float>(value));
		BOUND_RECOMPUTE(VALUE_TYPE, float, state.extremes32);
	}
	if (MAXWIDTH >= 64 && state.current_level == 64) {
		UpdateExtremes<double, IS_MAX>(state.extremes64, key_hash, static_cast<double>(value));
		BOUND_RECOMPUTE(VALUE_TYPE, double, state.extremes64);
	}
#endif
}

// Backwards compatible wrapper
template <bool IS_MAX>
AUTOVECTORIZE static inline void PacMinMaxUpdateOneDouble(PacMinMaxDoubleState<IS_MAX> &state, uint64_t key_hash,
                                                          double value, ArenaAllocator &allocator) {
	PacMinMaxUpdateOneFloat<IS_MAX, double>(state, key_hash, value, allocator);
}

// ============================================================================
// Batch Update functions (simple_update - single state pointer)
// ============================================================================

template <class State, bool SIGNED, bool IS_MAX, int MAXWIDTH, class VALUE_TYPE, class INPUT_TYPE>
static void PacMinMaxUpdate(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p, idx_t count) {
	auto &state = *reinterpret_cast<State *>(state_p);
	if (state.seen_null)
		return;

	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);

	if (hash_data.validity.AllValid() && value_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			PacMinMaxUpdateOne<SIGNED, IS_MAX, MAXWIDTH>(state, hashes[hash_data.sel->get_index(i)],
			                                             static_cast<VALUE_TYPE>(values[value_data.sel->get_index(i)]),
			                                             aggr.allocator);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
				state.seen_null = true;
				return;
			}
			PacMinMaxUpdateOne<SIGNED, IS_MAX, MAXWIDTH>(state, hashes[h_idx], static_cast<VALUE_TYPE>(values[v_idx]),
			                                             aggr.allocator);
		}
	}
}

// Double batch update
template <bool IS_MAX, class INPUT_TYPE>
static void PacMinMaxUpdateDouble(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p, idx_t count) {
	auto &state = *reinterpret_cast<PacMinMaxDoubleState<IS_MAX> *>(state_p);
	if (state.seen_null)
		return;

	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);

	if (hash_data.validity.AllValid() && value_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			PacMinMaxUpdateOneDouble<IS_MAX>(state, hashes[hash_data.sel->get_index(i)],
			                                 static_cast<double>(values[value_data.sel->get_index(i)]), aggr.allocator);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
				state.seen_null = true;
				return;
			}
			PacMinMaxUpdateOneDouble<IS_MAX>(state, hashes[h_idx], static_cast<double>(values[v_idx]), aggr.allocator);
		}
	}
}

// ============================================================================
// Scatter Update functions (update - vector of state pointers)
// ============================================================================

template <class State, bool SIGNED, bool IS_MAX, int MAXWIDTH, class VALUE_TYPE, class INPUT_TYPE>
static void PacMinMaxScatterUpdate(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states, idx_t count) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);

	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);
	auto state_ptrs = UnifiedVectorFormat::GetData<State *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (state->seen_null) {
			continue;
		}
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			state->seen_null = true;
			continue;
		}
		PacMinMaxUpdateOne<SIGNED, IS_MAX, MAXWIDTH>(*state, hashes[h_idx], static_cast<VALUE_TYPE>(values[v_idx]),
		                                             aggr.allocator);
	}
}

// Double scatter update
template <bool IS_MAX, class INPUT_TYPE>
static void PacMinMaxScatterUpdateDouble(Vector inputs[], AggregateInputData &aggr, idx_t, Vector &states,
                                         idx_t count) {
	UnifiedVectorFormat hash_data, value_data, sdata;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	states.ToUnifiedFormat(count, sdata);

	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);
	auto state_ptrs = UnifiedVectorFormat::GetData<PacMinMaxDoubleState<IS_MAX> *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto h_idx = hash_data.sel->get_index(i);
		auto v_idx = value_data.sel->get_index(i);
		auto state = state_ptrs[sdata.sel->get_index(i)];
		if (state->seen_null) {
			continue;
		}
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			state->seen_null = true;
			continue;
		}
		PacMinMaxUpdateOneDouble<IS_MAX>(*state, hashes[h_idx], static_cast<double>(values[v_idx]), aggr.allocator);
	}
}

// ============================================================================
// Combine functions
// ============================================================================

// Helper to get src value at appropriate level
template <typename State, typename T>
static inline T GetSrcVal(const State *s, int j);

template <bool SIGNED, bool IS_MAX, int MAXWIDTH>
AUTOVECTORIZE static void PacMinMaxCombineInt(Vector &src, Vector &dst, AggregateInputData &aggr, idx_t count) {
	using State = PacMinMaxIntState<SIGNED, IS_MAX, MAXWIDTH>;
	using TMAX = typename State::TMAX;
	auto src_state = FlatVector::GetData<State *>(src);
	auto dst_state = FlatVector::GetData<State *>(dst);

	for (idx_t i = 0; i < count; i++) {
		if (src_state[i]->seen_null) {
			dst_state[i]->seen_null = true;
		}
		if (dst_state[i]->seen_null) {
			continue;
		}
		auto *s = src_state[i];
		auto *d = dst_state[i];
#ifdef PAC_MINMAX_NONCASCADING
		if (!s->initialized)
			continue;
		if (!d->initialized)
			d->Initialize();
		for (int j = 0; j < 64; j++) {
			d->extremes[j] = PAC_BETTER(d->extremes[j], s->extremes[j]);
		}
		d->global_bound = ComputeGlobalBound<TMAX, TMAX, IS_MAX>(d->extremes);
#else
		if (!s->initialized) {
			continue;
		}
		if (!d->initialized) {
			d->AllocateFirstLevel(aggr.allocator);
		}
		// Upgrade dst to match src level
		if (MAXWIDTH >= 16 && d->current_level == 8 && d->current_level < s->current_level) {
			d->UpgradeTo16();
		}
		if (MAXWIDTH >= 32 && d->current_level == 16 && d->current_level < s->current_level) {
			d->UpgradeTo32();
		}
		if (MAXWIDTH >= 64 && d->current_level == 32 && d->current_level < s->current_level) {
			d->UpgradeTo64();
		}
		if (MAXWIDTH >= 128 && d->current_level == 64 && d->current_level < s->current_level) {
			d->UpgradeTo128();
		}
		// Combine at dst's current level, using GetValueAs to read source at appropriate level
		if (d->current_level == 8) {
			for (int j = 0; j < 64; j++) {
				d->extremes8[j] = PAC_BETTER(d->extremes8[j], s->template GetValueAs<typename State::T8>(j));
			}
			d->global_bound = ComputeGlobalBound<typename State::T8, TMAX, IS_MAX>(d->extremes8);
		}
		if (MAXWIDTH >= 16 && d->current_level == 16) {
			for (int j = 0; j < 64; j++) {
				d->extremes16[j] = PAC_BETTER(d->extremes16[j], s->template GetValueAs<typename State::T16>(j));
			}
			d->global_bound = ComputeGlobalBound<typename State::T16, TMAX, IS_MAX>(d->extremes16);
		}
		if (MAXWIDTH >= 32 && d->current_level == 32) {
			for (int j = 0; j < 64; j++) {
				d->extremes32[j] = PAC_BETTER(d->extremes32[j], s->template GetValueAs<typename State::T32>(j));
			}
			d->global_bound = ComputeGlobalBound<typename State::T32, TMAX, IS_MAX>(d->extremes32);
		}
		if (MAXWIDTH >= 64 && d->current_level == 64) {
			for (int j = 0; j < 64; j++) {
				d->extremes64[j] = PAC_BETTER(d->extremes64[j], s->template GetValueAs<typename State::T64>(j));
			}
			d->global_bound = ComputeGlobalBound<typename State::T64, TMAX, IS_MAX>(d->extremes64);
		}
		if (MAXWIDTH >= 128 && d->current_level == 128) {
			for (int j = 0; j < 64; j++) {
				d->extremes128[j] = PAC_BETTER(d->extremes128[j], s->template GetValueAs<typename State::T128>(j));
			}
			d->global_bound = ComputeGlobalBound<typename State::T128, TMAX, IS_MAX>(d->extremes128);
		}
#endif
	}
}

template <bool IS_MAX, int MAXWIDTH = 64>
AUTOVECTORIZE static void PacMinMaxCombineDouble(Vector &src, Vector &dst, AggregateInputData &aggr, idx_t count) {
	using State = PacMinMaxFloatState<IS_MAX, double, MAXWIDTH>;
	auto src_state = FlatVector::GetData<State *>(src);
	auto dst_state = FlatVector::GetData<State *>(dst);

	for (idx_t i = 0; i < count; i++) {
		if (src_state[i]->seen_null) {
			dst_state[i]->seen_null = true;
		}
		if (dst_state[i]->seen_null) {
			continue;
		}
		auto *s = src_state[i];
		auto *d = dst_state[i];
#ifdef PAC_MINMAX_NONCASCADING
		if (!s->initialized)
			continue;
		if (!d->initialized)
			d->Initialize();
		for (int j = 0; j < 64; j++) {
			d->extremes[j] = PAC_BETTER(d->extremes[j], s->extremes[j]);
		}
		d->global_bound = ComputeGlobalBound<double, double, IS_MAX>(d->extremes);
#else
		if (!s->initialized) {
			continue;
		}
		if (!d->initialized) {
			d->AllocateFirstLevel(aggr.allocator);
		}
		// Upgrade dst to double if src is at double level
		if (MAXWIDTH >= 64 && d->current_level == 32 && s->current_level == 64) {
			d->UpgradeToDoublePrecision();
		}
		// Combine at float level
		if (d->current_level == 32) {
			for (int j = 0; j < 64; j++) {
				d->extremes32[j] =
				    PAC_IS_BETTER(s->extremes32[j], d->extremes32[j]) ? s->extremes32[j] : d->extremes32[j];
			}
			d->global_bound = static_cast<double>(ComputeGlobalBound<float, float, IS_MAX>(d->extremes32));
		}
		// Combine at double level
		if (MAXWIDTH >= 64 && d->current_level == 64) {
			for (int j = 0; j < 64; j++) {
				double sv = (s->current_level >= 64) ? s->extremes64[j] : static_cast<double>(s->extremes32[j]);
				d->extremes64[j] = PAC_BETTER(d->extremes64[j], sv);
			}
			d->global_bound = ComputeGlobalBound<double, double, IS_MAX>(d->extremes64);
		}
#endif
	}
}

// ============================================================================
// Finalize function
// ============================================================================

template <class State, class RESULT_TYPE>
static void PacMinMaxFinalize(Vector &states, AggregateInputData &input, Vector &result, idx_t count, idx_t offset) {
	auto state = FlatVector::GetData<State *>(states);
	auto data = FlatVector::GetData<RESULT_TYPE>(result);
	auto &result_mask = FlatVector::Validity(result);

	uint64_t seed = input.bind_data ? input.bind_data->Cast<PacBindData>().seed : std::random_device {}();
	std::mt19937_64 gen(seed);
	double mi = input.bind_data ? input.bind_data->Cast<PacBindData>().mi : 128.0;

	for (idx_t i = 0; i < count; i++) {
		if (state[i]->seen_null) {
			result_mask.SetInvalid(offset + i);
		} else {
			double buf[64];
			state[i]->GetTotalsAsDouble(buf);
			data[offset + i] = FromDouble<RESULT_TYPE>(PacNoisySampleFrom64Counters(buf, mi, gen) + buf[41]);
		}
	}
}

// ============================================================================
// State size and initialize
// ============================================================================

template <bool SIGNED, bool IS_MAX, int MAXWIDTH = 64>
static idx_t PacMinMaxIntStateSize(const AggregateFunction &) {
	using State = PacMinMaxIntState<SIGNED, IS_MAX, MAXWIDTH>;
#ifdef PAC_MINMAX_NONCASCADING
	return sizeof(State);
#else
	// Use offsetof to avoid allocating unused pointers
	// Pointers are ordered: extremes8, extremes16, extremes32, extremes64, extremes128
	if (MAXWIDTH >= 128) {
		return sizeof(State);
	} else if (MAXWIDTH >= 64) {
		return offsetof(State, extremes128);
	} else if (MAXWIDTH >= 32) {
		return offsetof(State, extremes64);
	} else if (MAXWIDTH >= 16) {
		return offsetof(State, extremes32);
	} else {
		return offsetof(State, extremes16);
	}
#endif
}

template <bool SIGNED, bool IS_MAX, int MAXWIDTH = 64>
static void PacMinMaxIntInitialize(const AggregateFunction &func, data_ptr_t p) {
	memset(p, 0, PacMinMaxIntStateSize<SIGNED, IS_MAX, MAXWIDTH>(func));
}

template <bool IS_MAX, int MAXWIDTH = 64>
static idx_t PacMinMaxFloatStateSize(const AggregateFunction &) {
	using State = PacMinMaxFloatState<IS_MAX, double, MAXWIDTH>;
#ifdef PAC_MINMAX_NONCASCADING
	return sizeof(State);
#else
	// Use offsetof to avoid allocating unused pointers
	// Pointers are ordered: extremes32, extremes64
	if (MAXWIDTH >= 64) {
		return sizeof(State);
	} else {
		// MAXWIDTH=32 (float-only): don't need extremes64
		return offsetof(State, extremes64);
	}
#endif
}

// Backwards-compatible alias
template <bool IS_MAX>
static idx_t PacMinMaxDoubleStateSize(const AggregateFunction &func) {
	return PacMinMaxFloatStateSize<IS_MAX, 64>(func);
}

template <bool IS_MAX>
static void PacMinMaxDoubleInitialize(const AggregateFunction &func, data_ptr_t p) {
	memset(p, 0, PacMinMaxDoubleStateSize<IS_MAX>(func));
}

// ============================================================================
// Instantiated Update/ScatterUpdate wrappers
// ============================================================================

// Integer updates (MAXWIDTH=64)
#define INT_UPDATE_WRAPPER(NAME, SIGNED, WIDTH, VALTYPE, INTYPE)                                                       \
	template <bool IS_MAX>                                                                                             \
	void NAME(Vector inputs[], AggregateInputData &aggr, idx_t n, data_ptr_t state_p, idx_t count) {                   \
		PacMinMaxUpdate<PacMinMaxIntState<SIGNED, IS_MAX, WIDTH>, SIGNED, IS_MAX, WIDTH, VALTYPE, INTYPE>(             \
		    inputs, aggr, n, state_p, count);                                                                          \
	}
INT_UPDATE_WRAPPER(PacMinMaxUpdateInt8, true, 64, int64_t, int8_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateInt16, true, 64, int64_t, int16_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateInt32, true, 64, int64_t, int32_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateInt64, true, 64, int64_t, int64_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUInt8, false, 64, uint64_t, uint8_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUInt16, false, 64, uint64_t, uint16_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUInt32, false, 64, uint64_t, uint32_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUInt64, false, 64, uint64_t, uint64_t)
// HugeInt updates (MAXWIDTH=128)
INT_UPDATE_WRAPPER(PacMinMaxUpdateHugeInt, true, 128, hugeint_t, hugeint_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUHugeInt, false, 128, uhugeint_t, uhugeint_t)
#undef INT_UPDATE_WRAPPER

// Float/Double updates
template <bool IS_MAX>
void PacMinMaxUpdateFloat(Vector in[], AggregateInputData &a, idx_t n, data_ptr_t s, idx_t c) {
	PacMinMaxUpdateDouble<IS_MAX, float>(in, a, n, s, c);
}
template <bool IS_MAX>
void PacMinMaxUpdateDoubleW(Vector in[], AggregateInputData &a, idx_t n, data_ptr_t s, idx_t c) {
	PacMinMaxUpdateDouble<IS_MAX, double>(in, a, n, s, c);
}

// Integer scatter updates (MAXWIDTH=64)
#define INT_SCATTER_WRAPPER(NAME, SIGNED, WIDTH, VALTYPE, INTYPE)                                                      \
	template <bool IS_MAX>                                                                                             \
	void NAME(Vector inputs[], AggregateInputData &aggr, idx_t n, Vector &states, idx_t count) {                       \
		PacMinMaxScatterUpdate<PacMinMaxIntState<SIGNED, IS_MAX, WIDTH>, SIGNED, IS_MAX, WIDTH, VALTYPE, INTYPE>(      \
		    inputs, aggr, n, states, count);                                                                           \
	}
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateInt8, true, 64, int64_t, int8_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateInt16, true, 64, int64_t, int16_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateInt32, true, 64, int64_t, int32_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateInt64, true, 64, int64_t, int64_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUInt8, false, 64, uint64_t, uint8_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUInt16, false, 64, uint64_t, uint16_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUInt32, false, 64, uint64_t, uint32_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUInt64, false, 64, uint64_t, uint64_t)
// HugeInt scatter updates (MAXWIDTH=128)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateHugeInt, true, 128, hugeint_t, hugeint_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUHugeInt, false, 128, uhugeint_t, uhugeint_t)
#undef INT_SCATTER_WRAPPER

// Float/Double scatter updates
template <bool IS_MAX>
void PacMinMaxScatterUpdateFloat(Vector in[], AggregateInputData &a, idx_t n, Vector &s, idx_t c) {
	PacMinMaxScatterUpdateDouble<IS_MAX, float>(in, a, n, s, c);
}
template <bool IS_MAX>
void PacMinMaxScatterUpdateDoubleW(Vector in[], AggregateInputData &a, idx_t n, Vector &s, idx_t c) {
	PacMinMaxScatterUpdateDouble<IS_MAX, double>(in, a, n, s, c);
}

// Combine wrappers
template <bool IS_MAX>
void PacMinMaxCombineIntSigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombineInt<true, IS_MAX, 64>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineIntUnsigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombineInt<false, IS_MAX, 64>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineDoubleWrapper(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombineDouble<IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineHugeIntSigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombineInt<true, IS_MAX, 128>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineHugeIntUnsigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombineInt<false, IS_MAX, 128>(src, dst, a, c);
}

// ============================================================================
// Bind function
// ============================================================================

template <bool IS_MAX>
static unique_ptr<FunctionData> PacMinMaxBind(ClientContext &ctx, AggregateFunction &function,
                                              vector<unique_ptr<Expression>> &args) {
	// Get the value type (arg 1, arg 0 is hash)
	auto &value_type = args[1]->return_type;
	auto physical_type = value_type.InternalType();

	// Set return type to match input type
	function.return_type = value_type;
	function.arguments[1] = value_type;

	// Select implementation based on physical type
	// NOTE: function.update = scatter update (Vector &states)
	//       function.simple_update = simple update (data_ptr_t state_p)
	switch (physical_type) {
	case PhysicalType::INT8:
		function.state_size = PacMinMaxIntStateSize<true, IS_MAX>;
		function.initialize = PacMinMaxIntInitialize<true, IS_MAX>;
		function.update = PacMinMaxScatterUpdateInt8<IS_MAX>;
		function.simple_update = PacMinMaxUpdateInt8<IS_MAX>;
		function.combine = PacMinMaxCombineIntSigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX>, int8_t>;
		break;

	case PhysicalType::INT16:
		function.state_size = PacMinMaxIntStateSize<true, IS_MAX>;
		function.initialize = PacMinMaxIntInitialize<true, IS_MAX>;
		function.update = PacMinMaxScatterUpdateInt16<IS_MAX>;
		function.simple_update = PacMinMaxUpdateInt16<IS_MAX>;
		function.combine = PacMinMaxCombineIntSigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX>, int16_t>;
		break;

	case PhysicalType::INT32:
		function.state_size = PacMinMaxIntStateSize<true, IS_MAX>;
		function.initialize = PacMinMaxIntInitialize<true, IS_MAX>;
		function.update = PacMinMaxScatterUpdateInt32<IS_MAX>;
		function.simple_update = PacMinMaxUpdateInt32<IS_MAX>;
		function.combine = PacMinMaxCombineIntSigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX>, int32_t>;
		break;

	case PhysicalType::INT64:
		function.state_size = PacMinMaxIntStateSize<true, IS_MAX>;
		function.initialize = PacMinMaxIntInitialize<true, IS_MAX>;
		function.update = PacMinMaxScatterUpdateInt64<IS_MAX>;
		function.simple_update = PacMinMaxUpdateInt64<IS_MAX>;
		function.combine = PacMinMaxCombineIntSigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX>, int64_t>;
		break;

	case PhysicalType::UINT8:
		function.state_size = PacMinMaxIntStateSize<false, IS_MAX>;
		function.initialize = PacMinMaxIntInitialize<false, IS_MAX>;
		function.update = PacMinMaxScatterUpdateUInt8<IS_MAX>;
		function.simple_update = PacMinMaxUpdateUInt8<IS_MAX>;
		function.combine = PacMinMaxCombineIntUnsigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<false, IS_MAX>, uint8_t>;
		break;

	case PhysicalType::UINT16:
		function.state_size = PacMinMaxIntStateSize<false, IS_MAX>;
		function.initialize = PacMinMaxIntInitialize<false, IS_MAX>;
		function.update = PacMinMaxScatterUpdateUInt16<IS_MAX>;
		function.simple_update = PacMinMaxUpdateUInt16<IS_MAX>;
		function.combine = PacMinMaxCombineIntUnsigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<false, IS_MAX>, uint16_t>;
		break;

	case PhysicalType::UINT32:
		function.state_size = PacMinMaxIntStateSize<false, IS_MAX>;
		function.initialize = PacMinMaxIntInitialize<false, IS_MAX>;
		function.update = PacMinMaxScatterUpdateUInt32<IS_MAX>;
		function.simple_update = PacMinMaxUpdateUInt32<IS_MAX>;
		function.combine = PacMinMaxCombineIntUnsigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<false, IS_MAX>, uint32_t>;
		break;

	case PhysicalType::UINT64:
		function.state_size = PacMinMaxIntStateSize<false, IS_MAX>;
		function.initialize = PacMinMaxIntInitialize<false, IS_MAX>;
		function.update = PacMinMaxScatterUpdateUInt64<IS_MAX>;
		function.simple_update = PacMinMaxUpdateUInt64<IS_MAX>;
		function.combine = PacMinMaxCombineIntUnsigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<false, IS_MAX>, uint64_t>;
		break;

	case PhysicalType::FLOAT:
		function.state_size = PacMinMaxDoubleStateSize<IS_MAX>;
		function.initialize = PacMinMaxDoubleInitialize<IS_MAX>;
		function.update = PacMinMaxScatterUpdateFloat<IS_MAX>;
		function.simple_update = PacMinMaxUpdateFloat<IS_MAX>;
		function.combine = PacMinMaxCombineDoubleWrapper<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxDoubleState<IS_MAX>, float>;
		break;

	case PhysicalType::DOUBLE:
		function.state_size = PacMinMaxDoubleStateSize<IS_MAX>;
		function.initialize = PacMinMaxDoubleInitialize<IS_MAX>;
		function.update = PacMinMaxScatterUpdateDoubleW<IS_MAX>;
		function.simple_update = PacMinMaxUpdateDoubleW<IS_MAX>;
		function.combine = PacMinMaxCombineDoubleWrapper<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxDoubleState<IS_MAX>, double>;
		break;

	case PhysicalType::INT128:
		function.state_size = PacMinMaxIntStateSize<true, IS_MAX, 128>;
		function.initialize = PacMinMaxIntInitialize<true, IS_MAX, 128>;
		function.update = PacMinMaxScatterUpdateHugeInt<IS_MAX>;
		function.simple_update = PacMinMaxUpdateHugeInt<IS_MAX>;
		function.combine = PacMinMaxCombineHugeIntSigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX, 128>, hugeint_t>;
		break;

	case PhysicalType::UINT128:
		function.state_size = PacMinMaxIntStateSize<false, IS_MAX, 128>;
		function.initialize = PacMinMaxIntInitialize<false, IS_MAX, 128>;
		function.update = PacMinMaxScatterUpdateUHugeInt<IS_MAX>;
		function.simple_update = PacMinMaxUpdateUHugeInt<IS_MAX>;
		function.combine = PacMinMaxCombineHugeIntUnsigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<false, IS_MAX, 128>, uhugeint_t>;
		break;

	default:
		throw NotImplementedException("pac_%s not implemented for type %s", IS_MAX ? "max" : "min",
		                              value_type.ToString());
	}

	// Get mi and seed
	double mi = 128.0;
	if (args.size() >= 3) {
		if (!args[2]->IsFoldable()) {
			throw InvalidInputException("pac_%s: mi parameter must be a constant", IS_MAX ? "max" : "min");
		}
		auto mi_val = ExpressionExecutor::EvaluateScalar(ctx, *args[2]);
		mi = mi_val.GetValue<double>();
		if (mi <= 0.0) {
			throw InvalidInputException("pac_%s: mi must be > 0", IS_MAX ? "max" : "min");
		}
	}

	uint64_t seed = std::random_device {}();
	Value pac_seed_val;
	if (ctx.TryGetCurrentSetting("pac_seed", pac_seed_val) && !pac_seed_val.IsNull()) {
		seed = uint64_t(pac_seed_val.GetValue<int64_t>());
	}

	return make_uniq<PacBindData>(mi, seed);
}

// ============================================================================
// Registration
// ============================================================================

void RegisterPacMinFunctions(ExtensionLoader &loader) {
	AggregateFunctionSet fcn_set("pac_min");

	// Register with ANY type - bind callback handles specialization
	// 2-param version: pac_min(hash, value)
	fcn_set.AddFunction(AggregateFunction("pac_min", {LogicalType::UBIGINT, LogicalType::ANY}, LogicalType::ANY,
	                                      nullptr, nullptr, nullptr, nullptr, nullptr,
	                                      FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, PacMinMaxBind<false>));

	// 3-param version: pac_min(hash, value, mi)
	fcn_set.AddFunction(AggregateFunction("pac_min", {LogicalType::UBIGINT, LogicalType::ANY, LogicalType::DOUBLE},
	                                      LogicalType::ANY, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                      FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, PacMinMaxBind<false>));

	loader.RegisterFunction(fcn_set);
}

void RegisterPacMaxFunctions(ExtensionLoader &loader) {
	AggregateFunctionSet fcn_set("pac_max");

	// Register with ANY type - bind callback handles specialization
	// 2-param version: pac_max(hash, value)
	fcn_set.AddFunction(AggregateFunction("pac_max", {LogicalType::UBIGINT, LogicalType::ANY}, LogicalType::ANY,
	                                      nullptr, nullptr, nullptr, nullptr, nullptr,
	                                      FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, PacMinMaxBind<true>));

	// 3-param version: pac_max(hash, value, mi)
	fcn_set.AddFunction(AggregateFunction("pac_max", {LogicalType::UBIGINT, LogicalType::ANY, LogicalType::DOUBLE},
	                                      LogicalType::ANY, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                      FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, PacMinMaxBind<true>));

	loader.RegisterFunction(fcn_set);
}

// Explicit template instantiations
#define INST_U(NAME)                                                                                                   \
	template void NAME<false>(Vector *, AggregateInputData &, idx_t, data_ptr_t, idx_t);                               \
	template void NAME<true>(Vector *, AggregateInputData &, idx_t, data_ptr_t, idx_t)
#define INST_S(NAME)                                                                                                   \
	template void NAME<false>(Vector *, AggregateInputData &, idx_t, Vector &, idx_t);                                 \
	template void NAME<true>(Vector *, AggregateInputData &, idx_t, Vector &, idx_t)
#define INST_C(NAME)                                                                                                   \
	template void NAME<false>(Vector &, Vector &, AggregateInputData &, idx_t);                                        \
	template void NAME<true>(Vector &, Vector &, AggregateInputData &, idx_t)

INST_U(PacMinMaxUpdateInt8);
INST_U(PacMinMaxUpdateInt16);
INST_U(PacMinMaxUpdateInt32);
INST_U(PacMinMaxUpdateInt64);
INST_U(PacMinMaxUpdateUInt8);
INST_U(PacMinMaxUpdateUInt16);
INST_U(PacMinMaxUpdateUInt32);
INST_U(PacMinMaxUpdateUInt64);
INST_U(PacMinMaxUpdateFloat);
INST_U(PacMinMaxUpdateDoubleW);
INST_U(PacMinMaxUpdateHugeInt);
INST_U(PacMinMaxUpdateUHugeInt);

INST_S(PacMinMaxScatterUpdateInt8);
INST_S(PacMinMaxScatterUpdateInt16);
INST_S(PacMinMaxScatterUpdateInt32);
INST_S(PacMinMaxScatterUpdateInt64);
INST_S(PacMinMaxScatterUpdateUInt8);
INST_S(PacMinMaxScatterUpdateUInt16);
INST_S(PacMinMaxScatterUpdateUInt32);
INST_S(PacMinMaxScatterUpdateUInt64);
INST_S(PacMinMaxScatterUpdateFloat);
INST_S(PacMinMaxScatterUpdateDoubleW);
INST_S(PacMinMaxScatterUpdateHugeInt);
INST_S(PacMinMaxScatterUpdateUHugeInt);

INST_C(PacMinMaxCombineIntSigned);
INST_C(PacMinMaxCombineIntUnsigned);
INST_C(PacMinMaxCombineDoubleWrapper);
INST_C(PacMinMaxCombineHugeIntSigned);
INST_C(PacMinMaxCombineHugeIntUnsigned);
} // namespace duckdb
