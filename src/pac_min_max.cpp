#include "include/pac_min_max.hpp"

namespace duckdb {

// Integer state alias: PacMinMaxIntState<SIGNED, IS_MAX, MAXLEVEL>
// MAXLEVEL is the bit width: 8, 16, 32, 64, or 128
template <bool SIGNED, bool IS_MAX, int MAXLEVEL = 128>
using PacMinMaxIntState = PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL>;

// Float state: uses float[64] (256 bytes)
template <bool IS_MAX>
using PacMinMaxFloatState = PacMinMaxFloatingState<float, IS_MAX>;

// Double state: uses double[64] (512 bytes)
template <bool IS_MAX>
using PacMinMaxDoubleState = PacMinMaxFloatingState<double, IS_MAX>;

// ============================================================================
// FlushBufferIfNeeded - flush input buffer and transition to aggregation mode
// Only used in banked mode. No-op for non-banked or floating-point states.
// ============================================================================

// Forward declaration of PacMinMaxUpdateOneAggregation
template <class State, bool IS_MAX>
AUTOVECTORIZE static inline void PacMinMaxUpdateOneAggregation(State &state, uint64_t key_hash,
                                                               typename State::ValueType value,
                                                               ArenaAllocator &allocator);

// Helper to flush input buffer for integer states (banked mode only)
template <bool SIGNED, bool IS_MAX, int MAXLEVEL>
static inline void FlushBufferIfNeeded(PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL> &state, ArenaAllocator &allocator) {
#if !defined(PAC_MINMAX_NONBANKED) && !defined(PAC_MINMAX_NOBUFFERING)
	if (state.IsBuffering()) {
		// Copy buffer to stack before AllocateFirstLevel overwrites union
		using State = PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL>;
		uint64_t saved_hashes[State::BUFFER_CAPACITY];
		typename State::TMAX saved_values[State::BUFFER_CAPACITY];
		uint8_t buf_count = state.GetBufferCount();
		for (uint8_t i = 0; i < buf_count; i++) {
			saved_hashes[i] = state.buf_hashes[i];
			saved_values[i] = state.buf_values[i];
		}
		// Transition to aggregation mode
		state.AllocateFirstLevel(allocator);
		// Process buffered values
		for (uint8_t i = 0; i < buf_count; i++) {
			PacMinMaxUpdateOneAggregation<State, IS_MAX>(state, saved_hashes[i], saved_values[i], allocator);
		}
	}
#endif
}

// Pointer overload for use in Finalize
template <bool SIGNED, bool IS_MAX, int MAXLEVEL>
static inline void FlushBufferIfNeeded(PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL> *state, ArenaAllocator &allocator) {
	FlushBufferIfNeeded(*state, allocator);
}

// No-op for floating-point states (no buffering)
template <typename T, bool IS_MAX>
static inline void FlushBufferIfNeeded(PacMinMaxFloatingState<T, IS_MAX> &, ArenaAllocator &) {
}
template <typename T, bool IS_MAX>
static inline void FlushBufferIfNeeded(PacMinMaxFloatingState<T, IS_MAX> *, ArenaAllocator &) {
}

// ============================================================================
// PacMinMaxUpdateOneAggregation - update without buffering check
// ============================================================================

template <class State, bool IS_MAX>
AUTOVECTORIZE static inline void PacMinMaxUpdateOneAggregation(State &state, uint64_t key_hash,
                                                               typename State::ValueType value,
                                                               ArenaAllocator &allocator) {
#ifdef PAC_MINMAX_NONBANKED
	// NONBANKED: state is already initialized
#ifndef PAC_MINMAX_NOBOUNDOPT
	if (!PAC_IS_BETTER(value, state.global_bound)) {
		return; // early out
	}
#endif
	UpdateExtremes<typename State::ValueType, IS_MAX>(state.extremes, key_hash, value);
	state.RecomputeBound();
#else
	// Banked mode: ensure first level is allocated (needed for NOBUFFERING mode)
	if (!state.initialized) {
		state.AllocateFirstLevel(allocator);
	}
#ifndef PAC_MINMAX_NOBOUNDOPT
	if (!PAC_IS_BETTER(value, state.global_bound)) {
		return; // early out
	}
#endif
	state.MaybeUpgrade(allocator, value);
	state.UpdateAtCurrentLevel(key_hash, value);
	state.RecomputeBound();
#endif
}

// ============================================================================
// Input buffering helpers - use overloading to avoid if constexpr (C++17)
// ============================================================================

// Try to buffer a value for integer states - returns true if buffered, false if should aggregate
template <bool SIGNED, bool IS_MAX, int MAXLEVEL>
static inline bool TryBufferValue(PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL> &state, uint64_t key_hash,
                                  typename PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL>::TMAX value,
                                  ArenaAllocator &allocator) {
#if !defined(PAC_MINMAX_NONBANKED) && !defined(PAC_MINMAX_NOBUFFERING)
	using State = PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL>;
	if (state.IsBuffering()) {
		uint8_t buf_idx = state.GetBufferCount();
		if (buf_idx < State::BUFFER_CAPACITY) {
			// Still have buffer space - store and return
			state.buf_hashes[buf_idx] = key_hash;
			state.buf_values[buf_idx] = value;
			state.SetBufferCount(buf_idx + 1);
			return true;
		}
		// Buffer full - flush it (sets buffering=false, initialized=true)
		FlushBufferIfNeeded(state, allocator);
	}
#endif
	return false;
}

// Floating-point states don't buffer - always return false
template <typename T, bool IS_MAX>
static inline bool TryBufferValue(PacMinMaxFloatingState<T, IS_MAX> &, uint64_t, T, ArenaAllocator &) {
	return false;
}

// Flush src's buffer directly into dst for integer states - returns true if src was buffering
template <bool SIGNED, bool IS_MAX, int MAXLEVEL>
static inline bool FlushSrcBufferIntoDst(PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL> *src,
                                         PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL> *dst, ArenaAllocator &allocator) {
#if !defined(PAC_MINMAX_NONBANKED) && !defined(PAC_MINMAX_NOBUFFERING)
	using State = PacMinMaxState<SIGNED, IS_MAX, MAXLEVEL>;
	if (src->IsBuffering()) {
		uint8_t buf_count = src->GetBufferCount();
		for (uint8_t j = 0; j < buf_count; j++) {
			PacMinMaxUpdateOneAggregation<State, IS_MAX>(*dst, src->buf_hashes[j], src->buf_values[j], allocator);
		}
		return true;
	}
#endif
	return false;
}

// Floating-point states don't buffer - always return false
template <typename T, bool IS_MAX>
static inline bool FlushSrcBufferIntoDst(PacMinMaxFloatingState<T, IS_MAX> *, PacMinMaxFloatingState<T, IS_MAX> *,
                                         ArenaAllocator &) {
	return false;
}

// ============================================================================
// PacMinMaxUpdateOne - process one value into the aggregation state (64 extremes)
// Works for both int and float states via the common interface
// ============================================================================

template <class State, bool IS_MAX>
AUTOVECTORIZE static inline void PacMinMaxUpdateOne(State &state, uint64_t key_hash, typename State::ValueType value,
                                                    ArenaAllocator &allocator) {
	if (TryBufferValue(state, key_hash, value, allocator)) {
		return; // Value was buffered, don't aggregate yet
	}
	PacMinMaxUpdateOneAggregation<State, IS_MAX>(state, key_hash, value, allocator);
}

// DuckDB method for processing one vector for  aggregation without GROUP BY (there is only a single state)
template <class State, bool IS_MAX, class INPUT_TYPE>
static void PacMinMaxUpdate(Vector inputs[], AggregateInputData &aggr, idx_t, data_ptr_t state_p, idx_t count) {
	auto &state = *reinterpret_cast<State *>(state_p);
#ifdef PAC_MINMAX_UNSAFENULL
	if (state.seen_null)
		return;
#endif

	UnifiedVectorFormat hash_data, value_data;
	inputs[0].ToUnifiedFormat(count, hash_data);
	inputs[1].ToUnifiedFormat(count, value_data);
	auto hashes = UnifiedVectorFormat::GetData<uint64_t>(hash_data);
	auto values = UnifiedVectorFormat::GetData<INPUT_TYPE>(value_data);

	if (hash_data.validity.AllValid() && value_data.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			PacMinMaxUpdateOne<State, IS_MAX>(
			    state, hashes[hash_data.sel->get_index(i)],
			    static_cast<typename State::ValueType>(values[value_data.sel->get_index(i)]), aggr.allocator);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto h_idx = hash_data.sel->get_index(i);
			auto v_idx = value_data.sel->get_index(i);
			if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
#ifdef PAC_MINMAX_UNSAFENULL
				state.seen_null = true;
				return;
#else
				continue; // safe mode: ignore NULLs
#endif
			}
			PacMinMaxUpdateOne<State, IS_MAX>(state, hashes[h_idx],
			                                  static_cast<typename State::ValueType>(values[v_idx]), aggr.allocator);
		}
	}
}

// DuckDB method for processing one vector for GROUP BY aggregation (so there are many states))
template <class State, bool IS_MAX, class INPUT_TYPE>
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
#ifdef PAC_MINMAX_UNSAFENULL
		if (state->seen_null) {
			continue;
		}
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			state->seen_null = true;
			continue;
		}
#else
		if (!hash_data.validity.RowIsValid(h_idx) || !value_data.validity.RowIsValid(v_idx)) {
			continue; // safe mode: ignore NULLs
		}
#endif
		PacMinMaxUpdateOne<State, IS_MAX>(*state, hashes[h_idx], static_cast<typename State::ValueType>(values[v_idx]),
		                                  aggr.allocator);
	}
}

// combine two Min/Max aggregate states (used in parallel query processing and partitioned aggregation)
template <class State, bool IS_MAX>
AUTOVECTORIZE static void PacMinMaxCombine(Vector &src, Vector &dst, AggregateInputData &aggr, idx_t count) {
	auto src_state = FlatVector::GetData<State *>(src);
	auto dst_state = FlatVector::GetData<State *>(dst);

	for (idx_t i = 0; i < count; i++) {
#ifdef PAC_MINMAX_UNSAFENULL
		if (src_state[i]->seen_null) {
			dst_state[i]->seen_null = true;
		}
		if (dst_state[i]->seen_null) {
			continue;
		}
#endif
#ifdef PAC_MINMAX_NONBANKED
		// States are already initialized by PacMinMaxInitialize
		auto *s = src_state[i];
		auto *d = dst_state[i];
		for (int j = 0; j < 64; j++) {
			d->extremes[j] = PAC_BETTER(d->extremes[j], s->extremes[j]);
		}
		d->global_bound = ComputeGlobalBound<typename State::ValueType, typename State::ValueType, IS_MAX>(d->extremes);
#else
		auto *s = src_state[i];
		auto *d = dst_state[i];

		// First flush dst's buffer (allocates in dst which we keep)
		FlushBufferIfNeeded(*d, aggr.allocator);

		// If src is buffering, flush its values directly into dst (avoids allocating in src)
		if (FlushSrcBufferIntoDst(s, d, aggr.allocator)) {
			continue;
		}
		d->CombineWith(*s, aggr.allocator);
#endif
	}
}

// Finalize computes the final result from the aggregate state
template <class State, class RESULT_TYPE>
static void PacMinMaxFinalize(Vector &states, AggregateInputData &input, Vector &result, idx_t count, idx_t offset) {
	auto state = FlatVector::GetData<State *>(states);
	auto data = FlatVector::GetData<RESULT_TYPE>(result);
	auto &result_mask = FlatVector::Validity(result);

	uint64_t seed = input.bind_data ? input.bind_data->Cast<PacBindData>().seed : std::random_device {}();
	std::mt19937_64 gen(seed);
	double mi = input.bind_data ? input.bind_data->Cast<PacBindData>().mi : 128.0;
	bool use_deterministic_noise =
	    input.bind_data ? input.bind_data->Cast<PacBindData>().use_deterministic_noise : true;

	// Get init value to detect never-updated counters (replace with 0 to avoid leaking type info)
	double init_val = State::InitAsDouble();

	for (idx_t i = 0; i < count; i++) {
#ifdef PAC_MINMAX_UNSAFENULL
		if (state[i]->seen_null) {
			result_mask.SetInvalid(offset + i);
			continue;
		}
#endif
		// Flush any buffered values before finalization
		FlushBufferIfNeeded(state[i], input.allocator);
		double buf[64];
		state[i]->GetTotalsAsDouble(buf);
		for (int j = 0; j < 64; j++) {
			// Replace init values with 0 to avoid leaking domain/type information
			if (buf[j] == init_val) {
				buf[j] = 0.0;
			} else {
				buf[j] /= 2.0;
			}
		}
		data[offset + i] =
		    FromDouble<RESULT_TYPE>(PacNoisySampleFrom64Counters(buf, mi, gen, use_deterministic_noise) + buf[41]);
	}
}

// Unified State size and initialize (StateSize() is  a member of each state class)
template <class State>
static idx_t PacMinMaxStateSize(const AggregateFunction &) {
	return State::StateSize();
}

template <class State>
static void PacMinMaxInitialize(const AggregateFunction &, data_ptr_t p) {
	memset(p, 0, State::StateSize());
#ifdef PAC_MINMAX_NONBANKED
	reinterpret_cast<State *>(p)->Initialize();
#else
	// Integer states start in buffering mode, floating-point states initialize immediately
	reinterpret_cast<State *>(p)->InitializeBufferingMode();
#endif
}

// ============================================================================
//  Update/ScatterUpdate wrappers that instantiate the templates
// ============================================================================

// Integer updates (MAXLEVEL is bit width: 8, 16, 32, 64, 128)
#define INT_UPDATE_WRAPPER(NAME, SIGNED, MAXLEVEL, INTYPE)                                                             \
	template <bool IS_MAX>                                                                                             \
	void NAME(Vector inputs[], AggregateInputData &aggr, idx_t n, data_ptr_t state_p, idx_t count) {                   \
		PacMinMaxUpdate<PacMinMaxIntState<SIGNED, IS_MAX, MAXLEVEL>, IS_MAX, INTYPE>(inputs, aggr, n, state_p, count); \
	}
// Use type-appropriate MAXLEVEL: each type cascades up to its own width only
INT_UPDATE_WRAPPER(PacMinMaxUpdateInt8, true, 8, int8_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateInt16, true, 16, int16_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateInt32, true, 32, int32_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateInt64, true, 64, int64_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUInt8, false, 8, uint8_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUInt16, false, 16, uint16_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUInt32, false, 32, uint32_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUInt64, false, 64, uint64_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateHugeInt, true, 128, hugeint_t)
INT_UPDATE_WRAPPER(PacMinMaxUpdateUHugeInt, false, 128, uhugeint_t)
#undef INT_UPDATE_WRAPPER

// Float updates (uses float[64] state)
template <bool IS_MAX>
void PacMinMaxUpdateFloat(Vector in[], AggregateInputData &a, idx_t n, data_ptr_t s, idx_t c) {
	PacMinMaxUpdate<PacMinMaxFloatState<IS_MAX>, IS_MAX, float>(in, a, n, s, c);
}

// Double updates (uses double[64] state)
template <bool IS_MAX>
void PacMinMaxUpdateDoubleW(Vector in[], AggregateInputData &a, idx_t n, data_ptr_t s, idx_t c) {
	PacMinMaxUpdate<PacMinMaxDoubleState<IS_MAX>, IS_MAX, double>(in, a, n, s, c);
}

// Integer scatter updates (MAXLEVEL is bit width: 8, 16, 32, 64, 128)
#define INT_SCATTER_WRAPPER(NAME, SIGNED, MAXLEVEL, INTYPE)                                                            \
	template <bool IS_MAX>                                                                                             \
	void NAME(Vector input[], AggregateInputData &aggr, idx_t n, Vector &states, idx_t count) {                        \
		PacMinMaxScatterUpdate<PacMinMaxIntState<SIGNED, IS_MAX, MAXLEVEL>, IS_MAX, INTYPE>(input, aggr, n, states,    \
		                                                                                    count);                    \
	}
// Use type-appropriate MAXLEVEL: each type cascades up to its own width only
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateInt8, true, 8, int8_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateInt16, true, 16, int16_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateInt32, true, 32, int32_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateInt64, true, 64, int64_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUInt8, false, 8, uint8_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUInt16, false, 16, uint16_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUInt32, false, 32, uint32_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUInt64, false, 64, uint64_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateHugeInt, true, 128, hugeint_t)
INT_SCATTER_WRAPPER(PacMinMaxScatterUpdateUHugeInt, false, 128, uhugeint_t)
#undef INT_SCATTER_WRAPPER

// Float scatter updates (uses float[64] state)
template <bool IS_MAX>
void PacMinMaxScatterUpdateFloat(Vector in[], AggregateInputData &a, idx_t n, Vector &s, idx_t c) {
	PacMinMaxScatterUpdate<PacMinMaxFloatState<IS_MAX>, IS_MAX, float>(in, a, n, s, c);
}

// Double scatter updates (uses double[64] state)
template <bool IS_MAX>
void PacMinMaxScatterUpdateDoubleW(Vector in[], AggregateInputData &a, idx_t n, Vector &s, idx_t c) {
	PacMinMaxScatterUpdate<PacMinMaxDoubleState<IS_MAX>, IS_MAX, double>(in, a, n, s, c);
}

// Combine wrappers - one per (SIGNED, MAXLEVEL) combination
template <bool IS_MAX>
void PacMinMaxCombineInt8Signed(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<true, IS_MAX, 8>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineInt16Signed(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<true, IS_MAX, 16>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineInt32Signed(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<true, IS_MAX, 32>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineInt64Signed(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<true, IS_MAX, 64>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineInt8Unsigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<false, IS_MAX, 8>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineInt16Unsigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<false, IS_MAX, 16>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineInt32Unsigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<false, IS_MAX, 32>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineInt64Unsigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<false, IS_MAX, 64>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineFloat(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxFloatState<IS_MAX>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineDouble(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxDoubleState<IS_MAX>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineHugeIntSigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<true, IS_MAX, 128>, IS_MAX>(src, dst, a, c);
}
template <bool IS_MAX>
void PacMinMaxCombineHugeIntUnsigned(Vector &src, Vector &dst, AggregateInputData &a, idx_t c) {
	PacMinMaxCombine<PacMinMaxIntState<false, IS_MAX, 128>, IS_MAX>(src, dst, a, c);
}

// Bind function: decides from the LogicalType which implementation function to instantiate
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
	// Each type uses MAXLEVEL matching its bit width: int8=8, int16=16, int32=32, int64=64, hugeint=128
	switch (physical_type) {
	case PhysicalType::INT8:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<true, IS_MAX, 8>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<true, IS_MAX, 8>>;
		function.update = PacMinMaxScatterUpdateInt8<IS_MAX>;
		function.simple_update = PacMinMaxUpdateInt8<IS_MAX>;
		function.combine = PacMinMaxCombineInt8Signed<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX, 8>, int8_t>;
		break;

	case PhysicalType::INT16:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<true, IS_MAX, 16>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<true, IS_MAX, 16>>;
		function.update = PacMinMaxScatterUpdateInt16<IS_MAX>;
		function.simple_update = PacMinMaxUpdateInt16<IS_MAX>;
		function.combine = PacMinMaxCombineInt16Signed<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX, 16>, int16_t>;
		break;

	case PhysicalType::INT32:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<true, IS_MAX, 32>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<true, IS_MAX, 32>>;
		function.update = PacMinMaxScatterUpdateInt32<IS_MAX>;
		function.simple_update = PacMinMaxUpdateInt32<IS_MAX>;
		function.combine = PacMinMaxCombineInt32Signed<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX, 32>, int32_t>;
		break;

	case PhysicalType::INT64:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<true, IS_MAX, 64>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<true, IS_MAX, 64>>;
		function.update = PacMinMaxScatterUpdateInt64<IS_MAX>;
		function.simple_update = PacMinMaxUpdateInt64<IS_MAX>;
		function.combine = PacMinMaxCombineInt64Signed<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX, 64>, int64_t>;
		break;

	case PhysicalType::UINT8:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<false, IS_MAX, 8>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<false, IS_MAX, 8>>;
		function.update = PacMinMaxScatterUpdateUInt8<IS_MAX>;
		function.simple_update = PacMinMaxUpdateUInt8<IS_MAX>;
		function.combine = PacMinMaxCombineInt8Unsigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<false, IS_MAX, 8>, uint8_t>;
		break;

	case PhysicalType::UINT16:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<false, IS_MAX, 16>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<false, IS_MAX, 16>>;
		function.update = PacMinMaxScatterUpdateUInt16<IS_MAX>;
		function.simple_update = PacMinMaxUpdateUInt16<IS_MAX>;
		function.combine = PacMinMaxCombineInt16Unsigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<false, IS_MAX, 16>, uint16_t>;
		break;

	case PhysicalType::UINT32:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<false, IS_MAX, 32>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<false, IS_MAX, 32>>;
		function.update = PacMinMaxScatterUpdateUInt32<IS_MAX>;
		function.simple_update = PacMinMaxUpdateUInt32<IS_MAX>;
		function.combine = PacMinMaxCombineInt32Unsigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<false, IS_MAX, 32>, uint32_t>;
		break;

	case PhysicalType::UINT64:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<false, IS_MAX, 64>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<false, IS_MAX, 64>>;
		function.update = PacMinMaxScatterUpdateUInt64<IS_MAX>;
		function.simple_update = PacMinMaxUpdateUInt64<IS_MAX>;
		function.combine = PacMinMaxCombineInt64Unsigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<false, IS_MAX, 64>, uint64_t>;
		break;

	case PhysicalType::FLOAT:
		function.state_size = PacMinMaxStateSize<PacMinMaxFloatState<IS_MAX>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxFloatState<IS_MAX>>;
		function.update = PacMinMaxScatterUpdateFloat<IS_MAX>;
		function.simple_update = PacMinMaxUpdateFloat<IS_MAX>;
		function.combine = PacMinMaxCombineFloat<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxFloatState<IS_MAX>, float>;
		break;

	case PhysicalType::DOUBLE:
		function.state_size = PacMinMaxStateSize<PacMinMaxDoubleState<IS_MAX>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxDoubleState<IS_MAX>>;
		function.update = PacMinMaxScatterUpdateDoubleW<IS_MAX>;
		function.simple_update = PacMinMaxUpdateDoubleW<IS_MAX>;
		function.combine = PacMinMaxCombineDouble<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxDoubleState<IS_MAX>, double>;
		break;

	case PhysicalType::INT128:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<true, IS_MAX, 128>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<true, IS_MAX, 128>>;
		function.update = PacMinMaxScatterUpdateHugeInt<IS_MAX>;
		function.simple_update = PacMinMaxUpdateHugeInt<IS_MAX>;
		function.combine = PacMinMaxCombineHugeIntSigned<IS_MAX>;
		function.finalize = PacMinMaxFinalize<PacMinMaxIntState<true, IS_MAX, 128>, hugeint_t>;
		break;

	case PhysicalType::UINT128:
		function.state_size = PacMinMaxStateSize<PacMinMaxIntState<false, IS_MAX, 128>>;
		function.initialize = PacMinMaxInitialize<PacMinMaxIntState<false, IS_MAX, 128>>;
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
		if (mi < 0.0) {
			throw InvalidInputException("pac_%s: mi must be >= 0", IS_MAX ? "max" : "min");
		}
	}

	uint64_t seed = std::random_device {}();
	Value pac_seed_val;
	if (ctx.TryGetCurrentSetting("pac_seed", pac_seed_val) && !pac_seed_val.IsNull()) {
		seed = uint64_t(pac_seed_val.GetValue<int64_t>());
	}

	bool use_deterministic_noise = false;
	Value pac_det_noise_val;
	if (ctx.TryGetCurrentSetting("pac_deterministic_noise", pac_det_noise_val) && !pac_det_noise_val.IsNull()) {
		use_deterministic_noise = pac_det_noise_val.GetValue<bool>();
	}

	return make_uniq<PacBindData>(mi, seed, 1.0, use_deterministic_noise);
}

// Registration
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

INST_C(PacMinMaxCombineInt8Signed);
INST_C(PacMinMaxCombineInt16Signed);
INST_C(PacMinMaxCombineInt32Signed);
INST_C(PacMinMaxCombineInt64Signed);
INST_C(PacMinMaxCombineInt8Unsigned);
INST_C(PacMinMaxCombineInt16Unsigned);
INST_C(PacMinMaxCombineInt32Unsigned);
INST_C(PacMinMaxCombineInt64Unsigned);
INST_C(PacMinMaxCombineFloat);
INST_C(PacMinMaxCombineDouble);
INST_C(PacMinMaxCombineHugeIntSigned);
INST_C(PacMinMaxCombineHugeIntUnsigned);
} // namespace duckdb
