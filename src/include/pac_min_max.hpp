//
// pac_min_max.hpp - PAC MIN/MAX aggregate functions
//

#ifndef PAC_MIN_MAX_HPP
#define PAC_MIN_MAX_HPP

#include "duckdb.hpp"
#include "pac_aggregate.hpp"

namespace duckdb {

void RegisterPacMinFunctions(ExtensionLoader &loader);
void RegisterPacMaxFunctions(ExtensionLoader &loader);

// Recompute global_bound every N updates (reduces overhead of bound computation)
static constexpr uint16_t BOUND_RECOMPUTE_INTERVAL = 2048;

// Type-agnostic comparison macros (IS_MAX must be in scope)
#define PAC_IS_BETTER(a, b) (IS_MAX ? ((a) > (b)) : ((a) < (b)))
#define PAC_BETTER(a, b)    (PAC_IS_BETTER(a, b) ? (a) : (b))
#define PAC_WORSE(a, b)     (PAC_IS_BETTER(a, b) ? (b) : (a))

// ============================================================================
// SIMD-friendly update functions for Min/Max extremes arrays (SWAR layout)
// prototyped here: https://godbolt.org/z/dYWqd3qEW
// ============================================================================
// T: element type (int8_t, uint8_t, int16_t, uint16_t, int32_t, uint32_t)
// BitsT: signed type matching sizeof(T) for mask generation
// IS_MAX: true for pac_max, false for pac_min
// MASK: broadcast mask (e.g., 0x0101010101010101 for 8-bit)
// SHIFTS: number of uint64_t words (8 for 8-bit, 16 for 16-bit, 32 for 32-bit)

template <typename T, typename BitsT, bool IS_MAX, uint64_t MASK, int SHIFTS>
AUTOVECTORIZE static inline void UpdateExtremesSIMD(T *__restrict__ result, uint64_t key_hash, T value) {
	union {
		uint64_t u64[SHIFTS];
		BitsT bits[64];
	} buf;
	for (int i = 0; i < SHIFTS; i++) {
		buf.u64[i] = (key_hash >> i) & MASK;
	}
	// Both buf.bits and result are in SWAR layout - direct indexing
	for (int i = 0; i < 64; i++) {
		T mask = static_cast<T>(-buf.bits[i]);
		T extreme = PAC_BETTER(value, result[i]);
		result[i] = (extreme & mask) | (result[i] & ~mask);
	}
}

// Specialization for uint8_t MAX: uses optimized value & mask pattern
template <>
AUTOVECTORIZE inline void
UpdateExtremesSIMD<uint8_t, int8_t, true, 0x0101010101010101ULL, 8>(uint8_t *__restrict__ result, uint64_t key_hash,
                                                                    uint8_t value) {
	uint64_t buf[8];
	for (int i = 0; i < 8; i++) {
		buf[i] = (key_hash >> i) & 0x0101010101010101ULL;
	}
	int8_t *__restrict__ bits = reinterpret_cast<int8_t *>(buf);
	// Both bits and result are in SWAR layout - direct indexing
	for (int i = 0; i < 64; i++) {
		uint8_t mask = static_cast<uint8_t>(-bits[i]);
		result[i] = std::max(static_cast<uint8_t>(value & mask), result[i]);
	}
}

// Simple scalar update for 64-bit types (linear layout, no SWAR benefit)
template <typename T, bool IS_MAX>
AUTOVECTORIZE static inline void UpdateExtremes(T *__restrict__ result, uint64_t key_hash, T value) {
	for (int i = 0; i < 64; i++) {
		if (((key_hash >> i) & 1ULL) && PAC_IS_BETTER(value, result[i])) {
			result[i] = value;
		}
	}
}

// ============================================================================
// SWAR index helpers
// ============================================================================
// Convert linear bit index to SWAR index for given element width.
// This must match the layout produced by the union-based bit extraction in UpdateExtremesSIMD.
// ELEM_PER_U64: elements per uint64_t (8 for 8-bit, 4 for 16-bit, 2 for 32-bit)
template <int ELEM_PER_U64>
static inline int LinearToSWAR(int linear_idx) {
	constexpr int NUM_U64 = 64 / ELEM_PER_U64; // The union packs bits such that linear index L maps to SWAR index:
	return ELEM_PER_U64 * (linear_idx % NUM_U64) + (linear_idx / NUM_U64); // EPU * (L % NUM_U64) + (L / NUM_U64)
}

// Extract from SWAR layout to linear double array (where we do noising)
template <typename T, int ELEM_PER_U64>
static inline void ExtractSWAR(const T *swar_data, double *dst) {
	for (int i = 0; i < 64; i++) {
		int swar_idx = LinearToSWAR<ELEM_PER_U64>(i);
		dst[i] = static_cast<double>(swar_data[swar_idx]);
	}
}

// ============================================================================
// Helper to recompute global bound from extremes array
// NOTE: ToDouble<T> is already defined in pac_aggregate.hpp
// ============================================================================
template <typename T, typename BOUND_T, bool IS_MAX>
AUTOVECTORIZE static inline BOUND_T ComputeGlobalBound(const T *extremes) {
	BOUND_T bound = static_cast<BOUND_T>(extremes[0]);
	for (int i = 1; i < 64; i++) {
		BOUND_T ext = static_cast<BOUND_T>(extremes[i]);
		bound = PAC_WORSE(ext, bound);
	}
	return bound;
}

// ============================================================================
// PAC_MIN/PAC_MAX(hash_key, value) aggregate functions
// ============================================================================
// State: 64 extreme values, one for each bit position
// Update: for each (key_hash, value), update extremes[i] if bit i of key_hash is set
// Finalize: compute the PAC-noised min/max from the 64 counters
//
// Optimization: keep a global bound (min of all maxes, or max of all mins)
// to skip processing values that can't affect any extreme.
//
// Cascading: start with smallest integer type, upgrade when value doesn't fit.
// Define PAC_MINMAX_NONCASCADING to use fixed-width arrays (input value type).
// Define PAC_MINMAX_NONLAZY to pre-allocate all levels at initialization.

//#define PAC_MINMAX_NONCASCADING 1
//#define PAC_MINMAX_NONLAZY 1
//#define PAC_MINMAX_NOBOUNDOPT 1

// ============================================================================
// Integer state for min/max
// ============================================================================
// SIGNED: signed vs unsigned types
// IS_MAX: true for pac_max, false for pac_min
// MAXWIDTH: maximum bit width needed (8, 16, 32, 64, or 128)
//
// All pointer fields are always defined but ordered by width at the end of the struct.
// We use offsetof() to report smaller state_size to DuckDB, so it allocates only
// the memory needed for the MAXWIDTH. Regular if guards (not if constexpr) prevent
// access to unallocated fields - compiler optimizes these away since MAXWIDTH is constant.
template <bool SIGNED, bool IS_MAX, int MAXWIDTH = 64>
struct PacMinMaxIntState {
	// Level range constants for unified interface
	static constexpr int MINLEVEL = 8;
	static constexpr int MAXLEVEL = MAXWIDTH;

	// Type aliases based on signedness
	using T8 = typename std::conditional<SIGNED, int8_t, uint8_t>::type;
	using T16 = typename std::conditional<SIGNED, int16_t, uint16_t>::type;
	using T32 = typename std::conditional<SIGNED, int32_t, uint32_t>::type;
	using T64 = typename std::conditional<SIGNED, int64_t, uint64_t>::type;
	using T128 = typename std::conditional<SIGNED, hugeint_t, uhugeint_t>::type;
	// TMAX: widest type based on MAXWIDTH
	using TMAX = typename std::conditional<
	    MAXWIDTH == 128, T128,
	    typename std::conditional<
	        MAXWIDTH == 64, T64,
	        typename std::conditional<MAXWIDTH == 32, T32,
	                                  typename std::conditional<MAXWIDTH == 16, T16, T8>::type>::type>::type>::type;
	// ValueType alias for unified interface
	using ValueType = TMAX;

	// Get init value (worst possible for the aggregation direction)
	// Uses NumericLimits which is specialized for each type
	template <typename T>
	static inline T TypeInit() {
		return IS_MAX ? NumericLimits<T>::Minimum() : NumericLimits<T>::Maximum();
	}

	// Check if value fits in a given type
	static inline bool FitsIn8(TMAX val) {
		return val >= static_cast<TMAX>(SIGNED ? INT8_MIN : 0) &&
		       val <= static_cast<TMAX>(SIGNED ? INT8_MAX : UINT8_MAX);
	}
	static inline bool FitsIn16(TMAX val) {
		return val >= static_cast<TMAX>(SIGNED ? INT16_MIN : 0) &&
		       val <= static_cast<TMAX>(SIGNED ? INT16_MAX : UINT16_MAX);
	}
	static inline bool FitsIn32(TMAX val) {
		return val >= static_cast<TMAX>(SIGNED ? INT32_MIN : 0) &&
		       val <= static_cast<TMAX>(SIGNED ? INT32_MAX : UINT32_MAX);
	}
	static inline bool FitsIn64(TMAX val) {
		return val >= static_cast<TMAX>(SIGNED ? INT64_MIN : 0) &&
		       val <= static_cast<TMAX>(SIGNED ? INT64_MAX : UINT64_MAX);
	}

	// ========== Common fields (defined once for both modes) ==========
	bool initialized;
	bool seen_null;
	uint16_t update_count;
	TMAX global_bound; // For MAX: min of all maxes; for MIN: max of all mins

#ifdef PAC_MINMAX_NONCASCADING
	// ========== Non-cascading mode: single fixed-width array ==========
	TMAX extremes[64];

	void GetTotalsAsDouble(double *dst) const {
		for (int i = 0; i < 64; i++) {
			dst[i] = ToDouble(extremes[i]);
		}
	}

	void Initialize() {
		for (int i = 0; i < 64; i++) {
			extremes[i] = TypeInit<TMAX>();
		}
		global_bound = TypeInit<TMAX>();
		update_count = 0;
		initialized = true;
	}

	// Recompute global bound - periodically called after updates
	void RecomputeBound() {
#ifndef PAC_MINMAX_NOBOUNDOPT
		if (++update_count == BOUND_RECOMPUTE_INTERVAL) {
			update_count = 0;
			global_bound = ComputeGlobalBound<TMAX, TMAX, IS_MAX>(extremes);
		}
#endif
	}
#else
	// ========== Cascading mode fields ==========
	// All pointers defined, but ordered so unused ones are at the end.
	// DuckDB allocates only up to the needed pointer based on state_size.
	uint8_t current_level; // 8, 16, 32, 64, or 128
	ArenaAllocator *allocator;

	// Pointer fields ordered by width - unused ones at the end won't be allocated
	T8 *extremes8;
	T16 *extremes16;
	T32 *extremes32;
	T64 *extremes64;
	T128 *extremes128;

	// Allocate a level's buffer
	template <typename T>
	inline T *AllocateLevel(T init_value) {
		T *buf = reinterpret_cast<T *>(allocator->Allocate(64 * sizeof(T)));
		for (int i = 0; i < 64; i++) {
			buf[i] = init_value;
		}
		return buf;
	}

	// Helper to get SWAR index, handling different sizes
	// Returns linear index for types > 4 bytes (no SWAR benefit)
	template <typename T>
	static inline int GetIndex(int i) {
		// Types > 4 bytes use linear indexing
		if (sizeof(T) > 4)
			return i;
		// Types <= 4 bytes use SWAR layout
		if (sizeof(T) == 4)
			return LinearToSWAR<2>(i);
		if (sizeof(T) == 2)
			return LinearToSWAR<4>(i);
		return LinearToSWAR<8>(i); // sizeof(T) == 1
	}

	// Upgrade from one level to the next, automatically handling SWAR vs linear layout
	// Types <= 4 bytes use SWAR layout, types > 4 bytes use linear layout
	// If src value equals src_init (never updated), use dst_init instead
	template <typename SRC_T, typename DST_T>
	inline DST_T *UpgradeLevel(SRC_T *src, SRC_T src_init, DST_T dst_init) {
		DST_T *dst = reinterpret_cast<DST_T *>(allocator->Allocate(64 * sizeof(DST_T)));
		if (src) {
			for (int i = 0; i < 64; i++) {
				int src_idx = GetIndex<SRC_T>(i);
				int dst_idx = GetIndex<DST_T>(i);
				dst[dst_idx] = (src[src_idx] == src_init) ? dst_init : static_cast<DST_T>(src[src_idx]);
			}
		} else {
			for (int i = 0; i < 64; i++) {
				dst[i] = dst_init;
			}
		}
		return dst;
	}

	void AllocateFirstLevel(ArenaAllocator &alloc) {
		allocator = &alloc;
#ifdef PAC_MINMAX_NONLAZY
		// Pre-allocate all levels that exist for this MAXWIDTH
		extremes8 = AllocateLevel<T8>(TypeInit<T8>());
		if (MAXWIDTH >= 16) {
			extremes16 = AllocateLevel<T16>(TypeInit<T16>());
		}
		if (MAXWIDTH >= 32) {
			extremes32 = AllocateLevel<T32>(TypeInit<T32>());
		}
		if (MAXWIDTH >= 64) {
			extremes64 = AllocateLevel<T64>(TypeInit<T64>());
		}
		if (MAXWIDTH >= 128) {
			extremes128 = AllocateLevel<T128>(TypeInit<T128>());
		}
		current_level = MAXWIDTH;
#else
		extremes8 = AllocateLevel<T8>(TypeInit<T8>());
		current_level = 8;
#endif
		update_count = 0;
		global_bound = TypeInit<TMAX>();
		initialized = true;
	}

	void UpgradeTo16() {
		if (MAXWIDTH >= 16) {
			extremes16 = UpgradeLevel<T8, T16>(extremes8, TypeInit<T8>(), TypeInit<T16>());
			current_level = 16;
		}
	}

	void UpgradeTo32() {
		if (MAXWIDTH >= 32) {
			extremes32 = UpgradeLevel<T16, T32>(extremes16, TypeInit<T16>(), TypeInit<T32>());
			current_level = 32;
		}
	}

	void UpgradeTo64() {
		if (MAXWIDTH >= 64) {
			extremes64 = UpgradeLevel<T32, T64>(extremes32, TypeInit<T32>(), TypeInit<T64>());
			current_level = 64;
		}
	}

	void UpgradeTo128() {
		if (MAXWIDTH >= 128) {
			extremes128 = UpgradeLevel<T64, T128>(extremes64, TypeInit<T64>(), TypeInit<T128>());
			current_level = 128;
		}
	}

	// Get value at index j, cast to type T, from whatever level is current
	template <typename T>
	T GetValueAs(int j) const {
		if (MAXWIDTH >= 128 && current_level >= 128) {
			return static_cast<T>(extremes128[j]);
		}
		if (MAXWIDTH >= 64 && current_level >= 64) {
			return static_cast<T>(extremes64[j]);
		}
		if (MAXWIDTH >= 32 && current_level >= 32) {
			return static_cast<T>(extremes32[j]);
		}
		if (MAXWIDTH >= 16 && current_level >= 16) {
			return static_cast<T>(extremes16[j]);
		}
		return static_cast<T>(extremes8[j]);
	}

	void GetTotalsAsDouble(double *dst) const {
		if (MAXWIDTH >= 128 && current_level == 128) {
			for (int i = 0; i < 64; i++) {
				dst[i] = ToDouble(extremes128[i]);
			}
			return;
		}
		if (MAXWIDTH >= 64 && current_level == 64) {
			for (int i = 0; i < 64; i++) {
				dst[i] = ToDouble(extremes64[i]);
			}
			return;
		}
		if (MAXWIDTH >= 32 && current_level == 32) {
			ExtractSWAR<T32, 2>(extremes32, dst);
			return;
		}
		if (MAXWIDTH >= 16 && current_level == 16) {
			ExtractSWAR<T16, 4>(extremes16, dst);
			return;
		}
		if (current_level == 8) {
			ExtractSWAR<T8, 8>(extremes8, dst);
			return;
		}
		// Not initialized - return init values
		double init_val = ToDouble(TypeInit<TMAX>());
		for (int i = 0; i < 64; i++) {
			dst[i] = init_val;
		}
	}

	// Upgrade level if value doesn't fit in current level
	void MaybeUpgrade(TMAX value) {
		if (MAXWIDTH >= 16 && !FitsIn8(value) && current_level == 8) {
			UpgradeTo16();
		}
		if (MAXWIDTH >= 32 && !FitsIn16(value) && current_level == 16) {
			UpgradeTo32();
		}
		if (MAXWIDTH >= 64 && !FitsIn32(value) && current_level == 32) {
			UpgradeTo64();
		}
		if (MAXWIDTH >= 128 && !FitsIn64(value) && current_level == 64) {
			UpgradeTo128();
		}
	}

	// Update extremes at current level with value
	AUTOVECTORIZE void UpdateAtCurrentLevel(uint64_t key_hash, TMAX value) {
		if (current_level == 8) {
			UpdateExtremesSIMD<T8, int8_t, IS_MAX, 0x0101010101010101ULL, 8>(extremes8, key_hash,
			                                                                 static_cast<T8>(value));
		}
		if (MAXWIDTH >= 16 && current_level == 16) {
			UpdateExtremesSIMD<T16, int16_t, IS_MAX, 0x0001000100010001ULL, 16>(extremes16, key_hash,
			                                                                    static_cast<T16>(value));
		}
		if (MAXWIDTH >= 32 && current_level == 32) {
			UpdateExtremesSIMD<T32, int32_t, IS_MAX, 0x0000000100000001ULL, 32>(extremes32, key_hash,
			                                                                    static_cast<T32>(value));
		}
		if (MAXWIDTH >= 64 && current_level == 64) {
			UpdateExtremes<T64, IS_MAX>(extremes64, key_hash, static_cast<T64>(value));
		}
		if (MAXWIDTH >= 128 && current_level == 128) {
			UpdateExtremes<T128, IS_MAX>(extremes128, key_hash, static_cast<T128>(value));
		}
	}

	// Recompute global bound from current level's extremes - periodically called after updates
	void RecomputeBound() {
#ifndef PAC_MINMAX_NOBOUNDOPT
		if (++update_count == BOUND_RECOMPUTE_INTERVAL) {
			update_count = 0;
			if (current_level == 8) {
				global_bound = ComputeGlobalBound<T8, TMAX, IS_MAX>(extremes8);
			}
			if (MAXWIDTH >= 16 && current_level == 16) {
				global_bound = ComputeGlobalBound<T16, TMAX, IS_MAX>(extremes16);
			}
			if (MAXWIDTH >= 32 && current_level == 32) {
				global_bound = ComputeGlobalBound<T32, TMAX, IS_MAX>(extremes32);
			}
			if (MAXWIDTH >= 64 && current_level == 64) {
				global_bound = ComputeGlobalBound<T64, TMAX, IS_MAX>(extremes64);
			}
			if (MAXWIDTH >= 128 && current_level == 128) {
				global_bound = ComputeGlobalBound<T128, TMAX, IS_MAX>(extremes128);
			}
		}
#endif
	}

	// Combine with another state (merge src into this)
	void CombineWith(const PacMinMaxIntState &src, ArenaAllocator &alloc) {
		if (!src.initialized) {
			return;
		}
		if (!initialized) {
			allocator = &alloc;
			extremes8 = AllocateLevel<T8>(TypeInit<T8>());
			current_level = 8;
			update_count = 0;
			global_bound = TypeInit<TMAX>();
			initialized = true;
		}
		// Upgrade this state to match src level
		if (MAXWIDTH >= 16 && current_level == 8 && current_level < src.current_level) {
			UpgradeTo16();
		}
		if (MAXWIDTH >= 32 && current_level == 16 && current_level < src.current_level) {
			UpgradeTo32();
		}
		if (MAXWIDTH >= 64 && current_level == 32 && current_level < src.current_level) {
			UpgradeTo64();
		}
		if (MAXWIDTH >= 128 && current_level == 64 && current_level < src.current_level) {
			UpgradeTo128();
		}
		// Combine at current level
		if (current_level == 8) {
			for (int j = 0; j < 64; j++) {
				extremes8[j] = PAC_BETTER(extremes8[j], src.template GetValueAs<T8>(j));
			}
		}
		if (MAXWIDTH >= 16 && current_level == 16) {
			for (int j = 0; j < 64; j++) {
				extremes16[j] = PAC_BETTER(extremes16[j], src.template GetValueAs<T16>(j));
			}
		}
		if (MAXWIDTH >= 32 && current_level == 32) {
			for (int j = 0; j < 64; j++) {
				extremes32[j] = PAC_BETTER(extremes32[j], src.template GetValueAs<T32>(j));
			}
		}
		if (MAXWIDTH >= 64 && current_level == 64) {
			for (int j = 0; j < 64; j++) {
				extremes64[j] = PAC_BETTER(extremes64[j], src.template GetValueAs<T64>(j));
			}
		}
		if (MAXWIDTH >= 128 && current_level == 128) {
			for (int j = 0; j < 64; j++) {
				extremes128[j] = PAC_BETTER(extremes128[j], src.template GetValueAs<T128>(j));
			}
		}
		RecomputeBound();
	}
#endif

	// State size for DuckDB allocation - uses offsetof to exclude unused pointer fields
	static idx_t StateSize() {
#ifdef PAC_MINMAX_NONCASCADING
		return sizeof(PacMinMaxIntState);
#else
		if (MAXWIDTH >= 128)
			return sizeof(PacMinMaxIntState);
		if (MAXWIDTH >= 64)
			return offsetof(PacMinMaxIntState, extremes128);
		if (MAXWIDTH >= 32)
			return offsetof(PacMinMaxIntState, extremes64);
		if (MAXWIDTH >= 16)
			return offsetof(PacMinMaxIntState, extremes32);
		return offsetof(PacMinMaxIntState, extremes16);
#endif
	}
};

// ============================================================================
// Float state for min/max (floating point values)
// ============================================================================
// VALUE_TYPE: float or double (determines the return type)
// MAXWIDTH: 32 for float-only, 64 for float+double cascading
// Cascading: start with float if values fit in [-1000000, 1000000], upgrade to double otherwise
//
// Same approach as integer state: all pointers defined, ordered by width at end,
// use offsetof() for smaller state_size allocation.
template <bool IS_MAX, typename VALUE_TYPE = double, int MAXWIDTH = 64>
struct PacMinMaxFloatState {
	// Level range constants for unified interface
	static constexpr int MINLEVEL = 32;
	static constexpr int MAXLEVEL = MAXWIDTH;

	// Type aliases for unified interface (matching int state naming convention)
	using T32 = float;
	using T64 = double;
	using ValueType = VALUE_TYPE;

	static constexpr float FLOAT_RANGE_MIN = -1000000.0f;
	static constexpr float FLOAT_RANGE_MAX = 1000000.0f;

	// Get init value for a given floating point type
	template <typename T>
	static inline T TypeInit() {
		return IS_MAX ? -std::numeric_limits<T>::infinity() : std::numeric_limits<T>::infinity();
	}

	// Check if value fits in float range
	static inline bool FitsInFloat(VALUE_TYPE val) {
		return val >= FLOAT_RANGE_MIN && val <= FLOAT_RANGE_MAX;
	}
	// Alias for unified interface - maps to FitsIn32 equivalent
	static inline bool FitsIn32(VALUE_TYPE val) {
		return FitsInFloat(val);
	}

	// ========== Common fields (defined once for both modes) ==========
	bool initialized;
	bool seen_null;
	uint16_t update_count;
	VALUE_TYPE global_bound;

#ifdef PAC_MINMAX_NONCASCADING
	// ========== Non-cascading mode: single fixed-width array ==========
	VALUE_TYPE extremes[64];

	void GetTotalsAsDouble(double *dst) const {
		for (int i = 0; i < 64; i++) {
			dst[i] = static_cast<double>(extremes[i]);
		}
	}

	void Initialize() {
		VALUE_TYPE init = TypeInit<VALUE_TYPE>();
		for (int i = 0; i < 64; i++) {
			extremes[i] = init;
		}
		global_bound = init;
		update_count = 0;
		initialized = true;
	}

	// Recompute global bound - periodically called after updates
	void RecomputeBound() {
#ifndef PAC_MINMAX_NOBOUNDOPT
		if (++update_count == BOUND_RECOMPUTE_INTERVAL) {
			update_count = 0;
			global_bound = ComputeGlobalBound<VALUE_TYPE, VALUE_TYPE, IS_MAX>(extremes);
		}
#endif
	}
#else
	// ========== Cascading mode fields ==========
	// All pointers defined, ordered so unused ones are at the end
	uint8_t current_level; // 32 for float, 64 for double
	ArenaAllocator *allocator;

	// Pointer fields ordered by width - unused ones at the end won't be allocated
	float *extremes32;
	double *extremes64;

	// Allocate float level
	inline float *AllocateFloatLevel() {
		float *buf = reinterpret_cast<float *>(allocator->Allocate(64 * sizeof(float)));
		float init = TypeInit<float>();
		for (int i = 0; i < 64; i++) {
			buf[i] = init;
		}
		return buf;
	}

	// Allocate double level
	inline double *AllocateDoubleLevel() {
		double *buf = reinterpret_cast<double *>(allocator->Allocate(64 * sizeof(double)));
		double init = TypeInit<double>();
		for (int i = 0; i < 64; i++) {
			buf[i] = init;
		}
		return buf;
	}

	// Upgrade from float to double
	inline double *UpgradeToDouble() {
		double *dst = reinterpret_cast<double *>(allocator->Allocate(64 * sizeof(double)));
		if (extremes32) {
			for (int i = 0; i < 64; i++) {
				dst[i] = static_cast<double>(extremes32[i]);
			}
		} else {
			double init = TypeInit<double>();
			for (int i = 0; i < 64; i++) {
				dst[i] = init;
			}
		}
		return dst;
	}

	void AllocateFirstLevel(ArenaAllocator &alloc) {
		allocator = &alloc;
#ifdef PAC_MINMAX_NONLAZY
		// Pre-allocate all levels that exist for this MAXWIDTH
		extremes32 = AllocateFloatLevel();
		if (MAXWIDTH >= 64) {
			extremes64 = AllocateDoubleLevel();
		}
		current_level = MAXWIDTH;
#else
		extremes32 = AllocateFloatLevel();
		current_level = 32;
#endif
		update_count = 0;
		global_bound = TypeInit<VALUE_TYPE>();
		initialized = true;
	}

	void UpgradeToDoublePrecision() {
		if (MAXWIDTH >= 64) {
			extremes64 = UpgradeToDouble();
			current_level = 64;
		}
	}
	// Alias for unified interface consistency with int state naming
	void UpgradeTo64() {
		UpgradeToDoublePrecision();
	}

	// Get value at index j, cast to type T, from current level (for unified interface)
	template <typename T>
	T GetValueAs(int j) const {
		if (MAXWIDTH >= 64 && current_level >= 64) {
			return static_cast<T>(extremes64[j]);
		}
		return static_cast<T>(extremes32[j]);
	}

	void GetTotalsAsDouble(double *dst) const {
		if (MAXWIDTH >= 64 && current_level == 64) {
			for (int i = 0; i < 64; i++) {
				dst[i] = extremes64[i];
			}
			return;
		}
		if (current_level == 32) {
			for (int i = 0; i < 64; i++) {
				dst[i] = static_cast<double>(extremes32[i]);
			}
			return;
		}
		// Not initialized - return init values
		double init = TypeInit<double>();
		for (int i = 0; i < 64; i++) {
			dst[i] = init;
		}
	}

	// Upgrade level if value doesn't fit in current level
	void MaybeUpgrade(VALUE_TYPE value) {
		if (MAXWIDTH >= 64 && !FitsInFloat(value) && current_level == 32) {
			UpgradeToDoublePrecision();
		}
	}

	// Update extremes at current level with value
	AUTOVECTORIZE void UpdateAtCurrentLevel(uint64_t key_hash, VALUE_TYPE value) {
		if (current_level == 32) {
			UpdateExtremes<float, IS_MAX>(extremes32, key_hash, static_cast<float>(value));
		}
		if (MAXWIDTH >= 64 && current_level == 64) {
			UpdateExtremes<double, IS_MAX>(extremes64, key_hash, static_cast<double>(value));
		}
	}

	// Recompute global bound from current level's extremes - periodically called after updates
	void RecomputeBound() {
#ifndef PAC_MINMAX_NOBOUNDOPT
		if (++update_count == BOUND_RECOMPUTE_INTERVAL) {
			update_count = 0;
			if (current_level == 32) {
				global_bound = static_cast<VALUE_TYPE>(ComputeGlobalBound<float, float, IS_MAX>(extremes32));
			}
			if (MAXWIDTH >= 64 && current_level == 64) {
				global_bound = ComputeGlobalBound<double, double, IS_MAX>(extremes64);
			}
		}
#endif
	}

	// Combine with another state (merge src into this)
	void CombineWith(const PacMinMaxFloatState &src, ArenaAllocator &alloc) {
		if (!src.initialized) {
			return;
		}
		if (!initialized) {
			allocator = &alloc;
			extremes32 = AllocateFloatLevel();
			current_level = 32;
			update_count = 0;
			global_bound = TypeInit<VALUE_TYPE>();
			initialized = true;
		}
		// Upgrade to double if src is at double level
		if (MAXWIDTH >= 64 && current_level == 32 && src.current_level == 64) {
			UpgradeToDoublePrecision();
		}
		// Combine at current level
		if (current_level == 32) {
			for (int j = 0; j < 64; j++) {
				extremes32[j] = PAC_IS_BETTER(src.extremes32[j], extremes32[j]) ? src.extremes32[j] : extremes32[j];
			}
		}
		if (MAXWIDTH >= 64 && current_level == 64) {
			for (int j = 0; j < 64; j++) {
				double sv = (src.current_level >= 64) ? src.extremes64[j] : static_cast<double>(src.extremes32[j]);
				extremes64[j] = PAC_BETTER(extremes64[j], sv);
			}
		}
		RecomputeBound();
	}
#endif

	// State size for DuckDB allocation - uses offsetof to exclude unused pointer fields
	static idx_t StateSize() {
#ifdef PAC_MINMAX_NONCASCADING
		return sizeof(PacMinMaxFloatState);
#else
		if (MAXWIDTH >= 64)
			return sizeof(PacMinMaxFloatState);
		return offsetof(PacMinMaxFloatState, extremes64);
#endif
	}
};

// Type alias for backwards compatibility
template <bool IS_MAX>
using PacMinMaxDoubleState = PacMinMaxFloatState<IS_MAX, double>;

} // namespace duckdb

#endif // PAC_MIN_MAX_HPP
