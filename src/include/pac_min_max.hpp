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
// SIMD-friendly update functions for extremes arrays (SWAR layout)
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
		T extreme = IS_MAX ? std::max(value, result[i]) : std::min(value, result[i]);
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
		if (((key_hash >> i) & 1ULL) && (IS_MAX ? value > result[i] : value < result[i])) {
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
	constexpr int NUM_U64 = 64 / ELEM_PER_U64;
	// The union packs bits such that linear index L maps to SWAR index:
	// EPU * (L % NUM_U64) + (L / NUM_U64)
	return ELEM_PER_U64 * (linear_idx % NUM_U64) + (linear_idx / NUM_U64);
}

// Extract from SWAR layout to linear double array
template <typename T, int ELEM_PER_U64>
static inline void ExtractSWAR(const T *swar_data, double *dst) {
	for (int i = 0; i < 64; i++) {
		int swar_idx = LinearToSWAR<ELEM_PER_U64>(i);
		dst[i] = static_cast<double>(swar_data[swar_idx]);
	}
}

// ============================================================================
// Helper to recompute global bound from extremes array
// ============================================================================
template <typename T, typename BOUND_T, bool IS_MAX>
AUTOVECTORIZE static inline BOUND_T ComputeGlobalBound(const T *extremes) {
	BOUND_T bound = static_cast<BOUND_T>(extremes[0]);
	for (int i = 1; i < 64; i++) {
		BOUND_T ext = static_cast<BOUND_T>(extremes[i]);
		if constexpr (IS_MAX) {
			bound = (ext < bound) ? ext : bound; // Worse = min for MAX
		} else {
			bound = (ext > bound) ? ext : bound; // Worse = max for MIN
		}
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
// Inheritance chain for conditional extremes pointers (cascading mode only)
// Uses Empty Base Optimization to have zero overhead for unused levels
// ============================================================================
#ifndef PAC_MINMAX_NONCASCADING

// Base level - always has T8
template <bool SIGNED, int MAXWIDTH>
struct IntExtremesLevel8 {
	using T8 = typename std::conditional<SIGNED, int8_t, uint8_t>::type;
	T8 *extremes8;
};

// Level 16 - empty by default (EBO makes this zero-size)
template <bool SIGNED, int MAXWIDTH, bool HAS_16 = (MAXWIDTH >= 16)>
struct IntExtremesLevel16 : IntExtremesLevel8<SIGNED, MAXWIDTH> {};

// Level 16 - specialization that adds the field when MAXWIDTH >= 16
template <bool SIGNED, int MAXWIDTH>
struct IntExtremesLevel16<SIGNED, MAXWIDTH, true> : IntExtremesLevel8<SIGNED, MAXWIDTH> {
	using T16 = typename std::conditional<SIGNED, int16_t, uint16_t>::type;
	T16 *extremes16;
};

// Level 32 - empty by default
template <bool SIGNED, int MAXWIDTH, bool HAS_32 = (MAXWIDTH >= 32)>
struct IntExtremesLevel32 : IntExtremesLevel16<SIGNED, MAXWIDTH> {};

// Level 32 - specialization that adds the field when MAXWIDTH >= 32
template <bool SIGNED, int MAXWIDTH>
struct IntExtremesLevel32<SIGNED, MAXWIDTH, true> : IntExtremesLevel16<SIGNED, MAXWIDTH> {
	using T32 = typename std::conditional<SIGNED, int32_t, uint32_t>::type;
	T32 *extremes32;
};

// Level 64 - empty by default
template <bool SIGNED, int MAXWIDTH, bool HAS_64 = (MAXWIDTH >= 64)>
struct IntExtremesLevel64 : IntExtremesLevel32<SIGNED, MAXWIDTH> {};

// Level 64 - specialization that adds the field when MAXWIDTH >= 64
template <bool SIGNED, int MAXWIDTH>
struct IntExtremesLevel64<SIGNED, MAXWIDTH, true> : IntExtremesLevel32<SIGNED, MAXWIDTH> {
	using T64 = typename std::conditional<SIGNED, int64_t, uint64_t>::type;
	T64 *extremes64;
};

// Level 128 - empty by default
template <bool SIGNED, int MAXWIDTH, bool HAS_128 = (MAXWIDTH >= 128)>
struct IntExtremesLevel128 : IntExtremesLevel64<SIGNED, MAXWIDTH> {};

// Level 128 - specialization that adds the field when MAXWIDTH >= 128
template <bool SIGNED, int MAXWIDTH>
struct IntExtremesLevel128<SIGNED, MAXWIDTH, true> : IntExtremesLevel64<SIGNED, MAXWIDTH> {
	using T128 = typename std::conditional<SIGNED, hugeint_t, uhugeint_t>::type;
	T128 *extremes128;
};

// ============================================================================
// Float inheritance chain for conditional extremes pointers
// ============================================================================

// Base level - always has float (level 32)
template <int MAXWIDTH>
struct FloatExtremesLevel32 {
	float *extremesF;
};

// Level 64 - empty by default (EBO makes this zero-size)
template <int MAXWIDTH, bool HAS_64 = (MAXWIDTH >= 64)>
struct FloatExtremesLevel64 : FloatExtremesLevel32<MAXWIDTH> {};

// Level 64 - specialization that adds the field when MAXWIDTH >= 64
template <int MAXWIDTH>
struct FloatExtremesLevel64<MAXWIDTH, true> : FloatExtremesLevel32<MAXWIDTH> {
	double *extremesD;
};

#endif // PAC_MINMAX_NONCASCADING

// Templated integer state for min/max
// SIGNED: signed vs unsigned types
// IS_MAX: true for pac_max, false for pac_min
// MAXWIDTH: maximum bit width needed (8, 16, 32, 64, or 128) - controls which levels are present
template <bool SIGNED, bool IS_MAX, int MAXWIDTH = 64>
struct PacMinMaxIntState
#ifndef PAC_MINMAX_NONCASCADING
    : IntExtremesLevel128<SIGNED, MAXWIDTH> // Inherit extremes pointers (only those needed for MAXWIDTH)
#endif
{
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

	// Get init value (worst possible for the aggregation direction) based on type size
	template <typename T>
	static inline T TypeInit() {
		if constexpr (sizeof(T) == 1) {
			return IS_MAX ? (SIGNED ? INT8_MIN : 0) : (SIGNED ? INT8_MAX : UINT8_MAX);
		} else if constexpr (sizeof(T) == 2) {
			return IS_MAX ? (SIGNED ? INT16_MIN : 0) : (SIGNED ? INT16_MAX : UINT16_MAX);
		} else if constexpr (sizeof(T) == 4) {
			return IS_MAX ? (SIGNED ? INT32_MIN : 0) : (SIGNED ? INT32_MAX : UINT32_MAX);
		} else if constexpr (sizeof(T) == 8) {
			return IS_MAX ? (SIGNED ? INT64_MIN : 0) : (SIGNED ? INT64_MAX : UINT64_MAX);
		} else {
			return IS_MAX ? NumericLimits<T>::Minimum() : NumericLimits<T>::Maximum();
		}
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
	TMAX global_bound; // For MAX: min of all maxes; for MIN: max of all mins
	uint16_t update_count;
	bool seen_null;
	bool initialized;

#ifdef PAC_MINMAX_NONCASCADING
	// ========== Non-cascading mode: single fixed-width array ==========
	TMAX extremes[64];

	void GetTotalsAsDouble(double *dst) const {
		for (int i = 0; i < 64; i++) {
			if constexpr (sizeof(TMAX) == 16) {
				if constexpr (SIGNED) {
					dst[i] = Hugeint::Cast<double>(extremes[i]);
				} else {
					dst[i] = Uhugeint::Cast<double>(extremes[i]);
				}
			} else {
				dst[i] = static_cast<double>(extremes[i]);
			}
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
#else
	// ========== Cascading mode fields ==========
	// extremes8/16/32/64/128 pointers are inherited from IntExtremesLevel128 chain
	// (only those needed for MAXWIDTH are actually present due to EBO)
	uint8_t current_level; // 8, 16, 32, 64, or 128
	ArenaAllocator *allocator;

	// Allocate a level's buffer
	template <typename T>
	inline T *AllocateLevel(T init_value) {
		T *buf = reinterpret_cast<T *>(allocator->Allocate(64 * sizeof(T)));
		for (int i = 0; i < 64; i++) {
			buf[i] = init_value;
		}
		return buf;
	}

	// Upgrade from one level to the next, automatically handling SWAR vs linear layout
	// Types <= 4 bytes use SWAR layout, types > 4 bytes use linear layout
	// If src value equals src_init (never updated), use dst_init instead
	template <typename SRC_T, typename DST_T>
	inline DST_T *UpgradeLevel(SRC_T *src, SRC_T src_init, DST_T dst_init) {
		DST_T *dst = reinterpret_cast<DST_T *>(allocator->Allocate(64 * sizeof(DST_T)));
		if (src) {
			for (int i = 0; i < 64; i++) {
				int src_idx, dst_idx;
				if constexpr (sizeof(SRC_T) <= 4) {
					src_idx = LinearToSWAR<8 / sizeof(SRC_T)>(i);
				} else {
					src_idx = i;
				}
				if constexpr (sizeof(DST_T) <= 4) {
					dst_idx = LinearToSWAR<8 / sizeof(DST_T)>(i);
				} else {
					dst_idx = i;
				}
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
		this->extremes8 = AllocateLevel<T8>(TypeInit<T8>());
		if constexpr (MAXWIDTH >= 16) {
			this->extremes16 = AllocateLevel<T16>(TypeInit<T16>());
		}
		if constexpr (MAXWIDTH >= 32) {
			this->extremes32 = AllocateLevel<T32>(TypeInit<T32>());
		}
		if constexpr (MAXWIDTH >= 64) {
			this->extremes64 = AllocateLevel<T64>(TypeInit<T64>());
		}
		if constexpr (MAXWIDTH >= 128) {
			this->extremes128 = AllocateLevel<T128>(TypeInit<T128>());
		}
		current_level = MAXWIDTH;
#else
		this->extremes8 = AllocateLevel<T8>(TypeInit<T8>());
		current_level = 8;
#endif
		update_count = 0;
		global_bound = TypeInit<TMAX>();
		initialized = true;
	}

	void UpgradeTo16() {
		if constexpr (MAXWIDTH >= 16) {
			this->extremes16 = UpgradeLevel<T8, T16>(this->extremes8, TypeInit<T8>(), TypeInit<T16>());
			current_level = 16;
		}
	}

	void UpgradeTo32() {
		if constexpr (MAXWIDTH >= 32) {
			this->extremes32 = UpgradeLevel<T16, T32>(this->extremes16, TypeInit<T16>(), TypeInit<T32>());
			current_level = 32;
		}
	}

	void UpgradeTo64() {
		if constexpr (MAXWIDTH >= 64) {
			this->extremes64 = UpgradeLevel<T32, T64>(this->extremes32, TypeInit<T32>(), TypeInit<T64>());
			current_level = 64;
		}
	}

	void UpgradeTo128() {
		if constexpr (MAXWIDTH >= 128) {
			this->extremes128 = UpgradeLevel<T64, T128>(this->extremes64, TypeInit<T64>(), TypeInit<T128>());
			current_level = 128;
		}
	}

	// Get value at index j, cast to type T, from whatever level is current
	template <typename T>
	T GetValueAs(int j) const {
		if constexpr (MAXWIDTH >= 128) {
			if (current_level >= 128)
				return static_cast<T>(this->extremes128[j]);
		}
		if constexpr (MAXWIDTH >= 64) {
			if (current_level >= 64)
				return static_cast<T>(this->extremes64[j]);
		}
		if constexpr (MAXWIDTH >= 32) {
			if (current_level >= 32)
				return static_cast<T>(this->extremes32[j]);
		}
		if constexpr (MAXWIDTH >= 16) {
			if (current_level >= 16)
				return static_cast<T>(this->extremes16[j]);
		}
		return static_cast<T>(this->extremes8[j]);
	}

	void GetTotalsAsDouble(double *dst) const {
		// Use if-else chain with if constexpr to avoid referencing non-existent fields
		if constexpr (MAXWIDTH >= 128) {
			if (current_level == 128) {
				for (int i = 0; i < 64; i++) {
					if constexpr (SIGNED) {
						dst[i] = Hugeint::Cast<double>(this->extremes128[i]);
					} else {
						dst[i] = Uhugeint::Cast<double>(this->extremes128[i]);
					}
				}
				return;
			}
		}
		if constexpr (MAXWIDTH >= 64) {
			if (current_level == 64) {
				for (int i = 0; i < 64; i++) {
					dst[i] = static_cast<double>(this->extremes64[i]);
				}
				return;
			}
		}
		if constexpr (MAXWIDTH >= 32) {
			if (current_level == 32) {
				ExtractSWAR<T32, 2>(this->extremes32, dst);
				return;
			}
		}
		if constexpr (MAXWIDTH >= 16) {
			if (current_level == 16) {
				ExtractSWAR<T16, 4>(this->extremes16, dst);
				return;
			}
		}
		if (current_level == 8) {
			ExtractSWAR<T8, 8>(this->extremes8, dst);
			return;
		}
		// Not initialized - return init values
		for (int i = 0; i < 64; i++) {
			if constexpr (MAXWIDTH >= 128) {
				if constexpr (SIGNED) {
					dst[i] = Hugeint::Cast<double>(TypeInit<T128>());
				} else {
					dst[i] = Uhugeint::Cast<double>(TypeInit<T128>());
				}
			} else {
				dst[i] = static_cast<double>(TypeInit<TMAX>());
			}
		}
	}

#endif
};

// Float state for min/max (floating point values)
// VALUE_TYPE: float or double (determines the return type)
// MAXWIDTH: 32 for float-only, 64 for float+double cascading
// Cascading: start with float if values fit in [-1000000, 1000000], upgrade to double otherwise
template <bool IS_MAX, typename VALUE_TYPE = double, int MAXWIDTH = 64>
struct PacMinMaxFloatState
#ifndef PAC_MINMAX_NONCASCADING
    : FloatExtremesLevel64<MAXWIDTH> // Inherit extremes pointers (only those needed for MAXWIDTH)
#endif
{
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

	// ========== Common fields (defined once for both modes) ==========
	VALUE_TYPE global_bound;
	uint16_t update_count;
	bool seen_null;
	bool initialized;

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
#else
	// ========== Cascading mode fields ==========
	// extremesF/extremesD pointers are inherited from FloatExtremesLevel64 chain
	// (only those needed for MAXWIDTH are actually present due to EBO)
	uint8_t current_level; // 32 for float, 64 for double
	ArenaAllocator *allocator;

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
		if (this->extremesF) {
			for (int i = 0; i < 64; i++) {
				dst[i] = static_cast<double>(this->extremesF[i]);
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
		this->extremesF = AllocateFloatLevel();
		if constexpr (MAXWIDTH >= 64) {
			this->extremesD = AllocateDoubleLevel();
		}
		current_level = MAXWIDTH;
#else
		this->extremesF = AllocateFloatLevel();
		current_level = 32;
#endif
		update_count = 0;
		global_bound = TypeInit<VALUE_TYPE>();
		initialized = true;
	}

	void UpgradeToDoublePrecision() {
		if constexpr (MAXWIDTH >= 64) {
			this->extremesD = UpgradeToDouble();
			current_level = 64;
		}
	}

	void GetTotalsAsDouble(double *dst) const {
		// Use if-else chain with if constexpr to avoid referencing non-existent fields
		if constexpr (MAXWIDTH >= 64) {
			if (current_level == 64) {
				for (int i = 0; i < 64; i++) {
					dst[i] = this->extremesD[i];
				}
				return;
			}
		}
		if (current_level == 32) {
			for (int i = 0; i < 64; i++) {
				dst[i] = static_cast<double>(this->extremesF[i]);
			}
			return;
		}
		// Not initialized - return init values
		double init = TypeInit<double>();
		for (int i = 0; i < 64; i++) {
			dst[i] = init;
		}
	}
#endif
};

// Type alias for backwards compatibility
template <bool IS_MAX>
using PacMinMaxDoubleState = PacMinMaxFloatState<IS_MAX, double>;

} // namespace duckdb

#endif // PAC_MIN_MAX_HPP
