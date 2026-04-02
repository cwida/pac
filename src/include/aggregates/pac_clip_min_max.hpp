//
// pac_clip_min_max: Approximate min/max with per-level int8_t extremes + distinct bitmaps
// Single-sided (signed arithmetic shift preserves sign), 62 levels covering 128-bit
//
#ifndef PAC_CLIP_MIN_MAX_HPP
#define PAC_CLIP_MIN_MAX_HPP

#include "duckdb.hpp"
#include "pac_aggregate.hpp"
#include "pac_min_max.hpp" // for UpdateExtremesSIMD
#include <cmath>
#include <climits>

namespace duckdb {

void RegisterPacClipMinFunctions(ExtensionLoader &loader);
void RegisterPacClipMaxFunctions(ExtensionLoader &loader);
void RegisterPacNoisedClipMinFunctions(ExtensionLoader &loader);
void RegisterPacNoisedClipMaxFunctions(ExtensionLoader &loader);

// ============================================================================
// Constants (same level structure as pac_clip_sum)
// ============================================================================
constexpr int PCMM_NUM_LEVELS = 62;       // 62 levels × 2-bit bands covers full 128-bit
constexpr int PCMM_LEVEL_SHIFT = 2;       // 2^2 = 4x per level
constexpr int PCMM_SWAR = 8;              // 8 × uint64_t = 64 × int8_t extremes (SWAR packed)
constexpr int PCMM_ELEMENTS = 9;          // 8 SWAR + 1 bitmap
constexpr int PCMM_INLINE_THRESHOLD = 53; // levels 0-52 can use inline (53 pointers + 9 inline = 62)

// Float/double scale factors (same as pac_clip_sum)
constexpr int PCMM_FLOAT_SHIFT = 20;
constexpr int PCMM_DOUBLE_SHIFT = 27;
constexpr double PCMM_FLOAT_SCALE = static_cast<double>(1 << PCMM_FLOAT_SHIFT);
constexpr double PCMM_DOUBLE_SCALE = static_cast<double>(1 << PCMM_DOUBLE_SHIFT);

// ============================================================================
// PacClipMinMaxIntState: core state with int8_t extremes per level
// ============================================================================
template <bool IS_MAX>
struct PacClipMinMaxIntState {
	uint64_t key_hash;
	uint64_t update_count;
	int8_t max_level_used;                // -1 if none
	int8_t inline_level_idx;              // which level uses inline, -1 if none
	int8_t level_bounds[PCMM_NUM_LEVELS]; // BOUNDOPT: worst-of-64 per level for early skip

	// 62 level pointers = 496 bytes.
	// Inline optimization: last PCMM_ELEMENTS slots = 72 bytes = one level.
	union {
		uint64_t *levels[PCMM_NUM_LEVELS]; // 496 bytes
		struct {
			uint64_t *_ptrs[PCMM_INLINE_THRESHOLD]; // levels 0-52 pointers (424 bytes)
			uint64_t inline_level[PCMM_ELEMENTS];   // 72 bytes for one inline level
		};
	};

	// ========================================================================
	// GetLevel: route value to lowest level where shifted value fits in int8_t [-128,127]
	// Threshold 128 (not 256 like sum's uint8_t): abs_val < 128 → level 0
	// 2-bit bands, same structure as clip_sum but with 7-bit magnitude range
	// ========================================================================
	static inline int GetLevel(uint64_t abs_val) {
		if (abs_val < 128) {
			return 0;
		}
		int bit_pos = 63 - pac_clzll(abs_val);
		return std::min((bit_pos - 5) >> 1, PCMM_NUM_LEVELS - 1);
	}

	static inline int GetLevel128(uint64_t upper, uint64_t lower) {
		if (upper == 0) {
			return GetLevel(lower);
		}
		int bit_pos = 127 - pac_clzll(upper);
		return std::min((bit_pos - 5) >> 1, PCMM_NUM_LEVELS - 1);
	}

	// ========================================================================
	// Level allocation
	// ========================================================================
	inline void AllocateLevel(ArenaAllocator &allocator, int k) {
		if (k >= PCMM_INLINE_THRESHOLD && inline_level_idx >= 0) {
			// Evict inline level to arena
			auto *ext = reinterpret_cast<uint64_t *>(allocator.Allocate(PCMM_ELEMENTS * sizeof(uint64_t)));
			memcpy(ext, inline_level, PCMM_ELEMENTS * sizeof(uint64_t));
			levels[inline_level_idx] = ext;
			inline_level_idx = -1;
			memset(inline_level, 0, PCMM_ELEMENTS * sizeof(uint64_t));
		}
		uint64_t *buf;
		if (k < PCMM_INLINE_THRESHOLD && inline_level_idx < 0) {
			buf = inline_level;
			inline_level_idx = static_cast<int8_t>(k);
		} else {
			buf = reinterpret_cast<uint64_t *>(allocator.Allocate(PCMM_ELEMENTS * sizeof(uint64_t)));
		}
		// Plain int8_t: IS_MAX init to 0x80 (-128, worst max), IS_MIN init to 0x7F (+127, worst min)
		if constexpr (IS_MAX) {
			memset(buf, 0x80, PCMM_SWAR * sizeof(uint64_t));
		} else {
			memset(buf, 0x7F, PCMM_SWAR * sizeof(uint64_t));
		}
		buf[PCMM_SWAR] = 0; // bitmap starts empty
		levels[k] = buf;
		level_bounds[k] = IS_MAX ? INT8_MIN : INT8_MAX; // init bound to worst case
	}

	inline void EnsureLevelAllocated(ArenaAllocator &allocator, int k) {
		if (DUCKDB_LIKELY(k <= max_level_used)) {
			return;
		}
		for (int i = max_level_used + 1; i <= k; i++) {
			AllocateLevel(allocator, i);
		}
		max_level_used = static_cast<int8_t>(k);
	}

	// ========================================================================
	// BOUNDOPT: recompute worst-of-64 bound for level k
	// ========================================================================
	void RecomputeBound(int k) {
		auto *extremes = reinterpret_cast<const int8_t *>(levels[k]);
		int8_t worst = extremes[0];
		for (int i = 1; i < 64; i++) {
			worst = PAC_WORSE(worst, extremes[i]);
		}
		level_bounds[k] = worst;
	}

	// ========================================================================
	// EstimateDistinct: birthday-paradox formula from 64-bit bitmap
	// ========================================================================
	static inline int EstimateDistinct(uint64_t bitmap) {
		int k = pac_popcount64(bitmap);
		if (k >= 64) {
			return 256;
		}
		if (k == 0) {
			return 0;
		}
		return static_cast<int>(-64.0 * std::log(1.0 - k / 64.0));
	}

	// ========================================================================
	// UpdateExtreme: reuse the SIMD kernel from pac_min_max.hpp
	// int8_t: SHIFTS=8, MASK=0x0101..., SIGNED=true, FLOAT=false
	// ========================================================================
	inline void UpdateExtreme(uint64_t *buf, int8_t shifted_val, uint64_t kh) {
		auto *extremes = reinterpret_cast<int8_t *>(buf);
		UpdateExtremesSIMD<int8_t, uint8_t, int8_t, 8, 0x0101010101010101ULL, IS_MAX, false, true>(extremes, kh,
		                                                                                           shifted_val);
	}

	// ========================================================================
	// GetTotals: non-mutating finalization — compute min/max across supported levels
	// ========================================================================
	void GetTotals(PAC_FLOAT *dst, int clip_support_threshold = 0) const {
		// Initialize to worst-case: -INF for MAX, +INF for MIN
		for (int j = 0; j < 64; j++) {
			if constexpr (IS_MAX) {
				dst[j] = -std::numeric_limits<PAC_FLOAT>::infinity();
			} else {
				dst[j] = std::numeric_limits<PAC_FLOAT>::infinity();
			}
		}

		for (int k = 0; k <= max_level_used; k++) {
			if (!levels[k]) {
				continue;
			}

			// Check clipping support
			if (clip_support_threshold > 0 && EstimateDistinct(levels[k][PCMM_SWAR]) < clip_support_threshold) {
				continue; // skip unsupported levels
			}

			PAC_FLOAT scale = std::exp2(static_cast<PAC_FLOAT>(PCMM_LEVEL_SHIFT * k));
			auto *extremes = reinterpret_cast<const int8_t *>(levels[k]);

			// Undo SWAR interleaving from UpdateExtremesSIMD (int8_t: ELEMS=8, SHIFTS=8)
			for (int bit = 0; bit < 64; bit++) {
				int swar_pos = (bit % 8) * 8 + bit / 8;
				PAC_FLOAT reconstructed = static_cast<PAC_FLOAT>(extremes[swar_pos]) * scale;
				if constexpr (IS_MAX) {
					if (reconstructed > dst[bit]) {
						dst[bit] = reconstructed;
					}
				} else {
					if (reconstructed < dst[bit]) {
						dst[bit] = reconstructed;
					}
				}
			}
		}

		// Replace infinities with 0 for bits that had no supported contribution
		for (int j = 0; j < 64; j++) {
			if (std::isinf(dst[j])) {
				dst[j] = 0.0;
			}
		}
	}

	// ========================================================================
	// CombineFrom: merge another state into this one
	// ========================================================================
	void CombineFrom(PacClipMinMaxIntState *src, ArenaAllocator &allocator) {
		if (!src) {
			return;
		}
		key_hash |= src->key_hash;
		update_count += src->update_count;

		for (int k = 0; k <= src->max_level_used; k++) {
			if (!src->levels[k]) {
				continue;
			}

			if (k > max_level_used || !levels[k]) {
				EnsureLevelAllocated(allocator, k);
				// Steal or copy src level
				if (k != src->inline_level_idx) {
					levels[k] = src->levels[k];
					src->levels[k] = nullptr;
				} else {
					memcpy(levels[k], src->levels[k], PCMM_ELEMENTS * sizeof(uint64_t));
				}
				continue;
			}

			// Both have this level: merge extremes element-wise
			auto *dst_ext = reinterpret_cast<int8_t *>(levels[k]);
			auto *src_ext = reinterpret_cast<const int8_t *>(src->levels[k]);
			for (int j = 0; j < 64; j++) {
				if constexpr (IS_MAX) {
					if (src_ext[j] > dst_ext[j]) {
						dst_ext[j] = src_ext[j];
					}
				} else {
					if (src_ext[j] < dst_ext[j]) {
						dst_ext[j] = src_ext[j];
					}
				}
			}
			// OR bitmaps
			levels[k][PCMM_SWAR] |= src->levels[k][PCMM_SWAR];
		}
	}

	PacClipMinMaxIntState *GetState() {
		return this;
	}
	PacClipMinMaxIntState *EnsureState(ArenaAllocator &) {
		return this;
	}
};

// ============================================================================
// PacClipMinMaxStateWrapper: buffering wrapper (no two-sided for min/max)
// ============================================================================
template <bool IS_MAX>
struct PacClipMinMaxStateWrapper {
	using State = PacClipMinMaxIntState<IS_MAX>;
	static constexpr int BUF_SIZE = 2;
	static constexpr uint64_t BUF_MASK = 3ULL;

	int64_t val_buf[BUF_SIZE];
	uint64_t hash_buf[BUF_SIZE];
	union {
		uint64_t n_buffered; // lower 2 bits: count, upper bits: state pointer
		State *state;
	};

	State *GetState() const {
		return reinterpret_cast<State *>(reinterpret_cast<uintptr_t>(state) & ~7ULL);
	}

	State *EnsureState(ArenaAllocator &a) {
		State *s = GetState();
		if (!s) {
			s = reinterpret_cast<State *>(a.Allocate(sizeof(State)));
			memset(s, 0, sizeof(State));
			s->max_level_used = -1;
			s->inline_level_idx = -1;
			state = s;
		}
		return s;
	}

	static idx_t StateSize() {
		return sizeof(PacClipMinMaxStateWrapper);
	}
};

} // namespace duckdb

#endif // PAC_CLIP_MIN_MAX_HPP
