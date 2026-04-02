//
// pac_clip_min_max: Approximate min/max with per-level uint8_t extremes + distinct bitmaps
// Two-sided (unsigned pos/neg), NUM_LEVELS sized for input type width
//
#ifndef PAC_CLIP_MIN_MAX_HPP
#define PAC_CLIP_MIN_MAX_HPP

#include "duckdb.hpp"
#include "pac_clip_aggr.hpp"
#include "pac_min_max.hpp" // for UpdateExtremesSIMD

namespace duckdb {

void RegisterPacClipMinFunctions(ExtensionLoader &loader);
void RegisterPacClipMaxFunctions(ExtensionLoader &loader);
void RegisterPacNoisedClipMinFunctions(ExtensionLoader &loader);
void RegisterPacNoisedClipMaxFunctions(ExtensionLoader &loader);

// ============================================================================
// Min/max-specific constants (shared constants in pac_clip_aggr.hpp)
// ============================================================================
constexpr int PCMM_SWAR = 8;     // 8 × uint64_t = 64 × uint8_t extremes (SWAR packed)
constexpr int PCMM_ELEMENTS = 9; // 8 SWAR + 1 bitmap

// ============================================================================
// PacClipMinMaxIntState: core state with uint8_t extremes per level
// Unsigned: stores absolute values only. Caller routes negatives to a
// separate state with !IS_MAX (two-sided approach, same as clip_sum).
// NUM_LEVELS: 30 for ≤64-bit types, 62 for 128-bit types
// ============================================================================
template <bool IS_MAX, int NUM_LEVELS = CLIP_NUM_LEVELS_64>
struct PacClipMinMaxIntState {
	static constexpr int INLINE_THRESHOLD = NUM_LEVELS - PCMM_ELEMENTS;

	uint64_t key_hash;
	uint64_t update_count;
	int8_t max_level_used;            // -1 if none
	int8_t inline_level_idx;          // which level uses inline, -1 if none
	uint8_t level_bounds[NUM_LEVELS]; // BOUNDOPT: worst-of-64 per level for early skip

	// Level pointers with inline optimization: last PCMM_ELEMENTS slots
	// overlap with one inline level buffer, saving one arena allocation.
	union {
		uint64_t *levels[NUM_LEVELS];
		struct {
			uint64_t *_ptrs[INLINE_THRESHOLD];
			uint64_t inline_level[PCMM_ELEMENTS];
		};
	};

	// ========================================================================
	// GetLevel: route value to lowest level where shifted value fits in uint8_t [0,255]
	// ========================================================================
	static inline int GetLevel(uint64_t abs_val) {
		if (abs_val < 256) {
			return 0;
		}
		int bit_pos = 63 - pac_clzll(abs_val);
		return std::min((bit_pos - 4) >> 1, NUM_LEVELS - 1);
	}

	static inline int GetLevel128(uint64_t upper, uint64_t lower) {
		if (upper == 0) {
			return GetLevel(lower);
		}
		int bit_pos = 127 - pac_clzll(upper);
		return std::min((bit_pos - 4) >> 1, NUM_LEVELS - 1);
	}

	// ========================================================================
	// Level allocation
	// ========================================================================
	inline void AllocateLevel(ArenaAllocator &allocator, int k) {
		if (k >= INLINE_THRESHOLD && inline_level_idx >= 0) {
			// Evict inline level to arena
			auto *ext = reinterpret_cast<uint64_t *>(allocator.Allocate(PCMM_ELEMENTS * sizeof(uint64_t)));
			memcpy(ext, inline_level, PCMM_ELEMENTS * sizeof(uint64_t));
			levels[inline_level_idx] = ext;
			inline_level_idx = -1;
			memset(inline_level, 0, PCMM_ELEMENTS * sizeof(uint64_t));
		}
		uint64_t *buf;
		if (k < INLINE_THRESHOLD && inline_level_idx < 0) {
			buf = inline_level;
			inline_level_idx = static_cast<int8_t>(k);
		} else {
			buf = reinterpret_cast<uint64_t *>(allocator.Allocate(PCMM_ELEMENTS * sizeof(uint64_t)));
		}
		// Unsigned uint8_t: IS_MAX init to 0x00 (worst max=0), IS_MIN init to 0xFF (worst min=255)
		if (IS_MAX) {
			memset(buf, 0x00, PCMM_SWAR * sizeof(uint64_t));
		} else {
			memset(buf, 0xFF, PCMM_SWAR * sizeof(uint64_t));
		}
		buf[PCMM_SWAR] = 0; // bitmap starts empty
		levels[k] = buf;
		level_bounds[k] = IS_MAX ? 0 : UINT8_MAX; // init bound to worst case
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
		auto *extremes = reinterpret_cast<const uint8_t *>(levels[k]);
		uint8_t worst = extremes[0];
		for (int i = 1; i < 64; i++) {
			worst = PAC_WORSE(worst, extremes[i]);
		}
		level_bounds[k] = worst;
	}

	// ========================================================================
	// UpdateExtreme: reuse the SIMD kernel from pac_min_max.hpp
	// uint8_t: SHIFTS=8, MASK=0x0101..., SIGNED=false, FLOAT=false
	// ========================================================================
	inline void UpdateExtreme(uint64_t *buf, uint8_t shifted_val, uint64_t kh) {
		auto *extremes = reinterpret_cast<uint8_t *>(buf);
		UpdateExtremesSIMD<uint8_t, uint8_t, uint8_t, 8, 0x0101010101010101ULL, IS_MAX, false, false>(extremes, kh,
		                                                                                              shifted_val);
	}

	// ========================================================================
	// GetTotals: non-mutating finalization — reconstruct unsigned extremes
	// Uses same boundary logic as clip_sum (shared helpers in pac_clip_aggr.hpp)
	// ========================================================================
	void GetTotals(PAC_FLOAT *dst, int clip_support_threshold = 0, bool clip_scale = false) const {
		memset(dst, 0, 64 * sizeof(PAC_FLOAT));

		// Pass 1: find first and last supported levels
		int first_supported = -1, last_supported = -1;
		if (clip_support_threshold > 0) {
			ClipFindSupportedRange(levels, max_level_used, PCMM_SWAR, clip_support_threshold, first_supported,
			                       last_supported);
		}

		// Pass 2: accumulate extremes
		for (int k = 0; k <= max_level_used; k++) {
			if (!levels[k]) {
				continue;
			}

			int eff =
			    (clip_support_threshold > 0) ? ClipEffectiveLevel(k, first_supported, last_supported, clip_scale) : k;
			if (eff < 0) {
				continue;
			}

			PAC_FLOAT scale = std::exp2(static_cast<PAC_FLOAT>(CLIP_LEVEL_SHIFT * eff));
			auto *extremes = reinterpret_cast<const uint8_t *>(levels[k]);

			// Undo SWAR interleaving from UpdateExtremesSIMD (uint8_t: ELEMS=8, SHIFTS=8)
			for (int bit = 0; bit < 64; bit++) {
				int swar_pos = (bit % 8) * 8 + bit / 8;
				PAC_FLOAT reconstructed = static_cast<PAC_FLOAT>(extremes[swar_pos]) * scale;
				if (IS_MAX) {
					if (reconstructed > dst[bit]) {
						dst[bit] = reconstructed;
					}
				} else {
					if (reconstructed < dst[bit] || dst[bit] == 0) {
						dst[bit] = reconstructed;
					}
				}
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
			auto *dst_ext = reinterpret_cast<uint8_t *>(levels[k]);
			auto *src_ext = reinterpret_cast<const uint8_t *>(src->levels[k]);
			for (int j = 0; j < 64; j++) {
				if (IS_MAX) {
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
// PacClipMinMaxStateWrapper: buffering wrapper with two-sided pos/neg
// neg_state uses !IS_MAX (opposite direction on absolute values)
// NUM_LEVELS: 30 for ≤64-bit types, 62 for 128-bit types
// ============================================================================
template <bool IS_MAX, int NUM_LEVELS = CLIP_NUM_LEVELS_64>
struct PacClipMinMaxStateWrapper {
	using State = PacClipMinMaxIntState<IS_MAX, NUM_LEVELS>;
	using NegState = PacClipMinMaxIntState<!IS_MAX, NUM_LEVELS>;
	static constexpr int BUF_SIZE = 2;
	static constexpr uint64_t BUF_MASK = 3ULL;

	int64_t val_buf[BUF_SIZE];
	uint64_t hash_buf[BUF_SIZE];
	union {
		uint64_t n_buffered; // lower 2 bits: count, upper bits: state pointer
		State *state;
	};
	NegState *neg_state; // separate state for negatives (stores absolute values, opposite direction)

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

	NegState *GetNegState() const {
		return neg_state;
	}

	NegState *EnsureNegState(ArenaAllocator &a) {
		if (!neg_state) {
			neg_state = reinterpret_cast<NegState *>(a.Allocate(sizeof(NegState)));
			memset(neg_state, 0, sizeof(NegState));
			neg_state->max_level_used = -1;
			neg_state->inline_level_idx = -1;
		}
		return neg_state;
	}

	// For unsigned types, neg_state is never used — report smaller size
	template <bool SIGNED>
	static idx_t StateSize() {
		if (!SIGNED) {
			return sizeof(PacClipMinMaxStateWrapper) - sizeof(NegState *);
		}
		return sizeof(PacClipMinMaxStateWrapper);
	}
};

} // namespace duckdb

#endif // PAC_CLIP_MIN_MAX_HPP
