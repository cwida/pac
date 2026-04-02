//
// pac_clip_sum: Approximate sum with per-level overflow + distinct bitmaps
// Always: buffered, approximate, two-sided (unsigned pos/neg), 31 levels covering 128-bit
//
#ifndef PAC_CLIP_SUM_HPP
#define PAC_CLIP_SUM_HPP

#include "duckdb.hpp"
#include "pac_clip_aggr.hpp"
#include <cmath>

namespace duckdb {

void RegisterPacClipSumFunctions(ExtensionLoader &loader);
void RegisterPacNoisedClipSumFunctions(ExtensionLoader &loader);
void RegisterPacNoisedClipSumCountFunctions(ExtensionLoader &loader);

// ============================================================================
// Sum-specific constants (shared constants in pac_clip_aggr.hpp)
// ============================================================================
constexpr int CLIP_NORMAL_SWAR = 16;       // 16 x uint64_t = 64 x uint16_t SWAR counters
constexpr int CLIP_NORMAL_ELEMENTS = 18;   // 16 SWAR + 1 packed ptr/ec + 1 bitmap
constexpr int CLIP_OVERFLOW_SWAR = 32;     // 32 x uint64_t = 64 x uint32_t SWAR counters
constexpr int CLIP_OVERFLOW_ELEMENTS = 33; // 32 SWAR + 1 exact_count
constexpr uint64_t CLIP_SWAR_MASK_16 = 0x0001000100010001ULL;

// ============================================================================
// Packed pointer + exact_count helpers
// Normal level[16] stores: upper 16 bits = exact_count, lower 48 bits = overflow pointer
// ============================================================================
static inline uint64_t *Pac2GetOverflowPtr(uint64_t packed) {
	return reinterpret_cast<uint64_t *>(packed & 0x0000FFFFFFFFFFFFULL);
}
static inline uint16_t Pac2GetExactCount(uint64_t packed) {
	return static_cast<uint16_t>(packed >> 48);
}
static inline void Pac2SetExactCount(uint64_t &packed, uint16_t count) {
	packed = (packed & 0x0000FFFFFFFFFFFFULL) | (static_cast<uint64_t>(count) << 48);
}
static inline void Pac2SetOverflowPtr(uint64_t &packed, uint64_t *ptr) {
	packed = (packed & 0xFFFF000000000000ULL) | (reinterpret_cast<uint64_t>(ptr) & 0x0000FFFFFFFFFFFFULL);
}

// ============================================================================
// SWAR kernel — identical to pac_sum's AddToTotalsSWAR for uint16_t
// ============================================================================
AUTOVECTORIZE static inline void Pac2AddToTotalsSWAR16(uint64_t *PAC_RESTRICT total, uint64_t value,
                                                       uint64_t key_hash) {
	uint64_t val_packed = static_cast<uint16_t>(value) * CLIP_SWAR_MASK_16;
	for (int i = 0; i < 16; i++) {
		uint64_t bits = (key_hash >> i) & CLIP_SWAR_MASK_16;
		uint64_t expanded = (bits << 16) - bits;
		total[i] += val_packed & expanded;
	}
}

// ============================================================================
// PacClipSumIntState — core state for one unsigned accumulator
// NUM_LEVELS: 30 for ≤64-bit types, 62 for 128-bit types
// ============================================================================
template <int NUM_LEVELS = CLIP_NUM_LEVELS_64>
struct PacClipSumIntState {
	static constexpr int INLINE_THRESHOLD = NUM_LEVELS - CLIP_NORMAL_ELEMENTS;

	uint64_t key_hash;
	uint64_t update_count;
	int8_t max_level_used;   // -1 if none
	int8_t inline_level_idx; // which level uses inline, -1 if none

	// Level pointers with inline optimization: last CLIP_NORMAL_ELEMENTS slots
	// overlap with one inline level buffer, saving one arena allocation.
	union {
		uint64_t *levels[NUM_LEVELS];
		struct {
			uint64_t *_ptrs[INLINE_THRESHOLD];
			uint64_t inline_level[CLIP_NORMAL_ELEMENTS];
		};
	};

	// ========================================================================
	// GetLevel: route value to lowest level where shifted value fits in 8 bits
	// ========================================================================
	static inline int GetLevel(uint64_t abs_val) {
		if (abs_val < 256) {
			return 0;
		}
		int bit_pos = 63 - pac_clzll(abs_val);
		return std::min((bit_pos - 4) >> 1, NUM_LEVELS - 1);
	}

	// For 128-bit (hugeint) values — clamps to max level for very large values
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
			auto *ext = reinterpret_cast<uint64_t *>(allocator.Allocate(CLIP_NORMAL_ELEMENTS * sizeof(uint64_t)));
			memcpy(ext, inline_level, CLIP_NORMAL_ELEMENTS * sizeof(uint64_t));
			levels[inline_level_idx] = ext;
			inline_level_idx = -1;
			memset(inline_level, 0, CLIP_NORMAL_ELEMENTS * sizeof(uint64_t));
		}
		if (k < INLINE_THRESHOLD && inline_level_idx < 0) {
			// Use inline storage
			levels[k] = inline_level;
			memset(inline_level, 0, CLIP_NORMAL_ELEMENTS * sizeof(uint64_t));
			inline_level_idx = static_cast<int8_t>(k);
		} else {
			auto *buf = reinterpret_cast<uint64_t *>(allocator.Allocate(CLIP_NORMAL_ELEMENTS * sizeof(uint64_t)));
			memset(buf, 0, CLIP_NORMAL_ELEMENTS * sizeof(uint64_t));
			levels[k] = buf;
		}
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
	// CascadeTop4: extract top 4 bits of 16-bit SWAR → add to 32-bit overflow
	// ========================================================================
	void CascadeTop4(uint64_t *normal_buf, ArenaAllocator &allocator) {
		// 1. Ensure overflow level allocated
		uint64_t *overflow = Pac2GetOverflowPtr(normal_buf[16]);
		if (!overflow) {
			overflow = reinterpret_cast<uint64_t *>(allocator.Allocate(CLIP_OVERFLOW_ELEMENTS * sizeof(uint64_t)));
			memset(overflow, 0, CLIP_OVERFLOW_ELEMENTS * sizeof(uint64_t));
			Pac2SetOverflowPtr(normal_buf[16], overflow);
		}

		// 2. Extract top 4 bits of each 16-bit counter → add to 32-bit overflow
		// SWAR 16-bit element i holds bit positions: i, i+16, i+32, i+48
		// SWAR 32-bit element i holds: i, i+32;  element i+16 holds: i+16, i+48
		for (int i = 0; i < 16; i++) {
			uint64_t swar = normal_buf[i];
			uint64_t top4 = (swar >> 12) & 0x000F000F000F000FULL;
			normal_buf[i] = swar & 0x0FFF0FFF0FFF0FFFULL;

			auto *t = reinterpret_cast<uint16_t *>(&top4);
			auto *o1 = reinterpret_cast<uint32_t *>(&overflow[i]);      // bits i, i+32
			auto *o2 = reinterpret_cast<uint32_t *>(&overflow[i + 16]); // bits i+16, i+48
			o1[0] += t[0];                                              // bit i
			o1[1] += t[2];                                              // bit i+32
			o2[0] += t[1];                                              // bit i+16
			o2[1] += t[3];                                              // bit i+48
		}

		// 3. Cascade exact_count top 4 bits
		uint16_t ec = Pac2GetExactCount(normal_buf[16]);
		auto *overflow_ec = reinterpret_cast<uint32_t *>(&overflow[32]);
		*overflow_ec += (ec >> 12);
		Pac2SetExactCount(normal_buf[16], ec & 0x0FFF);
	}

	// ========================================================================
	// AddValue: overflow-aware exact_count update
	// ========================================================================
	inline void AddToExactCount(uint64_t *normal_buf, uint16_t shifted_val, ArenaAllocator &allocator) {
		uint16_t ec = Pac2GetExactCount(normal_buf[16]);
		uint32_t new_ec = static_cast<uint32_t>(ec) + shifted_val;
		if (DUCKDB_UNLIKELY(new_ec > 0xFFFF)) {
			CascadeTop4(normal_buf, allocator);
			ec = Pac2GetExactCount(normal_buf[16]); // now ≤ 0x0FFF
			new_ec = static_cast<uint32_t>(ec) + shifted_val;
		}
		Pac2SetExactCount(normal_buf[16], static_cast<uint16_t>(new_ec));
	}

	// ========================================================================
	// GetTotals: non-mutating finalization — sums all levels
	// clip_support_threshold: levels with fewer estimated distinct PUs are clipped
	// clip_scale: false=omit unsupported prefix/suffix, true=scale to nearest supported
	// Interior unsupported levels (between first and last supported) always contribute.
	// ========================================================================
	void GetTotals(PAC_FLOAT *dst, int clip_support_threshold = 0, bool clip_scale = false) const {
		memset(dst, 0, 64 * sizeof(PAC_FLOAT));

		// Pass 1: find first and last supported levels
		int first_supported = -1, last_supported = -1;
		if (clip_support_threshold > 0) {
			ClipFindSupportedRange(levels, max_level_used, 17, clip_support_threshold, first_supported, last_supported);
		}

		// Pass 2: accumulate contributions
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

			// Add normal 16-bit SWAR contribution
			auto *counters = reinterpret_cast<const uint16_t *>(levels[k]);
			for (int j = 0; j < 64; j++) {
				int swar_idx = (j % 16) * 4 + (j / 16);
				dst[j] += static_cast<PAC_FLOAT>(counters[swar_idx]) * scale;
			}

			// Add overflow 32-bit SWAR contribution (scaled by 2^12 relative to normal)
			uint64_t *overflow = Pac2GetOverflowPtr(levels[k][16]);
			if (overflow) {
				PAC_FLOAT overflow_scale = scale * std::exp2(static_cast<PAC_FLOAT>(12));
				auto *ocounters = reinterpret_cast<const uint32_t *>(overflow);
				for (int j = 0; j < 64; j++) {
					int swar_idx = (j % 32) * 2 + (j / 32);
					dst[j] += static_cast<PAC_FLOAT>(ocounters[swar_idx]) * overflow_scale;
				}
			}
		}
	}

	// ========================================================================
	// CombineFrom: merge another state into this one
	// ========================================================================
	void CombineFrom(PacClipSumIntState *src, ArenaAllocator &allocator) {
		if (!src) {
			return;
		}
		key_hash |= src->key_hash;
		update_count += src->update_count;

		for (int k = 0; k <= src->max_level_used; k++) {
			if (!src->levels[k]) {
				continue;
			}

			// If dst doesn't have this level: steal src's pointer
			if (k > max_level_used || !levels[k]) {
				EnsureLevelAllocated(allocator, k); // ensures max_level_used >= k, allocates if needed
				// If we just allocated a fresh level, steal src's data over it
				if (k != src->inline_level_idx) {
					// src level is arena-allocated, can steal
					levels[k] = src->levels[k];
					src->levels[k] = nullptr;
				} else {
					// src is using inline — copy instead
					memcpy(levels[k], src->levels[k], CLIP_NORMAL_ELEMENTS * sizeof(uint64_t));
				}
				continue;
			}

			// Both have this level: merge
			// Add SWAR counters
			for (int i = 0; i < CLIP_NORMAL_SWAR; i++) {
				levels[k][i] += src->levels[k][i];
			}
			// OR bitmaps
			levels[k][17] |= src->levels[k][17];

			// Merge exact_counts (check overflow)
			uint16_t dst_ec = Pac2GetExactCount(levels[k][16]);
			uint16_t src_ec = Pac2GetExactCount(src->levels[k][16]);
			uint32_t sum_ec = static_cast<uint32_t>(dst_ec) + src_ec;
			if (sum_ec > 0xFFFF) {
				CascadeTop4(levels[k], allocator);
			}
			dst_ec = Pac2GetExactCount(levels[k][16]);
			Pac2SetExactCount(levels[k][16], dst_ec + src_ec);

			// Merge overflow levels
			uint64_t *src_overflow = Pac2GetOverflowPtr(src->levels[k][16]);
			uint64_t *dst_overflow = Pac2GetOverflowPtr(levels[k][16]);
			if (src_overflow && !dst_overflow) {
				// Steal overflow from src
				Pac2SetOverflowPtr(levels[k][16], src_overflow);
				Pac2SetOverflowPtr(src->levels[k][16], nullptr);
			} else if (src_overflow && dst_overflow) {
				// Merge overflow SWAR counters
				for (int i = 0; i < CLIP_OVERFLOW_SWAR; i++) {
					dst_overflow[i] += src_overflow[i];
				}
				// Merge overflow exact_counts
				auto *dec = reinterpret_cast<uint32_t *>(&dst_overflow[32]);
				auto *sec = reinterpret_cast<uint32_t *>(&src_overflow[32]);
				*dec += *sec;
			}
		}
	}

	// Interface methods
	PacClipSumIntState<NUM_LEVELS> *GetState() {
		return this;
	}
	PacClipSumIntState<NUM_LEVELS> *EnsureState(ArenaAllocator &) {
		return this;
	}
};

// ============================================================================
// PacClipSumStateWrapper: buffering wrapper with two-sided pos/neg
// NUM_LEVELS: 30 for ≤64-bit types, 62 for 128-bit types
// ============================================================================
template <int NUM_LEVELS = CLIP_NUM_LEVELS_64>
struct PacClipSumStateWrapper {
	using State = PacClipSumIntState<NUM_LEVELS>;
	using Value = uint64_t;
	static constexpr int BUF_SIZE = 2;
	static constexpr uint64_t BUF_MASK = 3ULL;

	uint64_t val_buf[BUF_SIZE];
	uint64_t hash_buf[BUF_SIZE];
	union {
		uint64_t n_buffered; // lower 2 bits: count, upper bits: state pointer
		State *state;
	};
	State *neg_state; // separate state for negatives (stores absolute values)

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

	State *GetNegState() const {
		return neg_state;
	}

	State *EnsureNegState(ArenaAllocator &a) {
		if (!neg_state) {
			neg_state = reinterpret_cast<State *>(a.Allocate(sizeof(State)));
			memset(neg_state, 0, sizeof(State));
			neg_state->max_level_used = -1;
			neg_state->inline_level_idx = -1;
		}
		return neg_state;
	}

	// For unsigned types, neg_state is never used — report smaller size
	template <bool SIGNED>
	static idx_t StateSize() {
		if (!SIGNED) {
			return sizeof(PacClipSumStateWrapper) - sizeof(State *);
		}
		return sizeof(PacClipSumStateWrapper);
	}
};

} // namespace duckdb

#endif // PAC_CLIP_SUM_HPP
