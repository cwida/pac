//
// pac_clip_sum: Approximate sum with per-level overflow + distinct bitmaps
// Always: buffered, approximate, two-sided (unsigned pos/neg), 31 levels covering 128-bit
//
#ifndef PAC_CLIP_SUM_HPP
#define PAC_CLIP_SUM_HPP

#include "duckdb.hpp"
#include "pac_aggregate.hpp"
#include <cmath>

namespace duckdb {

void RegisterPacClipSumFunctions(ExtensionLoader &loader);
void RegisterPacNoisedClipSumFunctions(ExtensionLoader &loader);
void RegisterPacNoisedClipSumCountFunctions(ExtensionLoader &loader);

// ============================================================================
// Constants
// ============================================================================
constexpr int PAC2_NUM_LEVELS = 31;
constexpr int PAC2_NORMAL_SWAR = 16;       // 16 x uint64_t = 64 x uint16_t SWAR counters
constexpr int PAC2_NORMAL_ELEMENTS = 18;   // 16 SWAR + 1 packed ptr/ec + 1 bitmap
constexpr int PAC2_OVERFLOW_SWAR = 32;     // 32 x uint64_t = 64 x uint32_t SWAR counters
constexpr int PAC2_OVERFLOW_ELEMENTS = 33; // 32 SWAR + 1 exact_count
constexpr int PAC2_LEVEL_SHIFT = 4;
constexpr uint64_t PAC2_SWAR_MASK_16 = 0x0001000100010001ULL;

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
	uint64_t val_packed = static_cast<uint16_t>(value) * PAC2_SWAR_MASK_16;
	for (int i = 0; i < 16; i++) {
		uint64_t bits = (key_hash >> i) & PAC2_SWAR_MASK_16;
		uint64_t expanded = (bits << 16) - bits;
		total[i] += val_packed & expanded;
	}
}

// ============================================================================
// PacClipSumIntState — core state for one unsigned accumulator
// ============================================================================
struct PacClipSumIntState {
	uint64_t key_hash;
	uint64_t update_count;
	int8_t max_level_used;   // -1 if none
	int8_t inline_level_idx; // which level uses inline, -1 if none

	// 31 level pointers = 248 bytes.
	// Inline optimization: last 18 slots (indices 13..30) = 144 bytes = one normal level.
	// Levels 0-12 can use inline storage without overlapping their own pointer slot.
	union {
		uint64_t *levels[PAC2_NUM_LEVELS]; // 248 bytes
		struct {
			uint64_t *_ptrs[13];                         // levels 0-12 pointers (104 bytes)
			uint64_t inline_level[PAC2_NORMAL_ELEMENTS]; // 144 bytes for one inline level
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
		return (bit_pos - 4) >> 2;
	}

	// For 128-bit (hugeint) values
	static inline int GetLevel128(uint64_t upper, uint64_t lower) {
		if (upper == 0) {
			return GetLevel(lower);
		}
		int bit_pos = 127 - pac_clzll(upper);
		return (bit_pos - 4) >> 2;
	}

	// ========================================================================
	// Level allocation
	// ========================================================================
	inline void AllocateLevel(ArenaAllocator &allocator, int k) {
		if (k >= 13 && inline_level_idx >= 0) {
			// Evict inline level to arena
			auto *ext = reinterpret_cast<uint64_t *>(allocator.Allocate(PAC2_NORMAL_ELEMENTS * sizeof(uint64_t)));
			memcpy(ext, inline_level, PAC2_NORMAL_ELEMENTS * sizeof(uint64_t));
			levels[inline_level_idx] = ext;
			inline_level_idx = -1;
			// Clear inline area so levels[13..30] read as nullptr
			memset(inline_level, 0, PAC2_NORMAL_ELEMENTS * sizeof(uint64_t));
		}
		if (k < 13 && inline_level_idx < 0) {
			// Use inline storage
			levels[k] = inline_level;
			memset(inline_level, 0, PAC2_NORMAL_ELEMENTS * sizeof(uint64_t));
			inline_level_idx = static_cast<int8_t>(k);
		} else {
			auto *buf = reinterpret_cast<uint64_t *>(allocator.Allocate(PAC2_NORMAL_ELEMENTS * sizeof(uint64_t)));
			memset(buf, 0, PAC2_NORMAL_ELEMENTS * sizeof(uint64_t));
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
			overflow = reinterpret_cast<uint64_t *>(allocator.Allocate(PAC2_OVERFLOW_ELEMENTS * sizeof(uint64_t)));
			memset(overflow, 0, PAC2_OVERFLOW_ELEMENTS * sizeof(uint64_t));
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
	// Estimate distinct count from 64-bit bitmap using birthday-paradox formula
	// ========================================================================
	static inline int EstimateDistinct(uint64_t bitmap) {
		int k = pac_popcount64(bitmap);
		if (k >= 64) {
			return 256; // saturated — could be any large number
		}
		if (k == 0) {
			return 0;
		}
		// n ≈ -64 * ln(1 - k/64)
		return static_cast<int>(-64.0 * std::log(1.0 - k / 64.0));
	}

	// ========================================================================
	// GetTotals: non-mutating finalization — sums all levels
	// clip_support_threshold: soft clamping of under-supported levels (0 = no clipping)
	//
	// Levels with fewer estimated distinct contributors than the threshold are
	// attenuated rather than zeroed:
	//   - Prefix (below first supported level): scaled UP by 16^distance to
	//     clamp small under-supported values toward the supported range.
	//   - Suffix (above last supported level): scaled DOWN by 16^distance to
	//     attenuate outlier levels toward the supported range.
	//   - Interior unsupported levels (between first and last supported): full
	//     contribution, no attenuation.
	// ========================================================================
	void GetTotals(PAC_FLOAT *dst, int clip_support_threshold = 0) const {
		memset(dst, 0, 64 * sizeof(PAC_FLOAT));

		// Pass 1: find first and last supported levels
		int first_supported = -1;
		int last_supported = -1;
		if (clip_support_threshold > 0) {
			for (int k = 0; k <= max_level_used; k++) {
				if (levels[k] && EstimateDistinct(levels[k][17]) >= clip_support_threshold) {
					if (first_supported < 0) {
						first_supported = k;
					}
					last_supported = k;
				}
			}
		}

		// Pass 2: accumulate contributions with soft clamping
		for (int k = 0; k <= max_level_used; k++) {
			if (!levels[k]) {
				continue;
			}

			// Determine effective scale: clamp under-supported prefix/suffix levels
			int effective_level = k;
			if (clip_support_threshold > 0 && first_supported >= 0) {
				if (k < first_supported) {
					// Prefix: clamp scale up to first supported level
					effective_level = first_supported;
				} else if (k > last_supported) {
					// Suffix: hard zero — unsupported outlier levels contribute nothing
					continue;
				}
			} else if (clip_support_threshold > 0 && first_supported < 0) {
				// No supported levels at all — zero everything
				continue;
			}

			PAC_FLOAT scale = std::exp2(static_cast<PAC_FLOAT>(PAC2_LEVEL_SHIFT * effective_level));

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
					memcpy(levels[k], src->levels[k], PAC2_NORMAL_ELEMENTS * sizeof(uint64_t));
				}
				continue;
			}

			// Both have this level: merge
			// Add SWAR counters
			for (int i = 0; i < PAC2_NORMAL_SWAR; i++) {
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
				for (int i = 0; i < PAC2_OVERFLOW_SWAR; i++) {
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
	PacClipSumIntState *GetState() {
		return this;
	}
	PacClipSumIntState *EnsureState(ArenaAllocator &) {
		return this;
	}
};

// ============================================================================
// PacClipSumStateWrapper: buffering wrapper with two-sided pos/neg
// ============================================================================
struct PacClipSumStateWrapper {
	using State = PacClipSumIntState;
	using Value = uint64_t;
	static constexpr int BUF_SIZE = 2;
	static constexpr uint64_t BUF_MASK = 3ULL;

	uint64_t val_buf[BUF_SIZE];
	uint64_t hash_buf[BUF_SIZE];
	union {
		uint64_t n_buffered; // lower 2 bits: count, upper bits: state pointer
		PacClipSumIntState *state;
	};
	PacClipSumIntState *neg_state; // separate state for negatives (stores absolute values)

	PacClipSumIntState *GetState() const {
		return reinterpret_cast<PacClipSumIntState *>(reinterpret_cast<uintptr_t>(state) & ~7ULL);
	}

	PacClipSumIntState *EnsureState(ArenaAllocator &a) {
		PacClipSumIntState *s = GetState();
		if (!s) {
			s = reinterpret_cast<PacClipSumIntState *>(a.Allocate(sizeof(PacClipSumIntState)));
			memset(s, 0, sizeof(PacClipSumIntState));
			s->max_level_used = -1;
			s->inline_level_idx = -1;
			state = s;
		}
		return s;
	}

	PacClipSumIntState *GetNegState() const {
		return neg_state;
	}

	PacClipSumIntState *EnsureNegState(ArenaAllocator &a) {
		if (!neg_state) {
			neg_state = reinterpret_cast<PacClipSumIntState *>(a.Allocate(sizeof(PacClipSumIntState)));
			memset(neg_state, 0, sizeof(PacClipSumIntState));
			neg_state->max_level_used = -1;
			neg_state->inline_level_idx = -1;
		}
		return neg_state;
	}

	static idx_t StateSize() {
		return sizeof(PacClipSumStateWrapper);
	}
};

} // namespace duckdb

#endif // PAC_CLIP_SUM_HPP
