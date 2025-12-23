//
// Created by ila on 12/19/25.
//

#ifndef PAC_COUNT_HPP
#define PAC_COUNT_HPP

#include "duckdb.hpp"
#include "pac_aggregate.hpp"

namespace duckdb {

void RegisterPacCountFunctions(ExtensionLoader &);

// PAC_COUNT(key_hash) implements a COUNT aggregate that for each privacy-unit (identified by a key_hash)
// computes 64 independent counts, where each independent count randomly (50% chance) includes a PU or not.
// The observation is that the 64-bits of a hashed key of the PU are random (50% 0, 50% 1), so we can take
// the 64 bits of the key to make 64 independent decisions.
//
// A COUNT() aggregate in its implementation simply performs total += 1
//
// PAC_COUNT() needs to do for(i=0; i<64; i++) total[i] += (key_hash >> i) & 1; (extract bit i)
//
// We want to do this in a SIMD-friendly way. Therefore, we want to create 64 subtotals of uint8_t (i.e. bytes),
// and perform 64 byte-additions, because in the widest SIMD implementation, AVX512, this means that this
// could be done in a *SINGLE* instruction (AVX512 has 64 lanes of uint8, as 64x8=512)
//
// However, to help auto-vectorizing compilers, we do not use uint8_t subtotals[64], but uint64_t subtotals[8]
// because key_hash is also uint64_t. We apply the below mask to key_hash to extract the lowest bit of each byte:

#define PAC_COUNT_MASK                                                                                                 \
	(1ULL | (1ULL << 8) | (1ULL << 16) | (1ULL << 24) | (1ULL << 32) | (1ULL << 40) | (1ULL << 48) | (1ULL << 56))

// For each of the 8 iterations i, we then do (hash_key>>i) & PAC_COUNT_MASK which selects 8 bits, and then add these
// with a single uint64_t ADD to a uint64 subtotals[]. You can only do that 255 times before the bytes in this uint64_t
// start touching each other (causing overflow).
// So after 255 iterations, the subtotals[] are added to full uint64_t totals[64] and reset to 0.
//
// The idea is that we get very fast performance 254 times and slower performance once every 255 only.
// This SIMD-friendly implementation can make PAC counting almost as fast as normal counting.

// State for pac_count: 64 full uint64_t totals and just 8 uint64_t subtotals (where each consists of 8 byte-counters)
struct PacCountState {
	uint64_t probabilistic_subtotals[8]; // SIMD-friendly accumulators (8 x uint64_t, containing byte sub-counters)
	uint64_t probabilistic_totals[64];   // Final counters (64 x uint64_t full counters)
	uint32_t exact_subtotal;             // Counts updates until 255

	AUTOVECTORIZE void inline Flush() {
		if (++exact_subtotal == 255) {
			const uint8_t *probabilistic_subtotals8 = reinterpret_cast<const uint8_t *>(probabilistic_subtotals);
			for (int i = 0; i < 64; i++) {
				probabilistic_totals[i] += probabilistic_subtotals8[i];
			}
			memset(probabilistic_subtotals, 0, sizeof(probabilistic_subtotals));
			exact_subtotal = 0;
		}
	}
};

} // namespace duckdb

#endif // PAC_COUNT_HPP
