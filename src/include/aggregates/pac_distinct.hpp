//
// PAC DISTINCT aggregate variants: pac_count_distinct, pac_sum_distinct, pac_avg_distinct
//
// These are NON-DISTINCT aggregates that handle deduplication internally.
// DuckDB's DISTINCT deduplicates on ALL aggregate arguments, which breaks PAC
// because the per-row hash (e.g., hash(rowid)) makes every tuple unique.
//
// Instead, these functions accept (key_hash, value_or_hash) and maintain an
// internal hash map: value_key → OR(key_hashes). In finalize, they iterate the
// map to build 64 counters, then noise as usual.
//

#ifndef PAC_DISTINCT_HPP
#define PAC_DISTINCT_HPP

#include "duckdb.hpp"
#include "pac_aggregate.hpp"

namespace duckdb {

void RegisterPacCountDistinctFunctions(ExtensionLoader &);
void RegisterPacSumDistinctFunctions(ExtensionLoader &);
void RegisterPacAvgDistinctFunctions(ExtensionLoader &);

// ============================================================================
// PacFlatMap: open-addressing hash map with linear probing for uint64_t keys.
// ~3-10x faster than std::unordered_map due to flat memory layout and no
// per-entry heap allocation. Uses duckdb::vector for storage.
// ============================================================================
template <typename Value>
struct PacFlatMap {
	static constexpr uint64_t EMPTY_KEY = ~uint64_t(0);

	struct Entry {
		uint64_t key;
		Value value;
	};

	duckdb::vector<Entry> entries;
	uint32_t capacity;
	uint32_t count;
	bool has_sentinel;
	Value sentinel_value;

	explicit PacFlatMap(uint32_t initial_cap = 1024) : capacity(0), count(0), has_sentinel(false), sentinel_value {} {
		Grow(initial_cap);
	}

	PacFlatMap(const PacFlatMap &) = delete;
	PacFlatMap &operator=(const PacFlatMap &) = delete;

	void Grow(uint32_t new_cap) {
		duckdb::vector<Entry> old = std::move(entries);
		uint32_t old_cap = capacity;
		capacity = new_cap;
		count = has_sentinel ? 1 : 0;
		entries.resize(capacity);
		for (uint32_t i = 0; i < capacity; i++) {
			entries[i].key = EMPTY_KEY;
		}
		for (uint32_t i = 0; i < old_cap; i++) {
			if (old[i].key != EMPTY_KEY) {
				InsertForResize(old[i].key, old[i].value);
			}
		}
	}

	// Returns pointer to value for key. New entries are value-initialized ({}).
	Value *GetOrCreate(uint64_t key) {
		if (key == EMPTY_KEY) {
			if (!has_sentinel) {
				has_sentinel = true;
				sentinel_value = Value {};
				count++;
			}
			return &sentinel_value;
		}
		if (count * 4 >= capacity * 3) {
			Grow(capacity * 2);
		}
		uint32_t mask = capacity - 1;
		uint32_t idx = FibHash(key) & mask;
		while (true) {
			if (entries[idx].key == key) {
				return &entries[idx].value;
			}
			if (entries[idx].key == EMPTY_KEY) {
				entries[idx].key = key;
				entries[idx].value = Value {};
				count++;
				return &entries[idx].value;
			}
			idx = (idx + 1) & mask;
		}
	}

	template <typename Fn>
	void ForEach(Fn &&fn) const {
		if (has_sentinel) {
			fn(EMPTY_KEY, sentinel_value);
		}
		for (uint32_t i = 0; i < capacity; i++) {
			if (entries[i].key != EMPTY_KEY) {
				fn(entries[i].key, entries[i].value);
			}
		}
	}

private:
	static uint32_t FibHash(uint64_t key) {
		return static_cast<uint32_t>((key * 0x9E3779B97F4A7C15ULL) >> 32);
	}

	void InsertForResize(uint64_t key, const Value &val) {
		uint32_t mask = capacity - 1;
		uint32_t idx = FibHash(key) & mask;
		while (entries[idx].key != EMPTY_KEY) {
			idx = (idx + 1) & mask;
		}
		entries[idx].key = key;
		entries[idx].value = val;
		count++;
	}
};

// State for pac_count_distinct: tracks value_hash → OR(key_hashes)
struct PacCountDistinctState {
	PacFlatMap<uint64_t> *distinct_map;
	uint64_t key_hash;
	uint64_t update_count;
};

// State for pac_sum_distinct / pac_avg_distinct: tracks value → (OR(key_hashes), value)
struct PacSumDistinctState {
	PacFlatMap<std::pair<uint64_t, double>> *distinct_map;
	uint64_t key_hash;
	uint64_t update_count;
};

} // namespace duckdb

#endif // PAC_DISTINCT_HPP
