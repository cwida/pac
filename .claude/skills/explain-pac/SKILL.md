---
name: explain-pac
description: Reference material for PAC privacy internals. Auto-loaded when discussing PAC mechanism, noise, counters, or clipping.
---

## PAC Privacy Overview

PAC (Probably Approximately Correct) privacy is a framework for privatizing SQL
aggregates, described in [SIMD-PAC-DB](https://arxiv.org/abs/2603.15023).

### Core mechanism

- Each aggregate maintains **64 parallel counters** (one per bit of a hashed key)
- Each row's value is added to ~32 counters (determined by pac_hash of the PU key)
- At finalization, noise calibrated to a **mutual information bound** (pac_mi) is
  added, and the result is estimated from the counters
- PAC does NOT compute sensitivity (unlike differential privacy)

### SWAR bitslice encoding

- Counters are packed as 4 × uint16_t per uint64_t (SWAR = SIMD Within A Register)
- This enables processing 4 counters per instruction without actual SIMD intrinsics
- Overflow cascades to 32-bit overflow counters when 16-bit counters saturate

### pac_clip_sum (contribution clipping)

- **Pre-aggregation**: Query rewriter inserts `GROUP BY pu_hash` to sum each user's
  rows into a single contribution (handles the "50K small items" case)
- **Magnitude levels**: Values decomposed into levels (4x per level, 2-bit shift).
  Level 0: 0-255, Level 1: 256-1023, Level 2: 1024-4095, etc.
- **Bitmap tracking**: Each level maintains a 64-bit bitmap of distinct contributors
  (using birthday-paradox estimation from popcount)
- **Hard-zero**: Levels with fewer distinct contributors than `pac_clip_support`
  contribute nothing to the result (prevents variance side-channel attacks)

### Key settings

- `pac_mi`: Mutual information bound (0 = deterministic/no noise)
- `pac_seed`: RNG seed for reproducible noise
- `pac_clip_support`: Minimum distinct contributors per magnitude level (NULL = disabled)
- `pac_hash_repair`: Ensure pac_hash outputs exactly 32 bits set

### DDL

```sql
ALTER TABLE customer ADD PAC_KEY (c_custkey);
ALTER TABLE customer SET PU;
ALTER TABLE orders ADD PAC_LINK (o_custkey) REFERENCES customer (c_custkey);
```
