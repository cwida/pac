---
name: run-attacks
description: Run the pac_clip_sum membership inference attack test suite and summarize results.
---

## Context

PAC (Probably Approximately Correct) privacy privatizes SQL aggregates via 64 parallel
SWAR bitslice counters with MI-bounded noise. pac_clip_sum adds per-user contribution
clipping using magnitude-level decomposition (4x bands, 2-bit shift) with distinct-contributor
bitmaps. Unsupported outlier levels are hard-zeroed to prevent variance side-channel attacks.

## Instructions

1. Build if needed: `GEN=ninja make 2>&1 | tail -5`
2. Run the main attack suite: `bash attacks/clip_attack_test.sh 2>/dev/null`
3. Run the multi-row attack: `bash attacks/clip_multirow_test.sh 2>/dev/null`
4. Run stress tests if available: `bash attacks/clip_hardzero_stress.sh 2>/dev/null`

Summarize results as a table:
- Attack scenario, clip_support value, attack accuracy, std_in, std_out, std ratio
- Flag any accuracy above 60% as a potential regression
- Compare to baselines in `attacks/clip_attack_results.md`
