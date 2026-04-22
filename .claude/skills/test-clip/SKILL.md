---
name: test-clip
description: Build and run pac_clip_sum unit tests.
---

## Instructions

1. Build: `GEN=ninja make 2>&1 | tail -5`
2. Run clip_sum tests: `build/release/test/unittest "test/sql/pac_clip_sum*" 2>&1`
3. Report: number of assertions passed/failed
4. If any fail, show the failing test name and expected vs actual values
