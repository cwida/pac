#pragma once

// Set to 1 to enable verbose PAC debug output, 0 to disable
// This is separate from DuckDB's DEBUG macro to avoid cluttering test output
#define PRIVACY_DEBUG 0

#if PRIVACY_DEBUG
#define PRIVACY_DEBUG_PRINT(x) Printer::Print(x)
#else
#define PRIVACY_DEBUG_PRINT(x) ((void)0)
#endif
