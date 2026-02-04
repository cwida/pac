#pragma once

// Set to 1 to enable debug output, 0 to disable
#define PAC_DEBUG 0

#if PAC_DEBUG
#define PAC_DEBUG_PRINT(x) Printer::Print(x)
#else
#define PAC_DEBUG_PRINT(x) ((void)0)
#endif
