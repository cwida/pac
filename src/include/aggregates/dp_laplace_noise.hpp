#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

// Registers `dp_laplace_noise(value DOUBLE, scale DOUBLE) -> DOUBLE`.
// Returns value + Lap(scale) (location 0, scale = scale). Deterministic when `privacy_seed` is set.
void RegisterDpLaplaceNoiseFunction(ExtensionLoader &loader);

} // namespace duckdb
