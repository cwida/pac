#pragma once

#include "duckdb.hpp"
#include <unordered_set>
#include <string>

namespace duckdb {

// Read/write pac tables file
std::unordered_set<std::string> ReadPacTablesFile(const std::string &filename);
void WritePacTablesFile(const std::string &filename, const std::unordered_set<std::string> &tables);

// Helper: check if table exists in the current catalog
bool TableExists(ClientContext &context, const std::string &table_name);

// Pragma-style helpers: PRAGMA add_privacy_unit(...) and PRAGMA remove_privacy_unit(...)
void AddPrivacyUnitPragma(ClientContext &context, const FunctionParameters &parameters);
void RemovePrivacyUnitPragma(ClientContext &context, const FunctionParameters &parameters);

// Helper to delete the privacy unit file (for tests/cleanup)
void DeletePrivacyUnitFileFun(DataChunk &args, ExpressionState &state, Vector &result);

} // namespace duckdb
