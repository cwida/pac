//
// PAC Metadata Manager
//
// This file implements the PrivacyMetadataManager class which manages PAC table metadata in memory.
// It provides thread-safe storage and retrieval of metadata for PAC-protected tables including:
// - Primary keys (PAC KEY)
// - Foreign key links (PRIVACY_LINK)
// - Protected columns (PROTECTED)
//
// Created by refactoring privacy_parser.cpp on 1/22/26.
//

#include "parser/privacy_parser.hpp"
#include "metadata/privacy_metadata_manager.hpp"
#include "privacy_debug.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"

namespace duckdb {

// ============================================================================
// PrivacyMetadataManager Implementation
// ============================================================================

/**
 * PrivacyMetadataManager: Singleton that manages PAC table metadata in memory
 *
 * This class stores metadata for all PAC-protected tables:
 * - Primary keys (PAC KEY)
 * - Foreign key links (PRIVACY_LINK)
 * - Protected columns (PROTECTED)
 *
 * The metadata is stored in memory and can be serialized to/from JSON files
 * for persistence across database sessions.
 */
PrivacyMetadataManager &PrivacyMetadataManager::Get() {
	static PrivacyMetadataManager instance;
	return instance;
}

/**
 * AddOrUpdateTable: Adds a new table's metadata or updates existing metadata
 *
 * @param table_name - Name of the table (will be normalized to lowercase)
 * @param metadata - Metadata for the table
 *
 * This function is thread-safe and locks the metadata map during the operation.
 * Table names are normalized to lowercase for case-insensitive lookups.
 */
void PrivacyMetadataManager::AddOrUpdateTable(const string &table_name, const PrivacyTableMetadata &metadata) {
	std::lock_guard<std::mutex> lock(metadata_mutex);
	string normalized_name = StringUtil::Lower(table_name);
	table_metadata[normalized_name] = metadata;
}

/**
 * GetTableMetadata: Retrieves metadata for a table
 *
 * @param table_name - Name of the table (will be normalized to lowercase)
 * @return Pointer to the table metadata, or nullptr if not found
 *
 * Table names are normalized to lowercase for case-insensitive lookups.
 */
const PrivacyTableMetadata *PrivacyMetadataManager::GetTableMetadata(const string &table_name) const {
	std::lock_guard<std::mutex> lock(metadata_mutex);
	string normalized_name = StringUtil::Lower(table_name);
	auto it = table_metadata.find(normalized_name);
#if PRIVACY_DEBUG
	PRIVACY_DEBUG_PRINT("GetTableMetadata: looking up '" + table_name + "' (normalized: '" + normalized_name + "')");
	PRIVACY_DEBUG_PRINT("GetTableMetadata: metadata store has " + std::to_string(table_metadata.size()) + " entries");
	for (const auto &entry : table_metadata) {
		PRIVACY_DEBUG_PRINT("  stored table: '" + entry.first + "'");
	}
#endif
	if (it != table_metadata.end()) {
#if PRIVACY_DEBUG
		PRIVACY_DEBUG_PRINT("GetTableMetadata: FOUND metadata for '" + normalized_name + "'");
		PRIVACY_DEBUG_PRINT("  - links count: " + std::to_string(it->second.links.size()));
		PRIVACY_DEBUG_PRINT("  - protected_columns count: " + std::to_string(it->second.protected_columns.size()));
		PRIVACY_DEBUG_PRINT("  - primary_key_columns count: " + std::to_string(it->second.primary_key_columns.size()));
		for (const auto &link : it->second.links) {
			PRIVACY_DEBUG_PRINT("  - link to: " + link.referenced_table);
		}
#endif
		return &it->second;
	}
#if PRIVACY_DEBUG
	PRIVACY_DEBUG_PRINT("GetTableMetadata: NOT FOUND metadata for '" + normalized_name + "'");
#endif
	return nullptr;
}

PrivacyTableMetadata *PrivacyMetadataManager::GetMutableTableMetadata(const string &table_name) {
	std::lock_guard<std::mutex> lock(metadata_mutex);
	string normalized_name = StringUtil::Lower(table_name);
	auto it = table_metadata.find(normalized_name);
	return it != table_metadata.end() ? &it->second : nullptr;
}

/**
 * HasMetadata: Checks if metadata exists for a table
 *
 * @param table_name - Name of the table (will be normalized to lowercase)
 * @return True if metadata exists, false otherwise
 *
 * Table names are normalized to lowercase for case-insensitive lookups.
 */
bool PrivacyMetadataManager::HasMetadata(const string &table_name) const {
	std::lock_guard<std::mutex> lock(metadata_mutex);
	string normalized_name = StringUtil::Lower(table_name);
	return table_metadata.find(normalized_name) != table_metadata.end();
}

/**
 * GetAllTableNames: Retrieves a list of all table names with metadata
 *
 * @return Vector of table names
 */
vector<string> PrivacyMetadataManager::GetAllTableNames() const {
	std::lock_guard<std::mutex> lock(metadata_mutex);
	vector<string> names;
	names.reserve(table_metadata.size());
	for (const auto &entry : table_metadata) {
		names.push_back(entry.first);
	}
	return names;
}

/**
 * RemoveTable: Removes a table's metadata
 *
 * @param table_name - Name of the table (will be normalized to lowercase)
 *
 * This function is thread-safe and locks the metadata map during the operation.
 * Table names are normalized to lowercase for case-insensitive lookups.
 */
void PrivacyMetadataManager::RemoveTable(const string &table_name) {
	std::lock_guard<std::mutex> lock(metadata_mutex);
	string normalized_name = StringUtil::Lower(table_name);
	table_metadata.erase(normalized_name);
}

/**
 * Clear: Clears all metadata for tables
 *
 * This function is thread-safe and locks the metadata map during the operation.
 */
void PrivacyMetadataManager::Clear() {
	std::lock_guard<std::mutex> lock(metadata_mutex);
	table_metadata.clear();
}

/**
 * GetMetadataFilePath: Constructs the path to the PAC metadata JSON file
 *
 * Format: <db_directory>/privacy_metadata_<dbname>_<schema>.json
 *
 * For example:
 *   - Database: /data/tpch.db
 *   - Schema: main
 *   - Result: /data/privacy_metadata_tpch_main.json
 *
 * Returns empty string for in-memory databases (no file saved).
 */
string PrivacyMetadataManager::GetMetadataFilePath(ClientContext &context) {
	// Get the database path from the default catalog
	auto &db_name = DatabaseManager::GetDefaultDatabase(context);
	auto &catalog = Catalog::GetCatalog(context, db_name);
	string db_path = catalog.GetDBPath();

	// If in-memory database or empty path, return empty string (don't save to file)
	if (db_path.empty() || db_path == ":memory:") {
		return "";
	}

	// Extract schema name from the catalog search path
	string schema_name = DEFAULT_SCHEMA; // Fallback to "main"
	try {
		CatalogSearchPath search_path(context);
		const auto &entries = search_path.Get();
		if (!entries.empty()) {
			schema_name = entries[0].schema;
		}
	} catch (...) {
		// If we can't get search path, use default schema
		schema_name = DEFAULT_SCHEMA;
	}

	// Extract directory from database path and append metadata filename
	// Format: privacy_metadata_<dbname>_<schema>.json
	string filename = "privacy_metadata_" + db_name + "_" + schema_name + ".json";

	size_t last_slash = db_path.find_last_of("/\\");
	if (last_slash != string::npos) {
		return db_path.substr(0, last_slash + 1) + filename;
	}

	// No directory separator found, use current directory
	return filename;
}

} // namespace duckdb
