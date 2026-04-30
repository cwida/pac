//
// PAC Parser Helpers
//
// This file implements SQL parsing and clause extraction for PAC statements.
// It handles parsing of CREATE PU TABLE and ALTER PU TABLE statements,
// extracting PAC-specific clauses (PRIVACY_KEY, PRIVACY_LINK, PROTECTED), and
// stripping these clauses to produce valid SQL for DuckDB's standard parser.
//
// Created by refactoring privacy_parser.cpp on 1/22/26.
//

// IMPORTANT: <regex> must be included BEFORE duckdb headers on Windows MSVC
// because DuckDB defines its own std namespace that conflicts with <regex>
#include <regex>

#include "parser/privacy_parser.hpp"
#include "parser/privacy_parser_helpers.hpp"
#include "privacy_debug.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

// ============================================================================
// Table name extraction
// ============================================================================

/**
 * ExtractTableName: Extracts table name from CREATE or ALTER TABLE statement
 *
 * Handles:
 *   - CREATE TABLE table_name ...
 *   - CREATE PU TABLE table_name ...
 *   - CREATE TABLE IF NOT EXISTS table_name ...
 *   - ALTER TABLE table_name ...
 */
string PrivacyParserExtension::ExtractTableName(const string &sql, bool is_create) {
	if (is_create) {
		// Match: CREATE [PU] TABLE [IF NOT EXISTS] table_name
		std::regex create_regex(R"(create\s+(?:pu\s+)?table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z_][a-zA-Z0-9_]*))");
		std::smatch match;
		string sql_lower = StringUtil::Lower(sql);
		if (std::regex_search(sql_lower, match, create_regex)) {
			return match[1].str();
		}
	} else {
		// Match: ALTER TABLE table_name
		std::regex alter_regex(R"(alter\s+table\s+([a-zA-Z_][a-zA-Z0-9_]*))");
		std::smatch match;
		string sql_lower = StringUtil::Lower(sql);
		if (std::regex_search(sql_lower, match, alter_regex)) {
			return match[1].str();
		}
	}
	return "";
}

// ============================================================================
// PAC clause extraction
// ============================================================================

/**
 * ExtractPACPrimaryKey: Parses PRIVACY_KEY clause
 *
 * Syntax: PRIVACY_KEY (col1, col2, ...)
 *
 * Supports composite keys (multiple columns).
 */
bool PrivacyParserExtension::ExtractPACPrimaryKey(const string &clause, vector<string> &pk_columns) {
	// Match: PRIVACY_KEY (col1, col2, ...)
	std::regex pk_regex(R"(privacy_key\s*\(\s*([^)]+)\s*\))");
	std::smatch match;
	string clause_lower = StringUtil::Lower(clause);

	if (std::regex_search(clause_lower, match, pk_regex)) {
		string cols_str = match[1].str();
		// Split by comma
		auto cols = StringUtil::Split(cols_str, ',');
		for (auto &col : cols) {
			StringUtil::Trim(col);
			if (!col.empty()) {
				pk_columns.push_back(col);
			}
		}
		return true;
	}
	return false;
}

/**
 * ExtractPACLink: Parses PRIVACY_LINK clause
 *
 * Syntax: PRIVACY_LINK (local_col1, local_col2, ...) REFERENCES table(ref_col1, ref_col2, ...)
 *
 * This defines a foreign key relationship for PAC. The number of local columns
 * must match the number of referenced columns (composite key support).
 */
bool PrivacyParserExtension::ExtractPACLink(const string &clause, PACLink &link) {
	// Match: PRIVACY_LINK (col1, col2, ...) REFERENCES table_name(ref_col1, ref_col2, ...)
	// Support both single and composite foreign keys
	std::regex link_regex(
	    R"(privacy_link\s*\(\s*([^)]+)\s*\)\s+references\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(\s*([^)]+)\s*\))");
	std::smatch match;
	string clause_lower = StringUtil::Lower(clause);

	if (std::regex_search(clause_lower, match, link_regex)) {
		// Parse local columns (comma-separated)
		string local_cols_str = match[1].str();
		auto local_cols = StringUtil::Split(local_cols_str, ',');
		for (auto &col : local_cols) {
			StringUtil::Trim(col);
			if (!col.empty()) {
				link.local_columns.push_back(col);
			}
		}

		// Parse referenced table
		link.referenced_table = match[2].str();

		// Parse referenced columns (comma-separated)
		string ref_cols_str = match[3].str();
		auto ref_cols = StringUtil::Split(ref_cols_str, ',');
		for (auto &col : ref_cols) {
			StringUtil::Trim(col);
			if (!col.empty()) {
				link.referenced_columns.push_back(col);
			}
		}

		// Validate that number of local columns matches number of referenced columns
		if (link.local_columns.size() != link.referenced_columns.size()) {
			return false;
		}

		return !link.local_columns.empty();
	}
	return false;
}

/**
 * ExtractProtectedColumns: Parses PROTECTED clause
 *
 * Syntax: PROTECTED (col1, col2, ...)
 *
 * Protected columns are those that contain sensitive data and need PAC protection.
 */
bool PrivacyParserExtension::ExtractProtectedColumns(const string &clause, vector<string> &protected_cols) {
	// Match: PROTECTED (col1, col2, ...)
	// Use word boundary to avoid matching 'protected' as part of identifiers
	std::regex protected_regex(R"(\bprotected\s*\(\s*([^)]+)\s*\))");
	std::smatch match;
	string clause_lower = StringUtil::Lower(clause);

	if (std::regex_search(clause_lower, match, protected_regex)) {
		string cols_str = match[1].str();
		auto cols = StringUtil::Split(cols_str, ',');
		for (auto &col : cols) {
			StringUtil::Trim(col);
			if (!col.empty()) {
				protected_cols.push_back(col);
			}
		}
		return true;
	}
	return false;
}

/**
 * StripPACClauses: Removes all PAC-specific clauses from SQL
 *
 * This is necessary because DuckDB's standard parser doesn't understand PAC syntax.
 * We remove PAC clauses and execute the "clean" SQL, while storing PAC metadata separately.
 *
 * Removed clauses:
 *   - PRIVACY_KEY (...)
 *   - PRIVACY_LINK (...) REFERENCES table(...)
 *   - PROTECTED (...)
 */
string PrivacyParserExtension::StripPACClauses(const string &sql) {
	string result = sql;

	// Remove PRIVACY_KEY clauses (case-insensitive)
	result = std::regex_replace(result, std::regex(R"(,?\s*PRIVACY_KEY\s*\([^)]*\)\s*,?)", std::regex_constants::icase),
	                            " ");

	// Remove PRIVACY_LINK clauses (case-insensitive)
	// The "PRIVACY_LINK" and "REFERENCES" keywords ensure we match the right thing
	result = std::regex_replace(
	    result,
	    std::regex(R"(,?\s*PRIVACY_LINK\s*\([^)]*\)\s+REFERENCES\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\([^)]*\)\s*,?)",
	               std::regex_constants::icase),
	    " ");

	// Remove PROTECTED clauses (case-insensitive)
	// PROTECTED is a unique keyword that shouldn't conflict with SQL types
	// Use word boundary to avoid matching 'protected' as part of identifiers (e.g., 'regular_with_protected')
	result = std::regex_replace(result, std::regex(R"(,?\s*\bPROTECTED\s*\([^)]*\)\s*,?)", std::regex_constants::icase),
	                            " ");

	// Clean up multiple spaces and trailing commas
	result = std::regex_replace(result, std::regex(R"(\s+)"), " ");
	result = std::regex_replace(result, std::regex(R"(,\s*,)"), ",");
	result = std::regex_replace(result, std::regex(R"(\(\s*,)"), "(");
	result = std::regex_replace(result, std::regex(R"(,\s*\))"), ")");

	return result;
}

// ============================================================================
// SQL statement parsing
// ============================================================================

/**
 * ParseCreatePACTable: Parses CREATE PU TABLE statement
 *
 * Syntax:
 *   CREATE PU TABLE table_name (
 *     col1 INTEGER,
 *     col2 VARCHAR,
 *     PRIVACY_KEY (col1),
 *     PRIVACY_LINK (col2) REFERENCES other_table(other_col),
 *     PROTECTED (col1, col2)
 *   );
 *
 * Returns:
 *   - stripped_sql: SQL with PAC clauses removed
 *   - metadata: Extracted PAC metadata (keys, links, protected columns)
 */
bool PrivacyParserExtension::ParseCreatePACTable(const string &query, string &stripped_sql,
                                                 PrivacyTableMetadata &metadata) {
	string query_lower = StringUtil::Lower(query);

	// Check if this is a CREATE PU TABLE statement
	if (query_lower.find("create pu table") == string::npos && query_lower.find("create table") == string::npos) {
		return false;
	}

	// Extract table name
	metadata.table_name = ExtractTableName(query, true);
	if (metadata.table_name.empty()) {
		return false;
	}

	// Check if this is CREATE PU TABLE (marks the table as a privacy unit)
	bool is_create_pu_table = query_lower.find("create pu table") != string::npos;

	// Check if any PAC clauses exist
	// Use regex to avoid matching as part of identifiers
	bool has_pac_clauses = std::regex_search(query_lower, std::regex(R"(\bprivacy_key\s*\()")) ||
	                       std::regex_search(query_lower, std::regex(R"(\bprivacy_link\s*\()")) ||
	                       std::regex_search(query_lower, std::regex(R"(\bprotected\s*\()"));

	if (!has_pac_clauses && !is_create_pu_table) {
		return false;
	}

	// If CREATE PU TABLE, mark as privacy unit
	if (is_create_pu_table) {
		metadata.is_privacy_unit = true;
	}

	// Extract PAC clauses
	ExtractPACPrimaryKey(query, metadata.primary_key_columns);

	// Extract all PRIVACY_LINK clauses
	std::regex link_regex(R"(privacy_link\s*\([^)]+\)\s+references\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\([^)]+\))");
	auto begin = std::sregex_iterator(query_lower.begin(), query_lower.end(), link_regex);
	auto end = std::sregex_iterator();
	for (auto it = begin; it != end; ++it) {
		PACLink link;
		if (ExtractPACLink((*it).str(), link)) {
			metadata.links.push_back(link);
		}
	}

	// Check for PRIVACY_LINK without REFERENCES (common syntax mistake)
	// e.g. "PRIVACY_LINK(col, table, ref)" instead of "PRIVACY_LINK(col) REFERENCES table(ref)"
	if (std::regex_search(query_lower, std::regex(R"(\bprivacy_link\s*\()")) && metadata.links.empty()) {
		throw ParserException(
		    "Invalid PRIVACY_LINK syntax. Use PRIVACY_LINK (col) REFERENCES table(ref_col). "
		    "Example: CREATE TABLE t (id INT, fk INT, PRIVACY_LINK (fk) REFERENCES other_table(id), PROTECTED (id))");
	}

	// Extract protected columns
	ExtractProtectedColumns(query, metadata.protected_columns);

	// Check for bare PROTECTED keyword without parentheses (common syntax mistake)
	// e.g. "value INT PROTECTED" instead of "PROTECTED (value)"
	if (std::regex_search(query_lower, std::regex(R"(\bprotected\b)")) && metadata.protected_columns.empty()) {
		throw ParserException(
		    "Invalid PROTECTED syntax. Use PROTECTED (col1, col2, ...) as a separate clause, not inline after a column "
		    "type. Example: CREATE PU TABLE t (id INT, val INT, PRIVACY_KEY (id), PROTECTED (val))");
	}

	// Validate: CREATE PU TABLE must have PRIVACY_KEY
	if (is_create_pu_table) {
		if (metadata.primary_key_columns.empty()) {
			// Check for common typo: "PAC KEY" with space instead of underscore
			if (std::regex_search(query_lower, std::regex(R"(\bpac\s+key\b)"))) {
				throw ParserException("Did you mean PRIVACY_KEY (with underscore)? "
				                      "Use PRIVACY_KEY (col) to define the privacy unit key.");
			}
			throw ParserException("CREATE PU TABLE requires a PRIVACY_KEY clause to identify the privacy unit");
		}
	}

	// Strip PAC clauses and replace "CREATE PU TABLE" with "CREATE TABLE"
	stripped_sql = StripPACClauses(query);
	stripped_sql = std::regex_replace(stripped_sql, std::regex(R"(create\s+pu\s+table)", std::regex_constants::icase),
	                                  "CREATE TABLE");

	return true;
}

/**
 * ParseAlterTableAddPAC: Parses ALTER [PU] TABLE ... ADD ... statement
 *
 * Syntax:
 *   ALTER [PU] TABLE table_name ADD PRIVACY_KEY (col1);
 *   ALTER [PU] TABLE table_name ADD PRIVACY_LINK (col1) REFERENCES other(col2);
 *   ALTER [PU] TABLE table_name ADD PROTECTED (col1, col2);
 *
 * This operation is metadata-only (no actual DDL executed). It merges new
 * metadata with existing metadata for the table.
 *
 * Validation:
 *   - Columns must exist in the table
 *   - Protected columns can't be added twice (idempotency check)
 *   - PRIVACY_LINKs can't conflict (same local columns to different targets)
 */
bool PrivacyParserExtension::ParseAlterTableAddPAC(const string &query, string &stripped_sql,
                                                   PrivacyTableMetadata &metadata) {
	string query_lower = StringUtil::Lower(query);

	// Check if this is an ALTER [PU] TABLE ... ADD PRIVACY_KEY/PRIVACY_LINK/PROTECTED statement
	// Both "ALTER PU TABLE t ADD ..." and "ALTER TABLE t ADD ..." are supported,
	// but the PU keyword must match the table's actual PU status.
	bool has_pac_add =
	    (query_lower.find("add privacy_key") != string::npos || query_lower.find("add privacy_link") != string::npos ||
	     query_lower.find("add protected") != string::npos);
	if (!has_pac_add) {
		return false;
	}

	// Detect whether the user wrote ALTER PU TABLE or ALTER TABLE
	bool used_pu_keyword = (query_lower.find("alter pu table") != string::npos);

	// Extract table name - supports both "ALTER PU TABLE t" and "ALTER TABLE t"
	std::regex alter_pu_regex(R"(alter\s+(?:pu\s+)?table\s+([a-zA-Z_][a-zA-Z0-9_]*))");
	std::smatch match;
	if (!std::regex_search(query_lower, match, alter_pu_regex)) {
		return false;
	}
	metadata.table_name = match[1].str();

	// Get existing metadata if any
	auto existing = PrivacyMetadataManager::Get().GetTableMetadata(metadata.table_name);
	if (existing) {
		metadata = *existing;
#if PRIVACY_DEBUG
		std::cerr << "[PAC DEBUG] ParseAlterTableAddPAC: Found existing metadata for " << metadata.table_name
		          << ", links=" << existing->links.size() << ", protected=" << existing->protected_columns.size()
		          << "\n";
#endif
	} else {
#if PRIVACY_DEBUG
		std::cerr << "[PAC DEBUG] ParseAlterTableAddPAC: No existing metadata for " << metadata.table_name << "\n";
#endif
	}

	// Check for PAC-related keywords (needed before the PU keyword validation below)
	bool has_privacy_key = query_lower.find("privacy_key") != string::npos;
	bool has_privacy_link = query_lower.find("privacy_link") != string::npos;
	bool has_protected = query_lower.find("protected") != string::npos;

	// Validate PU keyword matches table status.
	// ADD PRIVACY_KEY is exempt from the !used_pu_keyword && is_pu error because:
	// - PRIVACY_KEY is idempotent (safe to re-add)
	// - In DuckDB's batch mode (-c flag), PRAGMA clear_privacy_metadata is parsed before it
	//   executes, so stale JSON metadata can make a PU table appear as PU at parse time
	bool is_pu = existing && existing->is_privacy_unit;
	if (used_pu_keyword && !is_pu) {
		throw ParserException("Table '" + metadata.table_name +
		                      "' is not a privacy unit. Use ALTER TABLE (without PU) for non-PU tables.");
	}
	if (!used_pu_keyword && is_pu && (has_privacy_link || has_protected)) {
		throw ParserException("Table '" + metadata.table_name +
		                      "' is a privacy unit. Use ALTER PU TABLE for PU tables.");
	}

	// Extract and merge new PAC clauses
	if (has_privacy_key) {
		vector<string> new_pk_cols;
		if (ExtractPACPrimaryKey(query, new_pk_cols)) {
			// Only add columns that don't already exist
			for (const auto &col : new_pk_cols) {
				if (std::find(metadata.primary_key_columns.begin(), metadata.primary_key_columns.end(), col) ==
				    metadata.primary_key_columns.end()) {
					metadata.primary_key_columns.push_back(col);
				}
			}
		}
		if (!is_pu) {
			Printer::Print("Note: PRIVACY_KEY added to non-PU table '" + metadata.table_name +
			               "'. This does not trigger PAC compilation until you run: ALTER TABLE " +
			               metadata.table_name + " SET PU");
		}
	}

	// Extract PAC LINK clauses
	if (has_privacy_link) {
		std::regex link_regex(R"(privacy_link\s*\([^)]+\)\s+references\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\([^)]+\))",
		                      std::regex_constants::icase);
		auto begin = std::sregex_iterator(query.begin(), query.end(), link_regex);
		auto end = std::sregex_iterator();
		idx_t new_links_count = 0;
		for (auto it = begin; it != end; ++it) {
			PACLink link;
			if (ExtractPACLink((*it).str(), link)) {
				new_links_count++;
				// Check if a link already exists on these local columns (to a different table or columns)
				for (const auto &existing_link : metadata.links) {
					if (existing_link.local_columns == link.local_columns) {
						// Check if it's exactly the same link (same target table and columns)
						if (StringUtil::Lower(existing_link.referenced_table) ==
						        StringUtil::Lower(link.referenced_table) &&
						    existing_link.referenced_columns == link.referenced_columns) {
							// This is the exact same link - skip it (idempotent)
							continue;
						}
						// Different target - this is an error
						throw ParserException(
						    "Column(s) already have a PRIVACY_LINK defined. A column or set of columns "
						    "can only reference one target.");
					}
				}

				// Add the link if it doesn't conflict
				bool link_exists = false;
				for (const auto &existing_link : metadata.links) {
					if (existing_link.local_columns == link.local_columns &&
					    StringUtil::Lower(existing_link.referenced_table) == StringUtil::Lower(link.referenced_table) &&
					    existing_link.referenced_columns == link.referenced_columns) {
						link_exists = true;
						break;
					}
				}
				if (!link_exists) {
					metadata.links.push_back(link);
				}
			}
		}

		// Check for PRIVACY_LINK keyword present but no valid link extracted (malformed syntax)
		if (new_links_count == 0) {
			throw ParserException("Invalid PRIVACY_LINK syntax. Use PRIVACY_LINK (col) REFERENCES table(ref_col). "
			                      "Example: CREATE TABLE t (id INT, fk INT, PRIVACY_LINK (fk) REFERENCES "
			                      "other_table(id), PROTECTED (id))");
		}
	}

	// Extract protected columns
	if (has_protected) {
		vector<string> new_protected_cols;
		if (ExtractProtectedColumns(query, new_protected_cols)) {
			// Check for duplicates in the new protected columns being added
			for (size_t i = 0; i < new_protected_cols.size(); i++) {
				for (size_t j = i + 1; j < new_protected_cols.size(); j++) {
					if (StringUtil::Lower(new_protected_cols[i]) == StringUtil::Lower(new_protected_cols[j])) {
						throw ParserException("Duplicate protected column '" + new_protected_cols[i] +
						                      "' in ALTER PU TABLE statement");
					}
				}
			}

			// Only add columns that don't already exist
			for (const auto &col : new_protected_cols) {
				bool already_protected = false;
				for (const auto &existing_col : metadata.protected_columns) {
					if (StringUtil::Lower(existing_col) == StringUtil::Lower(col)) {
						already_protected = true;
						break;
					}
				}
				if (already_protected) {
					throw ParserException("Column '" + col + "' is already marked as protected");
				}
				metadata.protected_columns.push_back(col);
			}
		}
	}

	// For ALTER PU TABLE, we only update metadata, no actual DDL execution needed
	stripped_sql = "";

	return true;
}

/**
 * ParseAlterTableDropPAC: Parses ALTER [PU] TABLE ... DROP ... statement
 *
 * Syntax:
 *   ALTER [PU] TABLE table_name DROP PRIVACY_LINK (col1);
 *   ALTER [PU] TABLE table_name DROP PROTECTED (col1, col2);
 *   ALTER TABLE table_name SET PU;
 *   ALTER TABLE table_name UNSET PU;
 *
 * This operation is metadata-only (no actual DDL executed). It removes
 * metadata entries from the table's metadata.
 *
 * Validation:
 *   - Table must have existing metadata
 *   - Columns/links must exist in metadata (can't drop non-existent entries)
 */
bool PrivacyParserExtension::ParseAlterTableDropPAC(const string &query, string &stripped_sql,
                                                    PrivacyTableMetadata &metadata) {
	string query_lower = StringUtil::Lower(query);

	// Check for ALTER TABLE ... SET PU (marks table as privacy unit)
	// Syntax: ALTER TABLE table_name SET PU;
	std::regex set_pu_regex(R"(alter\s+table\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+set\s+pu\s*$)");
	std::smatch match;
	if (std::regex_search(query_lower, match, set_pu_regex)) {
		metadata.table_name = match[1].str();

		// Get existing metadata if any, or create new
		auto existing = PrivacyMetadataManager::Get().GetTableMetadata(metadata.table_name);
		if (existing) {
			metadata = *existing;
		}

		// Mark as pending SET PU — validation deferred to plan time.
		// Parse-time check fails in batch mode (-c flag) where DuckDB parses all statements
		// before planning any: ADD PRIVACY_KEY's metadata isn't in PrivacyMetadataManager yet.
		metadata.is_set_pu_op = true;
		metadata.is_privacy_unit = true;

		// Metadata-only operation
		stripped_sql = "";
		return true;
	}

	// Check for ALTER TABLE ... UNSET PU (removes privacy unit status)
	// Syntax: ALTER TABLE table_name UNSET PU;
	std::regex unset_pu_regex(R"(alter\s+table\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+unset\s+pu\s*$)");
	if (std::regex_search(query_lower, match, unset_pu_regex)) {
		metadata.table_name = match[1].str();

		// Get existing metadata - it should exist
		auto existing = PrivacyMetadataManager::Get().GetTableMetadata(metadata.table_name);
		if (existing) {
			metadata = *existing;
		}
		metadata.is_privacy_unit = false;

		// Metadata-only operation
		stripped_sql = "";
		return true;
	}

	// Check if this is an ALTER [PU] TABLE ... DROP PRIVACY_LINK/PROTECTED statement
	// Both "ALTER PU TABLE t DROP ..." and "ALTER TABLE t DROP ..." are supported,
	// but the PU keyword must match the table's actual PU status.
	bool has_drop_link = query_lower.find("drop privacy_link") != string::npos;
	bool has_drop_protected = query_lower.find("drop protected") != string::npos;

	if (!has_drop_link && !has_drop_protected) {
		return false;
	}

	// Detect whether the user wrote ALTER PU TABLE or ALTER TABLE
	bool used_pu_keyword = (query_lower.find("alter pu table") != string::npos);

	// Extract table name - supports both "ALTER PU TABLE t" and "ALTER TABLE t"
	std::regex alter_drop_regex(R"(alter\s+(?:pu\s+)?table\s+([a-zA-Z_][a-zA-Z0-9_]*))");
	// Reuse 'match' variable declared earlier
	if (!std::regex_search(query_lower, match, alter_drop_regex)) {
		return false;
	}
	metadata.table_name = match[1].str();

	// Get existing metadata - it must exist for DROP operations
	auto existing = PrivacyMetadataManager::Get().GetTableMetadata(metadata.table_name);
	if (!existing) {
		throw ParserException("Table '" + metadata.table_name + "' does not have any PAC metadata to drop");
	}
	metadata = *existing;

	// Validate PU keyword matches table status
	if (used_pu_keyword && !metadata.is_privacy_unit) {
		throw ParserException("Table '" + metadata.table_name +
		                      "' is not a privacy unit. Use ALTER TABLE (without PU) for non-PU tables.");
	}
	if (!used_pu_keyword && metadata.is_privacy_unit) {
		throw ParserException("Table '" + metadata.table_name +
		                      "' is a privacy unit. Use ALTER PU TABLE for PU tables.");
	}

	// Handle DROP PAC LINK (col1, col2, ...)
	if (has_drop_link) {
		std::regex drop_link_regex(R"(drop\s+privacy_link\s*\(\s*([^)]+)\s*\))", std::regex_constants::icase);
		if (std::regex_search(query_lower, match, drop_link_regex)) {
			string cols_str = match[1].str();
			auto drop_cols = StringUtil::Split(cols_str, ',');
			vector<string> normalized_drop_cols;
			for (auto &col : drop_cols) {
				StringUtil::Trim(col);
				if (!col.empty()) {
					normalized_drop_cols.push_back(StringUtil::Lower(col));
				}
			}

			// Find and remove the link with these local columns
			bool found = false;
			for (auto it = metadata.links.begin(); it != metadata.links.end(); ++it) {
				// Normalize local columns for comparison
				vector<string> normalized_local_cols;
				for (const auto &col : it->local_columns) {
					normalized_local_cols.push_back(StringUtil::Lower(col));
				}

				if (normalized_local_cols == normalized_drop_cols) {
					metadata.links.erase(it);
					found = true;
					break;
				}
			}

			if (!found) {
				if (normalized_drop_cols.size() == 1) {
					throw ParserException("No PAC LINK found on column '" + normalized_drop_cols[0] + "'");
				} else {
					string cols_list;
					for (size_t i = 0; i < normalized_drop_cols.size(); i++) {
						if (i > 0) {
							cols_list += ", ";
						}
						cols_list += "'" + normalized_drop_cols[i] + "'";
					}
					throw ParserException("No PAC LINK found on columns (" + cols_list + ")");
				}
			}
		}
	}

	// Handle DROP PROTECTED (col1, col2, ...)
	if (has_drop_protected) {
		std::regex drop_protected_regex(R"(drop\s+protected\s*\(\s*([^)]+)\s*\))", std::regex_constants::icase);
		if (std::regex_search(query_lower, match, drop_protected_regex)) {
			string cols_str = match[1].str();
			auto drop_cols = StringUtil::Split(cols_str, ',');
			for (auto &col : drop_cols) {
				StringUtil::Trim(col);
				if (col.empty()) {
					continue;
				}

				// Find and remove the protected column
				bool found = false;
				for (auto it = metadata.protected_columns.begin(); it != metadata.protected_columns.end(); ++it) {
					if (StringUtil::Lower(*it) == StringUtil::Lower(col)) {
						metadata.protected_columns.erase(it);
						found = true;
						break;
					}
				}

				if (!found) {
					throw ParserException("Column '" + col + "' is not marked as protected");
				}
			}
		}
	}

	// For ALTER PU TABLE DROP, we only update metadata, no actual DDL execution needed
	stripped_sql = "";

	return true;
}

} // namespace duckdb
