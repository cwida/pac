//
// Test PAC Parser - JSON serialization and metadata management
//
#ifndef TEST_PAC_PARSER_HPP
#define TEST_PAC_PARSER_HPP

#include "duckdb.hpp"
#include "../../duckdb/third_party/catch/catch.hpp"

namespace duckdb {
class TestPACParser {
public:
	// Test JSON serialization of PAC table metadata
	static void TestJSONSerialization();
	// Test metadata manager add/get/clear operations
	static void TestMetadataManager();
	// Test saving and loading metadata from file
	static void TestFilePersistence();
	// Test parsing of CREATE PAC TABLE statements
	static void TestCreatePACTableParsing();
	// Test parsing of ALTER TABLE with PAC clauses
	static void TestAlterTablePACParsing();
	// Test regex patterns for PAC clauses
	static void TestRegexPatterns();
	// Run all tests
	static void RunAllTests();
};
} // namespace duckdb
#endif // TEST_PAC_PARSER_HPP
