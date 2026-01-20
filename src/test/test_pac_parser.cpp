//
// Test PAC Parser - JSON serialization and metadata management
//
#include "include/test_pac_parser.hpp"
#include "../include/pac_parser.hpp"
#include <fstream>
#include <sstream>
#include <iostream>

namespace duckdb {

#define TEST_ASSERT(condition, message)                                                                                \
	do {                                                                                                               \
		if (!(condition)) {                                                                                            \
			std::cerr << "FAILED: " << message << std::endl;                                                           \
			std::cerr << "  at " << __FILE__ << ":" << __LINE__ << std::endl;                                          \
			throw std::runtime_error(message);                                                                         \
		}                                                                                                              \
	} while (0)

void TestPACParser::TestJSONSerialization() {
	// Create test metadata
	PACTableMetadata metadata("test_table");
	metadata.primary_key_columns = {"id", "tenant_id"};
	metadata.links.push_back(PACLink("user_id", "users", "id"));
	metadata.links.push_back(PACLink("org_id", "organizations", "id"));
	metadata.protected_columns = {"salary", "ssn", "email"};
	// Serialize to JSON
	string json = PACMetadataManager::SerializeToJSON(metadata);
	// Verify JSON contains expected fields
	TEST_ASSERT(json.find("\"table_name\": \"test_table\"") != string::npos, "JSON should contain table_name");
	TEST_ASSERT(json.find("\"id\"") != string::npos, "JSON should contain id");
	TEST_ASSERT(json.find("\"tenant_id\"") != string::npos, "JSON should contain tenant_id");
	TEST_ASSERT(json.find("\"user_id\"") != string::npos, "JSON should contain user_id");
	TEST_ASSERT(json.find("\"users\"") != string::npos, "JSON should contain users");
	TEST_ASSERT(json.find("\"salary\"") != string::npos, "JSON should contain salary");
	// Deserialize from JSON
	PACTableMetadata deserialized = PACMetadataManager::DeserializeFromJSON(json);
	// Verify deserialized data matches original
	TEST_ASSERT(deserialized.table_name == "test_table", "Deserialized table_name should match");
	TEST_ASSERT(deserialized.primary_key_columns.size() == 2, "Deserialized should have 2 PK columns");
	TEST_ASSERT(deserialized.primary_key_columns[0] == "id", "First PK should be id");
	TEST_ASSERT(deserialized.primary_key_columns[1] == "tenant_id", "Second PK should be tenant_id");
	TEST_ASSERT(deserialized.links.size() == 2, "Deserialized should have 2 links");
	TEST_ASSERT(deserialized.links[0].local_column == "user_id", "First link local column should be user_id");
	TEST_ASSERT(deserialized.links[0].referenced_table == "users", "First link referenced table should be users");
	TEST_ASSERT(deserialized.links[0].referenced_column == "id", "First link referenced column should be id");
	TEST_ASSERT(deserialized.protected_columns.size() == 3, "Deserialized should have 3 protected columns");
	TEST_ASSERT(deserialized.protected_columns[0] == "salary", "First protected column should be salary");
}

void TestPACParser::TestMetadataManager() {
	auto &manager = PACMetadataManager::Get();
	manager.Clear();
	// Test adding metadata
	PACTableMetadata metadata1("table1");
	metadata1.primary_key_columns = {"id"};
	manager.AddOrUpdateTable("table1", metadata1);
	// Test getting metadata
	TEST_ASSERT(manager.HasMetadata("table1"), "Manager should have table1");
	auto *retrieved = manager.GetTableMetadata("table1");
	TEST_ASSERT(retrieved != nullptr, "Retrieved metadata should not be null");
	TEST_ASSERT(retrieved->table_name == "table1", "Retrieved table name should be table1");
	TEST_ASSERT(retrieved->primary_key_columns.size() == 1, "Retrieved should have 1 PK column");
	// Test non-existent table
	TEST_ASSERT(!manager.HasMetadata("nonexistent"), "Manager should not have nonexistent table");
	TEST_ASSERT(manager.GetTableMetadata("nonexistent") == nullptr, "Nonexistent table should return null");
	// Test adding another table
	PACTableMetadata metadata2("table2");
	metadata2.protected_columns = {"col1", "col2"};
	manager.AddOrUpdateTable("table2", metadata2);
	TEST_ASSERT(manager.HasMetadata("table2"), "Manager should have table2");
	// Test updating existing table
	metadata1.links.push_back(PACLink("fk", "table2", "id"));
	manager.AddOrUpdateTable("table1", metadata1);
	retrieved = manager.GetTableMetadata("table1");
	TEST_ASSERT(retrieved->links.size() == 1, "Updated table should have 1 link");
	// Test clear
	manager.Clear();
	TEST_ASSERT(!manager.HasMetadata("table1"), "Manager should not have table1 after clear");
	TEST_ASSERT(!manager.HasMetadata("table2"), "Manager should not have table2 after clear");
}

void TestPACParser::TestFilePersistence() {
	auto &manager = PACMetadataManager::Get();
	manager.Clear();
	// Create test metadata
	PACTableMetadata metadata1("users");
	metadata1.primary_key_columns = {"user_id"};
	metadata1.protected_columns = {"email", "password"};
	manager.AddOrUpdateTable("users", metadata1);
	PACTableMetadata metadata2("orders");
	metadata2.primary_key_columns = {"order_id"};
	metadata2.links.push_back(PACLink("user_id", "users", "user_id"));
	manager.AddOrUpdateTable("orders", metadata2);
	// Save to file
	string filepath = "/tmp/test_pac_metadata.json";
	manager.SaveToFile(filepath);
	// Verify file exists and contains expected content
	std::ifstream file(filepath);
	TEST_ASSERT(file.is_open(), "File should be open");
	std::stringstream buffer;
	buffer << file.rdbuf();
	file.close();
	string content = buffer.str();
	TEST_ASSERT(content.find("\"users\"") != string::npos, "File content should contain users");
	TEST_ASSERT(content.find("\"orders\"") != string::npos, "File content should contain orders");
	TEST_ASSERT(content.find("\"email\"") != string::npos, "File content should contain email");
	// Clear and reload
	manager.Clear();
	TEST_ASSERT(!manager.HasMetadata("users"), "Manager should not have users after clear");
	manager.LoadFromFile(filepath);
	// Verify data was loaded correctly
	TEST_ASSERT(manager.HasMetadata("users"), "Manager should have users after load");
	TEST_ASSERT(manager.HasMetadata("orders"), "Manager should have orders after load");
	auto *users = manager.GetTableMetadata("users");
	TEST_ASSERT(users != nullptr, "Users metadata should not be null");
	TEST_ASSERT(users->primary_key_columns.size() == 1, "Users should have 1 PK column");
	TEST_ASSERT(users->protected_columns.size() == 2, "Users should have 2 protected columns");
	auto *orders = manager.GetTableMetadata("orders");
	TEST_ASSERT(orders != nullptr, "Orders metadata should not be null");
	TEST_ASSERT(orders->links.size() == 1, "Orders should have 1 link");
	TEST_ASSERT(orders->links[0].referenced_table == "users", "Orders link should reference users");
	// Cleanup
	std::remove(filepath.c_str());
	manager.Clear();
}

void TestPACParser::TestCreatePACTableParsing() {
	// Test basic CREATE PAC TABLE
	string sql1 = "CREATE PAC TABLE users (id INTEGER, name VARCHAR, PAC KEY (id))";
	PACTableMetadata metadata1;
	string stripped1;
	bool result1 = PACParserExtension::ParseCreatePACTable(sql1, stripped1, metadata1);
	TEST_ASSERT(result1, "Parse should succeed");
	TEST_ASSERT(metadata1.table_name == "users", "Table name should be users");
	TEST_ASSERT(metadata1.primary_key_columns.size() == 1, "Should have 1 PK column");
	TEST_ASSERT(metadata1.primary_key_columns[0] == "id", "PK column should be id");
	TEST_ASSERT(stripped1.find("CREATE TABLE") != string::npos, "Stripped SQL should contain CREATE TABLE");
	TEST_ASSERT(stripped1.find("PAC KEY") == string::npos, "Stripped SQL should not contain PAC KEY");
	// Test with PAC LINK
	string sql2 = "CREATE PAC TABLE orders (id INTEGER, user_id INTEGER, PAC LINK (user_id) REFERENCES users(id))";
	PACTableMetadata metadata2;
	string stripped2;
	bool result2 = PACParserExtension::ParseCreatePACTable(sql2, stripped2, metadata2);
	TEST_ASSERT(result2, "Parse should succeed");
	TEST_ASSERT(metadata2.table_name == "orders", "Table name should be orders");
	TEST_ASSERT(metadata2.links.size() == 1, "Should have 1 link");
	TEST_ASSERT(metadata2.links[0].local_column == "user_id", "Link local column should be user_id");
	TEST_ASSERT(metadata2.links[0].referenced_table == "users", "Link referenced table should be users");
	TEST_ASSERT(metadata2.links[0].referenced_column == "id", "Link referenced column should be id");
	// Test with PROTECTED
	string sql3 = "CREATE PAC TABLE sensitive (id INTEGER, ssn VARCHAR, PROTECTED (ssn))";
	PACTableMetadata metadata3;
	string stripped3;
	bool result3 = PACParserExtension::ParseCreatePACTable(sql3, stripped3, metadata3);
	TEST_ASSERT(result3, "Parse should succeed");
	TEST_ASSERT(metadata3.table_name == "sensitive", "Table name should be sensitive");
	TEST_ASSERT(metadata3.protected_columns.size() == 1, "Should have 1 protected column");
	TEST_ASSERT(metadata3.protected_columns[0] == "ssn", "Protected column should be ssn");
	// Test with multiple clauses
	string sql4 = "CREATE PAC TABLE employees (emp_id INTEGER, dept_id INTEGER, salary INTEGER, "
	              "PAC KEY (emp_id), PAC LINK (dept_id) REFERENCES departments(id), "
	              "PROTECTED (salary))";
	PACTableMetadata metadata4;
	string stripped4;
	bool result4 = PACParserExtension::ParseCreatePACTable(sql4, stripped4, metadata4);
	TEST_ASSERT(result4, "Parse should succeed");
	TEST_ASSERT(metadata4.table_name == "employees", "Table name should be employees");
	TEST_ASSERT(metadata4.primary_key_columns.size() == 1, "Should have 1 PK column");
	TEST_ASSERT(metadata4.links.size() == 1, "Should have 1 link");
	TEST_ASSERT(metadata4.protected_columns.size() == 1, "Should have 1 protected column");
}

void TestPACParser::TestAlterTablePACParsing() {
	auto &manager = PACMetadataManager::Get();
	manager.Clear();
	// First create a table with initial metadata
	PACTableMetadata initial("products");
	initial.primary_key_columns = {"product_id"};
	manager.AddOrUpdateTable("products", initial);
	// Test ALTER PAC TABLE ADD PAC LINK
	string sql1 = "ALTER PAC TABLE products ADD PAC LINK (category_id) REFERENCES categories(id)";
	PACTableMetadata metadata1;
	string stripped1;
	bool result1 = PACParserExtension::ParseAlterTableAddPAC(sql1, stripped1, metadata1);
	TEST_ASSERT(result1, "Parse should succeed");
	TEST_ASSERT(metadata1.table_name == "products", "Table name should be products");
	TEST_ASSERT(metadata1.primary_key_columns.size() == 1, "Should preserve existing PK");
	TEST_ASSERT(metadata1.links.size() == 1, "Should have 1 link");
	TEST_ASSERT(metadata1.links[0].local_column == "category_id", "Link local column should be category_id");
	// Test ALTER PAC TABLE ADD PROTECTED
	string sql2 = "ALTER PAC TABLE products ADD PROTECTED (price, cost)";
	PACTableMetadata metadata2;
	string stripped2;
	bool result2 = PACParserExtension::ParseAlterTableAddPAC(sql2, stripped2, metadata2);
	TEST_ASSERT(result2, "Parse should succeed");
	TEST_ASSERT(metadata2.table_name == "products", "Table name should be products");
	TEST_ASSERT(metadata2.protected_columns.size() == 2, "Should have 2 protected columns");
	manager.Clear();
}

void TestPACParser::TestRegexPatterns() {
	// Test PAC KEY extraction
	string clause1 = "PAC KEY (id, tenant_id)";
	vector<string> pk_cols;
	bool result1 = PACParserExtension::ExtractPACPrimaryKey(clause1, pk_cols);
	TEST_ASSERT(result1, "Extract should succeed");
	TEST_ASSERT(pk_cols.size() == 2, "Should have 2 PK columns");
	TEST_ASSERT(pk_cols[0] == "id", "First PK should be id");
	TEST_ASSERT(pk_cols[1] == "tenant_id", "Second PK should be tenant_id");
	// Test PAC LINK extraction
	string clause2 = "PAC LINK (user_id) REFERENCES users(id)";
	PACLink link;
	bool result2 = PACParserExtension::ExtractPACLink(clause2, link);
	TEST_ASSERT(result2, "Extract should succeed");
	TEST_ASSERT(link.local_column == "user_id", "Local column should be user_id");
	TEST_ASSERT(link.referenced_table == "users", "Referenced table should be users");
	TEST_ASSERT(link.referenced_column == "id", "Referenced column should be id");
	// Test PROTECTED extraction
	string clause3 = "PROTECTED (salary, ssn, email)";
	vector<string> protected_cols;
	bool result3 = PACParserExtension::ExtractProtectedColumns(clause3, protected_cols);
	TEST_ASSERT(result3, "Extract should succeed");
	TEST_ASSERT(protected_cols.size() == 3, "Should have 3 protected columns");
	TEST_ASSERT(protected_cols[0] == "salary", "First protected column should be salary");
	TEST_ASSERT(protected_cols[1] == "ssn", "Second protected column should be ssn");
	TEST_ASSERT(protected_cols[2] == "email", "Third protected column should be email");
	// Test table name extraction - CREATE
	string sql1 = "CREATE PAC TABLE test_table (id INTEGER)";
	string table1 = PACParserExtension::ExtractTableName(sql1, true);
	TEST_ASSERT(table1 == "test_table", "Table name should be test_table");
	// Test table name extraction - ALTER
	string sql2 = "ALTER TABLE my_table ADD COLUMN col VARCHAR";
	string table2 = PACParserExtension::ExtractTableName(sql2, false);
	TEST_ASSERT(table2 == "my_table", "Table name should be my_table");
}

void TestPACParser::RunAllTests() {
	std::cout << "Running TestJSONSerialization..." << std::endl;
	TestJSONSerialization();
	std::cout << "PASSED: TestJSONSerialization" << std::endl;

	std::cout << "Running TestMetadataManager..." << std::endl;
	TestMetadataManager();
	std::cout << "PASSED: TestMetadataManager" << std::endl;

	std::cout << "Running TestFilePersistence..." << std::endl;
	TestFilePersistence();
	std::cout << "PASSED: TestFilePersistence" << std::endl;

	std::cout << "Running TestCreatePACTableParsing..." << std::endl;
	TestCreatePACTableParsing();
	std::cout << "PASSED: TestCreatePACTableParsing" << std::endl;

	std::cout << "Running TestAlterTablePACParsing..." << std::endl;
	TestAlterTablePACParsing();
	std::cout << "PASSED: TestAlterTablePACParsing" << std::endl;

	std::cout << "Running TestRegexPatterns..." << std::endl;
	TestRegexPatterns();
	std::cout << "PASSED: TestRegexPatterns" << std::endl;

	std::cout << "\nAll tests passed!" << std::endl;
}

} // namespace duckdb
