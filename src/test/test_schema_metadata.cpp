// Test runner for schema metadata functions (FindPacKey, FindPacLinks, FindPacLinkPath)

#include <iostream>

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "../include/utils/privacy_helpers.hpp"
#include "include/test_schema_metadata.hpp"
#include "parser/privacy_parser.hpp"

namespace duckdb {

static bool EqualVectors(vector<string> &a, vector<string> &b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (size_t i = 0; i < a.size(); ++i) {
		if (a[i] != b[i]) {
			return false;
		}
	}
	return true;
}

// Helper to strip schema qualification if present
static string Basename(const string &qname) {
	auto pos = qname.rfind('.');
	if (pos == string::npos) {
		return qname;
	}
	return qname.substr(pos + 1);
}

int RunSchemaMetadataTests() {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();

	auto &manager = PrivacyMetadataManager::Get();
	manager.Clear();

	int failures = 0;

	try {
		std::cerr << "=== Testing FindPacKey ===\n";

		// Test 1: no primary key
		con.Query("CREATE TABLE IF NOT EXISTS t_no_pk(a INTEGER, b INTEGER);");
		auto pk1 = FindPacKey(*con.context, "t_no_pk");
		if (!pk1.empty()) {
			std::cerr << "FAIL: expected no PK for t_no_pk\n";
			failures++;
		} else {
			std::cerr << "PASS: t_no_pk has no PK\n";
		}

		// Test 2: single-column primary key (via PAC metadata)
		con.Query("CREATE TABLE IF NOT EXISTS t_single_pk(id INTEGER, val INTEGER);");
		{
			PrivacyTableMetadata meta("t_single_pk");
			meta.primary_key_columns = {"id"};
			manager.AddOrUpdateTable("t_single_pk", meta);
		}
		auto pk2 = FindPacKey(*con.context, "t_single_pk");
		vector<string> expect2 = {"id"};
		if (!EqualVectors(pk2, expect2)) {
			std::cerr << "FAIL: expected PK [id] for t_single_pk\n";
			failures++;
		} else {
			std::cerr << "PASS: t_single_pk PK==[id]\n";
		}

		// Test 3: multi-column primary key (via PAC metadata)
		con.Query("CREATE TABLE IF NOT EXISTS t_multi_pk(a INTEGER, b INTEGER, c INTEGER);");
		{
			PrivacyTableMetadata meta("t_multi_pk");
			meta.primary_key_columns = {"a", "b"};
			manager.AddOrUpdateTable("t_multi_pk", meta);
		}
		auto pk3 = FindPacKey(*con.context, "t_multi_pk");
		vector<string> expect3 = {"a", "b"};
		if (!EqualVectors(pk3, expect3)) {
			std::cerr << "FAIL: expected PK [a,b] for t_multi_pk\n";
			failures++;
		} else {
			std::cerr << "PASS: t_multi_pk PK==[a,b]\n";
		}

		// Test 4: schema-qualified lookup (via PAC metadata with unqualified name)
		con.Query("CREATE SCHEMA IF NOT EXISTS myschema;");
		con.Query("CREATE TABLE IF NOT EXISTS myschema.t_schema_pk(x INTEGER, y INTEGER);");
		{
			PrivacyTableMetadata meta("t_schema_pk");
			meta.primary_key_columns = {"x"};
			manager.AddOrUpdateTable("t_schema_pk", meta);
		}
		auto pk4 = FindPacKey(*con.context, string("myschema.t_schema_pk"));
		vector<string> expect4 = {"x"};
		if (!EqualVectors(pk4, expect4)) {
			std::cerr << "FAIL: expected PK [x] for myschema.t_schema_pk\n";
			failures++;
		} else {
			std::cerr << "PASS: myschema.t_schema_pk PK==[x]\n";
		}

		// Test 5: string (TEXT) primary key should be ignored
		con.Query("CREATE TABLE IF NOT EXISTS t_string_pk(id TEXT, val INTEGER);");
		{
			PrivacyTableMetadata meta("t_string_pk");
			meta.primary_key_columns = {"id"};
			manager.AddOrUpdateTable("t_string_pk", meta);
		}
		auto pk5 = FindPacKey(*con.context, "t_string_pk");
		if (!pk5.empty()) {
			std::cerr << "FAIL: expected no PK for t_string_pk (TEXT PK should be ignored)\n";
			failures++;
		} else {
			std::cerr << "PASS: t_string_pk TEXT PK correctly ignored\n";
		}

		// Test 6: composite primary key with mixed types should be treated as no PK
		con.Query("CREATE TABLE IF NOT EXISTS t_mixed_pk(a INTEGER, b TEXT);");
		{
			PrivacyTableMetadata meta("t_mixed_pk");
			meta.primary_key_columns = {"a", "b"};
			manager.AddOrUpdateTable("t_mixed_pk", meta);
		}
		auto pk6 = FindPacKey(*con.context, "t_mixed_pk");
		if (!pk6.empty()) {
			std::cerr << "FAIL: expected no PK for t_mixed_pk\n";
			failures++;
		} else {
			std::cerr << "PASS: t_mixed_pk composite mixed-type PK correctly treated as no PK\n";
		}

		std::cerr << "\n=== Testing FindPacLinkPath ===\n";

		// Test 7: transitive FK detection via PRIVACY_LINK (t_a -> t_b -> t_c)
		con.Query("CREATE TABLE IF NOT EXISTS t_c(id INTEGER, val INTEGER);");
		con.Query("CREATE TABLE IF NOT EXISTS t_b(id INTEGER, cid INTEGER);");
		con.Query("CREATE TABLE IF NOT EXISTS t_a(id INTEGER, bid INTEGER);");
		con.Query("CREATE TABLE IF NOT EXISTS t_unrelated(x INTEGER);");
		{
			PrivacyTableMetadata meta_c("t_c");
			meta_c.primary_key_columns = {"id"};
			meta_c.is_privacy_unit = true;
			manager.AddOrUpdateTable("t_c", meta_c);

			PrivacyTableMetadata meta_b("t_b");
			meta_b.primary_key_columns = {"id"};
			meta_b.links.push_back(PACLink("cid", "t_c", "id"));
			manager.AddOrUpdateTable("t_b", meta_b);

			PrivacyTableMetadata meta_a("t_a");
			meta_a.primary_key_columns = {"id"};
			meta_a.links.push_back(PACLink("bid", "t_b", "id"));
			manager.AddOrUpdateTable("t_a", meta_a);
		}

		auto privacy_units = vector<string> {"t_c"};
		auto table_names = vector<string> {"t_a", "t_b", "t_unrelated"};
		auto paths = FindPacLinkPath(*con.context, privacy_units, table_names);

		// Expect t_a -> [t_a, t_b, t_c]
		auto it_a = paths.find("t_a");
		if (it_a == paths.end()) {
			std::cerr << "FAIL: expected path for t_a but none found\n";
			failures++;
		} else {
			auto &path = it_a->second;
			if (path.size() != 3 || Basename(path[0]) != "t_a" || Basename(path[1]) != "t_b" ||
			    Basename(path[2]) != "t_c") {
				std::cerr << "FAIL: unexpected path for t_a: [";
				for (auto &p : path) {
					std::cerr << p << " ";
				}
				std::cerr << "]\n";
				failures++;
			} else {
				std::cerr << "PASS: t_a -> t_b -> t_c detected\n";
			}
		}

		// Expect t_b -> [t_b, t_c]
		auto it_b = paths.find("t_b");
		if (it_b == paths.end()) {
			std::cerr << "FAIL: expected path for t_b but none found\n";
			failures++;
		} else {
			auto &path = it_b->second;
			if (path.size() != 2 || Basename(path[0]) != "t_b" || Basename(path[1]) != "t_c") {
				std::cerr << "FAIL: unexpected path for t_b: [";
				for (auto &p : path) {
					std::cerr << p << " ";
				}
				std::cerr << "]\n";
				failures++;
			} else {
				std::cerr << "PASS: t_b -> t_c detected\n";
			}
		}

		// Expect t_unrelated not present
		if (paths.find("t_unrelated") != paths.end()) {
			std::cerr << "FAIL: unexpected path found for t_unrelated\n";
			failures++;
		} else {
			std::cerr << "PASS: no path for t_unrelated\n";
		}

		// Test 8: long FK chain via PRIVACY_LINK
		con.Query("CREATE TABLE IF NOT EXISTS t_c_long(id INTEGER);");
		con.Query("CREATE TABLE IF NOT EXISTS t_long_2(id INTEGER, cid INTEGER);");
		con.Query("CREATE TABLE IF NOT EXISTS t_long_1(id INTEGER, n2 INTEGER);");
		con.Query("CREATE TABLE IF NOT EXISTS t_long_0(id INTEGER, n1 INTEGER);");
		{
			PrivacyTableMetadata meta_cl("t_c_long");
			meta_cl.primary_key_columns = {"id"};
			meta_cl.is_privacy_unit = true;
			manager.AddOrUpdateTable("t_c_long", meta_cl);

			PrivacyTableMetadata meta_l2("t_long_2");
			meta_l2.primary_key_columns = {"id"};
			meta_l2.links.push_back(PACLink("cid", "t_c_long", "id"));
			manager.AddOrUpdateTable("t_long_2", meta_l2);

			PrivacyTableMetadata meta_l1("t_long_1");
			meta_l1.primary_key_columns = {"id"};
			meta_l1.links.push_back(PACLink("n2", "t_long_2", "id"));
			manager.AddOrUpdateTable("t_long_1", meta_l1);

			PrivacyTableMetadata meta_l0("t_long_0");
			meta_l0.primary_key_columns = {"id"};
			meta_l0.links.push_back(PACLink("n1", "t_long_1", "id"));
			manager.AddOrUpdateTable("t_long_0", meta_l0);
		}

		auto privacy_long = vector<string> {"t_c_long"};
		auto start_long = vector<string> {"t_long_0"};
		auto paths_long = FindPacLinkPath(*con.context, privacy_long, start_long);
		auto it_long = paths_long.find("t_long_0");

		if (it_long == paths_long.end()) {
			std::cerr << "FAIL: expected path for t_long_0 but none found\n";
			failures++;
		} else {
			auto &path = it_long->second;
			if (path.size() != 4) {
				std::cerr << "FAIL: expected path length 4 for t_long_0, got " << path.size() << "\n";
				failures++;
			} else {
				std::cerr << "PASS: long path t_long_0 -> ... -> t_c_long detected\n";
			}
		}

		// Test 9: no FK path exists
		con.Query("CREATE TABLE IF NOT EXISTS t_unrelated2(id INTEGER);");
		auto privacy_none = vector<string> {"t_c"};
		auto starts_none = vector<string> {"t_unrelated2"};
		auto paths_none = FindPacLinkPath(*con.context, privacy_none, starts_none);

		if (paths_none.find("t_unrelated2") != paths_none.end()) {
			std::cerr << "FAIL: unexpected path found for t_unrelated2\n";
			failures++;
		} else {
			std::cerr << "PASS: no path for t_unrelated2\n";
		}

	} catch (std::exception &ex) {
		con.Rollback();
		manager.Clear();
		std::cerr << "Exception during tests: " << ex.what() << "\n";
		return 2;
	}

	con.Rollback();
	manager.Clear();

	if (failures == 0) {
		std::cerr << "\n=== ALL SCHEMA METADATA TESTS PASSED ===\n";
		return 0;
	} else {
		std::cerr << "\n=== " << failures << " SCHEMA METADATA TEST(S) FAILED ===\n";
		return 1;
	}
}

} // namespace duckdb
