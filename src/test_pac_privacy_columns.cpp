// filepath: /home/ila/Code/pac/src/pac_privacy_columns_test.cpp
// Small standalone test runner for FindPrimaryKey (pac_helpers)

#include <iostream>
#include <vector>
#include <string>
#include <algorithm>

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "include/pac_helpers.hpp"

using namespace duckdb;

static bool EqualVectors(vector<string> &a, vector<string> &b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i] != b[i]) return false;
    }
    return true;
}

int main() {
    DuckDB db(nullptr);
    Connection con(db);

    int failures = 0;

    try {
        // Test 1: no primary key
        con.Query("CREATE TABLE IF NOT EXISTS t_no_pk(a INTEGER, b INTEGER);");
        auto pk1 = FindPrimaryKey(*con.context, "t_no_pk");
        if (!pk1.empty()) {
            std::cerr << "FAIL: expected no PK for t_no_pk, got:";
            for (auto &c : pk1) std::cerr << " '" << c << "'";
            std::cerr << std::endl;
            failures++;
        } else {
            std::cerr << "PASS: t_no_pk has no PK\n";
        }

        // Test 2: single-column primary key
        con.Query("CREATE TABLE IF NOT EXISTS t_single_pk(id INTEGER PRIMARY KEY, val INTEGER);");
        auto pk2 = FindPrimaryKey(*con.context, "t_single_pk");
        vector<string> expect2 = {"id"};
        if (!EqualVectors(pk2, expect2)) {
            std::cerr << "FAIL: expected PK [id] for t_single_pk, got:";
            for (auto &c : pk2) std::cerr << " '" << c << "'";
            std::cerr << std::endl;
            failures++;
        } else {
            std::cerr << "PASS: t_single_pk PK==[id]\n";
        }

        // Test 3: multi-column primary key
        con.Query("CREATE TABLE IF NOT EXISTS t_multi_pk(a INTEGER, b INTEGER, c INTEGER, PRIMARY KEY(a, b));");
        auto pk3 = FindPrimaryKey(*con.context, "t_multi_pk");
        vector<string> expect3 = {"a", "b"};
        if (!EqualVectors(pk3, expect3)) {
            std::cerr << "FAIL: expected PK [a,b] for t_multi_pk, got:";
            for (auto &c : pk3) std::cerr << " '" << c << "'";
            std::cerr << std::endl;
            failures++;
        } else {
            std::cerr << "PASS: t_multi_pk PK==[a,b]\n";
        }

        // Test 4: schema-qualified lookup
        con.Query("CREATE SCHEMA IF NOT EXISTS myschema;");
        con.Query("CREATE TABLE IF NOT EXISTS myschema.t_schema_pk(x INTEGER PRIMARY KEY, y INTEGER);");
        auto pk4 = FindPrimaryKey(*con.context, string("myschema.t_schema_pk"));
        vector<string> expect4 = {"x"};
        if (!EqualVectors(pk4, expect4)) {
            std::cerr << "FAIL: expected PK [x] for myschema.t_schema_pk, got:";
            for (auto &c : pk4) std::cerr << " '" << c << "'";
            std::cerr << std::endl;
            failures++;
        } else {
            std::cerr << "PASS: myschema.t_schema_pk PK==[x]\n";
        }

    } catch (std::exception &ex) {
        std::cerr << "Exception during tests: " << ex.what() << std::endl;
        return 2;
    }

    if (failures == 0) {
        std::cerr << "ALL TESTS PASSED\n";
        return 0;
    } else {
        std::cerr << failures << " TEST(S) FAILED\n";
        return 1;
    }
}

