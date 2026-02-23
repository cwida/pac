//
// SQLStorm TPC-H benchmark runner for DuckDB.
// Two-pass design: runs all SQLStorm queries first without PAC (baseline),
// then loads the PAC schema and runs them again. Reports statistics for both.
//

#include "pac_sqlstorm_benchmark.hpp"

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/printer.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <ctime>
#include <iomanip>
#include <thread>
#include <atomic>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>

namespace duckdb {

static string Timestamp() {
	auto now = std::chrono::system_clock::now();
	std::time_t t = std::chrono::system_clock::to_time_t(now);
	char buf[64];
	std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
	return string(buf);
}

static void Log(const string &msg) {
	Printer::Print("[" + Timestamp() + "] " + msg);
}

static string FormatNumber(double v) {
	std::ostringstream oss;
	oss << std::setprecision(5) << std::defaultfloat << v;
	string s = oss.str();
	auto pos = s.find('.');
	if (pos != string::npos) {
		while (!s.empty() && s.back() == '0') { s.pop_back(); }
		if (!s.empty() && s.back() == '.') { s.pop_back(); }
	}
	return s;
}

static bool FileExists(const string &path) {
	struct stat buffer;
	return (stat(path.c_str(), &buffer) == 0);
}

static string ReadFileToString(const string &path) {
	std::ifstream in(path);
	if (!in.is_open()) {
		return string();
	}
	std::ostringstream ss;
	ss << in.rdbuf();
	return ss.str();
}

// Collect all .sql files from a directory
static vector<string> CollectQueryFiles(const string &dir) {
	vector<string> files;
	DIR *d = opendir(dir.c_str());
	if (!d) {
		return files;
	}
	struct dirent *entry;
	while ((entry = readdir(d)) != nullptr) {
		string name = entry->d_name;
		if (name.size() > 4 && name.substr(name.size() - 4) == ".sql") {
			files.push_back(dir + "/" + name);
		}
	}
	closedir(d);
	std::sort(files.begin(), files.end());
	return files;
}

// Extract query name from path (e.g. "/path/to/10001.sql" -> "10001")
static string QueryName(const string &path) {
	auto pos = path.find_last_of('/');
	string fname = (pos != string::npos) ? path.substr(pos + 1) : path;
	if (fname.size() > 4) {
		fname = fname.substr(0, fname.size() - 4);
	}
	return fname;
}

static string FindSchemaFile(const string &filename) {
	vector<string> candidates = {
		filename,
		"./" + filename,
		"../" + filename,
		"../../" + filename,
		"benchmark/" + filename,
		"benchmark/tpch/" + filename,
		"../benchmark/" + filename,
		"../benchmark/tpch/" + filename,
		"../../benchmark/" + filename,
		"../../benchmark/tpch/" + filename,
	};

	for (auto &cand : candidates) {
		if (FileExists(cand)) {
			return cand;
		}
	}

	// Try relative to executable
	char exe_path[PATH_MAX];
	ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
	if (len != -1) {
		exe_path[len] = '\0';
		string dir = string(exe_path);
		auto pos = dir.find_last_of('/');
		if (pos != string::npos) {
			dir = dir.substr(0, pos);
		}

		for (int i = 0; i < 6; ++i) {
			string cand = dir + "/benchmark/tpch/" + filename;
			if (FileExists(cand)) {
				return cand;
			}
			auto p2 = dir.find_last_of('/');
			if (p2 == string::npos) break;
			dir = dir.substr(0, p2);
		}
	}

	return "";
}

// Try to find the SQLStorm queries directory
static string FindQueriesDir() {
	vector<string> candidates = {
		"SQLStorm/v1.0/tpch/queries",
	    "benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	    "../benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	    "../../benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	    "../../../benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	};

	for (auto &c : candidates) {
		if (FileExists(c)) {
			return c;
		}
	}

	// Try relative to executable
	char exe_path[PATH_MAX];
	ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
	if (len != -1) {
		exe_path[len] = '\0';
		string dir = string(exe_path);
		auto pos = dir.find_last_of('/');
		if (pos != string::npos) {
			dir = dir.substr(0, pos);
		}
		for (int i = 0; i < 6; ++i) {
			string cand = dir + "/benchmark/SQLStorm/v1.0/tpch/queries";
			if (FileExists(cand)) {
				return cand;
			}
			auto p2 = dir.find_last_of('/');
			if (p2 == string::npos) break;
			dir = dir.substr(0, p2);
		}
	}

	return "";
}

struct BenchmarkQueryResult {
	string name;
	string state;   // "success", "error", "timeout", "crash"
	double time_ms;
	int64_t rows;
	string error;
	bool pac_applied; // true if EXPLAIN output contains pac_ functions
};

struct PassStats {
	int success = 0;
	int failed = 0;
	int timed_out = 0;
	int crashed = 0;
	int pac_applied = 0;
	double total_success_time = 0;
};

// Load the PAC TPC-H schema by executing each statement individually
static void LoadPACSchema(Connection &con) {
	string schema_file = FindSchemaFile("pac_tpch_schema.sql");
	if (schema_file.empty()) {
		throw std::runtime_error("Cannot find pac_tpch_schema.sql");
	}

	Log("Loading PAC schema from: " + schema_file);
	string schema_sql = ReadFileToString(schema_file);
	if (schema_sql.empty()) {
		throw std::runtime_error("PAC schema file is empty: " + schema_file);
	}

	// Execute statement-by-statement (PAC parser needs individual ALTER statements)
	std::istringstream sql_stream(schema_sql);
	string line;
	string current_statement;

	while (std::getline(sql_stream, line)) {
		string trimmed = line;
		size_t start = trimmed.find_first_not_of(" \t\r\n");
		if (start == string::npos) continue;
		trimmed = trimmed.substr(start);
		if (trimmed.empty() || trimmed.substr(0, 2) == "--") continue;

		current_statement += line + " ";

		if (trimmed.find(';') != string::npos) {
			auto result = con.Query(current_statement);
			if (result->HasError()) {
				throw std::runtime_error("PAC schema error: " + result->GetError() +
				                         "\nStatement: " + current_statement);
			}
			current_statement.clear();
		}
	}

	if (!current_statement.empty()) {
		string trimmed = current_statement;
		size_t start = trimmed.find_first_not_of(" \t\r\n");
		if (start != string::npos) {
			auto result = con.Query(current_statement);
			if (result->HasError()) {
				throw std::runtime_error("PAC schema error: " + result->GetError());
			}
		}
	}

	Log("PAC schema loaded successfully");
}

// Run a single query with timeout, returning the result
static BenchmarkQueryResult RunQuery(Connection &con, const string &name, const string &sql, double timeout_s) {
	BenchmarkQueryResult qr;
	qr.name = name;
	qr.rows = 0;
	qr.pac_applied = false;

	try {
		unique_ptr<MaterializedQueryResult> result;
		std::atomic<bool> query_done {false};
		std::exception_ptr query_exception;

		auto start = std::chrono::steady_clock::now();
		std::thread query_thread([&]() {
			try {
				result = con.Query(sql);
			} catch (...) {
				query_exception = std::current_exception();
			}
			query_done.store(true, std::memory_order_release);
		});

		auto deadline = start + std::chrono::duration<double>(timeout_s);
		while (!query_done.load(std::memory_order_acquire)) {
			if (std::chrono::steady_clock::now() >= deadline) {
				con.Interrupt();
				break;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		query_thread.join();
		auto end = std::chrono::steady_clock::now();
		qr.time_ms = std::chrono::duration<double, std::milli>(end - start).count();

		if (query_exception) {
			std::rethrow_exception(query_exception);
		}

		if (result && result->HasError()) {
			qr.error = result->GetError();
			// Strip stack traces for cleaner grouping
			auto stack_pos = qr.error.find("\n\nStack Trace");
			if (stack_pos != string::npos) {
				qr.error = qr.error.substr(0, stack_pos);
			}
			if (qr.error.size() > 200) {
				qr.error = qr.error.substr(0, 200);
			}
			if (qr.error.find("FATAL") != string::npos ||
			    qr.error.find("database has been invalidated") != string::npos) {
				qr.state = "crash";
			} else if (qr.error.find("INTERRUPT") != string::npos ||
			           qr.error.find("Interrupted") != string::npos) {
				qr.state = "timeout";
			} else {
				qr.state = "error";
			}
		} else if (qr.time_ms > timeout_s * 1000) {
			qr.state = "timeout";
		} else {
			qr.state = "success";
			if (result) {
				int64_t row_count = 0;
				while (auto chunk = result->Fetch()) {
					row_count += chunk->size();
				}
				qr.rows = row_count;
			}
		}
	} catch (std::exception &e) {
		string err = e.what();
		auto stack_pos = err.find("\n\nStack Trace");
		if (stack_pos != string::npos) {
			err = err.substr(0, stack_pos);
		}
		if (err.size() > 200) {
			err = err.substr(0, 200);
		}
		qr.error = err;
		// Classify the exception the same way as result errors:
		// only FATAL / database invalidation are true crashes
		if (err.find("FATAL") != string::npos ||
		    err.find("database has been invalidated") != string::npos) {
			qr.state = "crash";
		} else if (err.find("INTERRUPT") != string::npos ||
		           err.find("Interrupted") != string::npos) {
			qr.state = "timeout";
		} else {
			qr.state = "error";
		}
	} catch (...) {
		qr.state = "crash";
		qr.time_ms = 0;
		qr.error = "unexpected crash (non-std::exception)";
	}

	return qr;
}

// Run all queries in a single pass, reconnecting on crashes
static vector<BenchmarkQueryResult> RunPass(const string &label, vector<string> &query_files,
                                            unique_ptr<DuckDB> &db, unique_ptr<Connection> &con,
                                            const string &db_path, double timeout_s,
                                            PassStats &stats, bool check_pac = false) {
	int total = static_cast<int>(query_files.size());
	int log_interval = std::max(1, total / 20);
	vector<BenchmarkQueryResult> results;

	auto reconnect = [&]() {
		con.reset();
		db.reset();
		db = make_uniq<DuckDB>(db_path.c_str());
		con = make_uniq<Connection>(*db);
		con->Query("INSTALL icu; LOAD icu;");
		con->Query("INSTALL tpch; LOAD tpch;");
		auto r = con->Query("LOAD pac");
		if (r->HasError()) {
			throw std::runtime_error("Failed to load PAC extension: " + r->GetError());
		}
	};

	Log("=== " + label + " pass: running " + std::to_string(total) + " queries ===");

	for (int i = 0; i < total; ++i) {
		auto &qf = query_files[i];
		string name = QueryName(qf);
		string sql = ReadFileToString(qf);
		if (sql.empty()) {
			results.push_back({name, "error", 0, 0, "empty query file"});
			stats.failed++;
			continue;
		}

		auto qr = RunQuery(*con, name, sql, timeout_s);

		if (qr.state == "success") {
			stats.success++;
			stats.total_success_time += qr.time_ms;
			// Check if PAC was actually applied via EXPLAIN
			if (check_pac) {
				try {
					auto explain_result = con->Query("EXPLAIN " + sql);
					if (explain_result && !explain_result->HasError()) {
						for (idx_t row = 0; row < explain_result->RowCount(); row++) {
							string plan_text = explain_result->GetValue(1, row).ToString();
							if (plan_text.find("pac_") != string::npos) {
								qr.pac_applied = true;
								stats.pac_applied++;
								break;
							}
						}
					}
				} catch (...) {
					// EXPLAIN failed — skip silently
				}
			}
		} else if (qr.state == "timeout") {
			stats.timed_out++;
		} else if (qr.state == "crash") {
			// When the database was invalidated by a previous query, this query
			// never actually ran.  Attribute the crash to the previous query,
			// reconnect, and retry the current query on a fresh connection.
			if (qr.error.find("database has been invalidated") != string::npos && !results.empty()) {
				auto &prev = results.back();
				Log("Database crash caused by query " + prev.name + " (detected on query " + name + ")");
				if (prev.state != "crash") {
					if (prev.state == "error") {
						stats.failed--;
					} else if (prev.state == "success") {
						stats.success--;
						stats.total_success_time -= prev.time_ms;
					}
					prev.state = "crash";
					stats.crashed++;
				}
				Log("  reconnecting...");
				reconnect();
				// Retry this query on the fresh connection
				qr = RunQuery(*con, name, sql, timeout_s);
				// Classify the retried result normally
				if (qr.state == "success") {
					stats.success++;
					stats.total_success_time += qr.time_ms;
					if (check_pac) {
						try {
							auto explain_result = con->Query("EXPLAIN " + sql);
							if (explain_result && !explain_result->HasError()) {
								for (idx_t row = 0; row < explain_result->RowCount(); row++) {
									string plan_text = explain_result->GetValue(1, row).ToString();
									if (plan_text.find("pac_") != string::npos) {
										qr.pac_applied = true;
										stats.pac_applied++;
										break;
									}
								}
							}
						} catch (...) {}
					}
				} else if (qr.state == "timeout") {
					stats.timed_out++;
				} else if (qr.state == "crash") {
					stats.crashed++;
					Log("Database crash on query " + name + ": " + qr.error);
					Log("  reconnecting...");
					reconnect();
				} else {
					stats.failed++;
				}
			} else {
				stats.crashed++;
				Log("Database crash on query " + name + ": " + qr.error);
				Log("  reconnecting...");
				reconnect();
			}
		} else {
			stats.failed++;
		}

		results.push_back(qr);

		if ((i + 1) % log_interval == 0 || i + 1 == total) {
			string progress = "[" + label + "] [" + std::to_string(i + 1) + "/" + std::to_string(total) +
			    "] success=" + std::to_string(stats.success) +
			    " failed=" + std::to_string(stats.failed) +
			    " crash=" + std::to_string(stats.crashed) +
			    " timeout=" + std::to_string(stats.timed_out);
			if (check_pac) {
				progress += " pac=" + std::to_string(stats.pac_applied);
			}
			Log(progress);
		}
	}

	return results;
}

static void PrintPassStats(const string &label, const PassStats &stats, int total) {
	Log("--- " + label + " ---");
	Log("  Total:   " + std::to_string(total));
	Log("  Success: " + std::to_string(stats.success) + " (" + FormatNumber(100.0 * stats.success / total) + "%)");
	Log("  Failed:  " + std::to_string(stats.failed) + " (" + FormatNumber(100.0 * stats.failed / total) + "%)");
	Log("  Crash:   " + std::to_string(stats.crashed) + " (" + FormatNumber(100.0 * stats.crashed / total) + "%)");
	Log("  Timeout: " + std::to_string(stats.timed_out) + " (" + FormatNumber(100.0 * stats.timed_out / total) + "%)");
	if (stats.pac_applied > 0) {
		int not_applied = stats.success - stats.pac_applied;
		Log("  PAC applied:     " + std::to_string(stats.pac_applied) +
		    " (" + FormatNumber(100.0 * stats.pac_applied / total) + "% of total, " +
		    FormatNumber(100.0 * stats.pac_applied / std::max(1, stats.success)) + "% of successful)");
		Log("  PAC not applied: " + std::to_string(not_applied) +
		    " (" + FormatNumber(100.0 * not_applied / total) + "% of total, " +
		    FormatNumber(100.0 * not_applied / std::max(1, stats.success)) + "% of successful)");
	}
	if (stats.success > 0) {
		Log("  Total time (successful): " + FormatNumber(stats.total_success_time) + " ms");
		Log("  Avg time per successful query: " + FormatNumber(stats.total_success_time / stats.success) + " ms");
	}
}

static string FormatQueryList(const vector<string> &names, size_t max_show = 10) {
	string out;
	for (size_t i = 0; i < names.size(); ++i) {
		if (i > 0) out += ", ";
		if (i >= max_show) {
			out += "... +" + std::to_string(names.size() - i) + " more";
			break;
		}
		out += names[i];
	}
	return out;
}

static void PrintUnsupportedAggregateBreakdown(const string &label, const vector<BenchmarkQueryResult> &results, int total) {
	const string marker = "unsupported aggregate function ";
	std::map<string, int> agg_counts;
	int agg_total = 0;

	for (auto &r : results) {
		if (r.state != "error" && r.state != "crash") continue;
		auto pos = r.error.find(marker);
		if (pos == string::npos) continue;
		string func_name = r.error.substr(pos + marker.size());
		// Trim trailing whitespace / junk
		while (!func_name.empty() && (func_name.back() == ' ' || func_name.back() == '\n' || func_name.back() == '\r')) {
			func_name.pop_back();
		}
		if (!func_name.empty()) {
			agg_counts[func_name]++;
			agg_total++;
		}
	}

	if (agg_counts.empty()) return;

	vector<std::pair<string, int>> sorted(agg_counts.begin(), agg_counts.end());
	std::sort(sorted.begin(), sorted.end(),
	          [](const std::pair<string, int> &a, const std::pair<string, int> &b) {
		          return a.second > b.second;
	          });

	Log("=== " + label + " Unsupported Aggregate Breakdown (" + std::to_string(agg_total) +
	    " queries, " + FormatNumber(100.0 * agg_total / total) + "% of total) ===");
	for (auto &e : sorted) {
		string pct_total = FormatNumber(100.0 * e.second / total);
		string pct_agg = FormatNumber(100.0 * e.second / agg_total);
		Log("  " + e.first + ": " + std::to_string(e.second) +
		    " (" + pct_agg + "% of unsupported aggs, " + pct_total + "% of total)");
	}
}

static void PrintErrorBreakdown(const string &label, const vector<BenchmarkQueryResult> &results, int total) {
	int total_errors = 0;
	std::map<string, int> error_counts;
	std::map<string, vector<string>> error_queries;
	for (auto &r : results) {
		if (r.state == "error" || r.state == "crash") {
			error_counts[r.error]++;
			error_queries[r.error].push_back(r.name);
			total_errors++;
		}
	}
	if (error_counts.empty()) return;

	vector<std::pair<string, int>> sorted_errors(error_counts.begin(), error_counts.end());
	std::sort(sorted_errors.begin(), sorted_errors.end(),
	          [](const std::pair<string, int> &a, const std::pair<string, int> &b) {
		          return a.second > b.second;
	          });

	Log("=== " + label + " Error Breakdown (" + std::to_string(total_errors) + " total) ===");
	for (auto &e : sorted_errors) {
		string pct = FormatNumber(100.0 * e.second / total);
		string msg = e.first;
		if (msg.size() > 70) {
			msg = msg.substr(0, 70) + "...";
		}
		Log("  " + std::to_string(e.second) + "x (" + pct + "%) " + msg);
	}

	// Collect INTERNAL errors grouped by message, with query names
	std::map<string, vector<string>> internal_groups;
	for (auto &e : sorted_errors) {
		if (e.first.find("INTERNAL") != string::npos) {
			internal_groups[e.first] = error_queries[e.first];
		}
	}
	if (!internal_groups.empty()) {
		Log("=== " + label + " Internal Errors (queries) ===");
		for (auto &g : internal_groups) {
			string msg = g.first;
			if (msg.size() > 100) {
				msg = msg.substr(0, 100) + "...";
			}
			Log("  " + msg);
			Log("    queries: " + FormatQueryList(g.second));
		}
	}

	// Timeout and crash details
	vector<string> timeout_queries, crash_queries;
	for (auto &r : results) {
		if (r.state == "timeout") timeout_queries.push_back(r.name);
		else if (r.state == "crash") crash_queries.push_back(r.name);
	}
	if (!timeout_queries.empty()) {
		Log("=== " + label + " Timeouts (" + std::to_string(timeout_queries.size()) + ") ===");
		Log("  queries: " + FormatQueryList(timeout_queries, 20));
	}
	if (!crash_queries.empty()) {
		Log("=== " + label + " Crashes (" + std::to_string(crash_queries.size()) + ") ===");
		Log("  queries: " + FormatQueryList(crash_queries, 20));
	}
}

static void WriteCSV(const string &csv_path, const vector<BenchmarkQueryResult> &baseline_results,
                     const vector<BenchmarkQueryResult> &pac_results) {
	std::ofstream csv(csv_path, std::ofstream::out | std::ofstream::trunc);
	if (!csv.is_open()) {
		Log("ERROR: Failed to open output CSV: " + csv_path);
		return;
	}
	csv << "query,baseline_state,baseline_time_ms,baseline_rows,pac_state,pac_time_ms,pac_rows,pac_applied,pac_error\n";

	// Build a map from query name to pac result for easy lookup
	std::map<string, const BenchmarkQueryResult *> pac_map;
	for (auto &r : pac_results) {
		pac_map[r.name] = &r;
	}

	for (auto &b : baseline_results) {
		csv << b.name << "," << b.state << "," << FormatNumber(b.time_ms) << "," << b.rows;

		auto it = pac_map.find(b.name);
		if (it != pac_map.end()) {
			auto &p = *it->second;
			csv << "," << p.state << "," << FormatNumber(p.time_ms) << "," << p.rows
			    << "," << (p.pac_applied ? "yes" : "no");
			// Write error message (quote it for CSV safety)
			csv << ",\"";
			for (char c : p.error) {
				if (c == '"') csv << "\"\"";
				else if (c == '\n' || c == '\r') csv << ' ';
				else csv << c;
			}
			csv << "\"";
		} else {
			csv << ",,,,,";
		}
		csv << "\n";
	}
	csv.close();
	Log("Results written to: " + csv_path);
}

int RunSQLStormBenchmark(const string &queries_dir, const string &out_csv, double timeout_s) {
	try {
		// Resolve queries directory
		string qdir = queries_dir;
		if (qdir.empty()) {
			qdir = FindQueriesDir();
		}
		if (qdir.empty() || !FileExists(qdir)) {
			Log("ERROR: Cannot find SQLStorm TPC-H queries directory.");
			Log("Provide it with --queries <path>");
			return 1;
		}

		auto query_files = CollectQueryFiles(qdir);
		if (query_files.empty()) {
			Log("ERROR: No .sql files found in " + qdir);
			return 1;
		}

		Log("SQLStorm TPC-H benchmark (two-pass: baseline + PAC)");
		Log("Queries directory: " + qdir);
		Log("Found " + std::to_string(query_files.size()) + " queries");
		Log("Timeout: " + FormatNumber(timeout_s) + "s");

		// Open file-backed DuckDB database
		string db_path = "tpch_sqlstorm.db";
		bool db_exists = FileExists(db_path);

		unique_ptr<DuckDB> db;
		unique_ptr<Connection> con;

		auto reconnect = [&]() {
			con.reset();
			db.reset();
			db = make_uniq<DuckDB>(db_path.c_str());
			con = make_uniq<Connection>(*db);
			con->Query("INSTALL icu; LOAD icu;");
			con->Query("INSTALL tpch; LOAD tpch;");
			auto r = con->Query("LOAD pac");
			if (r->HasError()) {
				throw std::runtime_error("Failed to load PAC extension: " + r->GetError());
			}
		};

		reconnect();

		// Generate TPC-H data if database doesn't exist yet
		if (!db_exists) {
			Log("Generating TPC-H SF0.00001 data...");
			auto t0 = std::chrono::steady_clock::now();
			auto r_dbgen = con->Query("CALL dbgen(sf=0.00001);");
			auto t1 = std::chrono::steady_clock::now();
			double gen_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
			if (r_dbgen && r_dbgen->HasError()) {
				Log("CALL dbgen error: " + r_dbgen->GetError());
			}
			Log("TPC-H data generated in " + FormatNumber(gen_ms) + " ms");
		} else {
			Log("Using existing database: " + db_path);
		}

		// ===== PASS 1: Baseline (no PAC) =====
		// Clear any existing PAC metadata so queries run without PAC transforms
		con->Query("PRAGMA clear_pac_metadata;");
		Log("Cleared PAC metadata for baseline pass");

		PassStats baseline_stats;
		auto baseline_results = RunPass("Baseline", query_files, db, con, db_path, timeout_s, baseline_stats);

		// ===== Load PAC schema =====
		LoadPACSchema(*con);

		// ===== PASS 2: PAC =====
		PassStats pac_stats;
		auto pac_results = RunPass("PAC", query_files, db, con, db_path, timeout_s, pac_stats, true);

		// ===== Print statistics =====
		int total = static_cast<int>(query_files.size());
		Log("========================================");
		Log("=== RESULTS ===");
		Log("========================================");
		PrintPassStats("Baseline (no PAC)", baseline_stats, total);
		PrintPassStats("PAC", pac_stats, total);

		// Comparison: queries that changed state between passes
		int baseline_only_success = 0, pac_only_success = 0, both_success = 0;
		for (size_t i = 0; i < baseline_results.size() && i < pac_results.size(); ++i) {
			bool b_ok = baseline_results[i].state == "success";
			bool p_ok = pac_results[i].state == "success";
			if (b_ok && p_ok) both_success++;
			else if (b_ok && !p_ok) baseline_only_success++;
			else if (!b_ok && p_ok) pac_only_success++;
		}
		Log("--- Comparison ---");
		Log("  Both succeeded: " + std::to_string(both_success));
		Log("  Baseline only:  " + std::to_string(baseline_only_success));
		Log("  PAC only:       " + std::to_string(pac_only_success));

		// Timing comparison for queries that succeeded in both passes
		if (both_success > 0) {
			double sum_baseline = 0, sum_pac = 0;
			double sum_pac_applied_baseline = 0, sum_pac_applied_pac = 0;
			int pac_applied_both = 0;
			for (size_t i = 0; i < baseline_results.size() && i < pac_results.size(); ++i) {
				if (baseline_results[i].state == "success" && pac_results[i].state == "success") {
					sum_baseline += baseline_results[i].time_ms;
					sum_pac += pac_results[i].time_ms;
					if (pac_results[i].pac_applied) {
						sum_pac_applied_baseline += baseline_results[i].time_ms;
						sum_pac_applied_pac += pac_results[i].time_ms;
						pac_applied_both++;
					}
				}
			}
			Log("--- Timing (queries succeeding in both passes) ---");
			Log("  Baseline total: " + FormatNumber(sum_baseline) + " ms");
			Log("  PAC total:      " + FormatNumber(sum_pac) + " ms");
			if (sum_baseline > 0) {
				double overhead_pct = 100.0 * (sum_pac - sum_baseline) / sum_baseline;
				if (overhead_pct >= 0) {
					Log("  PAC is " + FormatNumber(overhead_pct) + "% slower than DuckDB");
				} else {
					Log("  PAC is " + FormatNumber(-overhead_pct) + "% faster than DuckDB");
				}
			}
			if (pac_applied_both > 0 && sum_pac_applied_baseline > 0) {
				double overhead_applied = 100.0 * (sum_pac_applied_pac - sum_pac_applied_baseline) / sum_pac_applied_baseline;
				Log("  PAC-transformed queries only (" + std::to_string(pac_applied_both) + " queries):");
				Log("    Baseline: " + FormatNumber(sum_pac_applied_baseline) + " ms, PAC: " + FormatNumber(sum_pac_applied_pac) + " ms");
				if (overhead_applied >= 0) {
					Log("    PAC is " + FormatNumber(overhead_applied) + "% slower than DuckDB");
				} else {
					Log("    PAC is " + FormatNumber(-overhead_applied) + "% faster than DuckDB");
				}
			}
		}

		PrintErrorBreakdown("Baseline", baseline_results, total);
		PrintErrorBreakdown("PAC", pac_results, total);
		PrintUnsupportedAggregateBreakdown("PAC", pac_results, total);

		// Write CSV with both passes
		string csv_path = out_csv.empty() ? "benchmark/sqlstorm/sqlstorm_results.csv" : out_csv;
		WriteCSV(csv_path, baseline_results, pac_results);

		// NOTE: PAC metadata is intentionally NOT cleared at the end
		return 0;

	} catch (std::exception &ex) {
		Log(string("Fatal error: ") + ex.what());
		return 2;
	}
}

} // namespace duckdb

static void PrintUsage() {
	std::cout << "Usage: pac_sqlstorm_benchmark [options]\n"
	          << "Options:\n"
	          << "  --queries <dir>   SQLStorm TPC-H queries directory\n"
	          << "                    (default: auto-detect benchmark/SQLStorm/v1.0/tpch/queries)\n"
	          << "  --out <csv>       Output CSV path (default: benchmark/sqlstorm/sqlstorm_results.csv)\n"
	          << "  --timeout <sec>   Per-query timeout in seconds (default: 10)\n"
	          << "  -h, --help        Show this help message\n";
}

int main(int argc, char **argv) {
	std::string queries_dir;
	std::string out_csv;
	double timeout_s = 1.0;

	for (int i = 1; i < argc; ++i) {
		std::string arg = argv[i];
		if (arg == "-h" || arg == "--help") {
			PrintUsage();
			return 0;
		} else if (arg == "--queries" && i + 1 < argc) {
			queries_dir = argv[++i];
		} else if (arg == "--out" && i + 1 < argc) {
			out_csv = argv[++i];
		} else if (arg == "--timeout" && i + 1 < argc) {
			timeout_s = std::stod(argv[++i]);
		} else {
			std::cerr << "Unknown option: " << arg << "\n";
			PrintUsage();
			return 1;
		}
	}

	return duckdb::RunSQLStormBenchmark(queries_dir, out_csv, timeout_s);
}
