//
// SQLStorm benchmark runner for DuckDB (TPC-H + StackOverflow).
// Two-pass design: runs all SQLStorm queries first without PAC (baseline mode),
// then loads the PAC schema and runs them again (PAC mode). Reports statistics for both.
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
#include <mutex>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
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

// Search for a file in common benchmark-relative paths
static string FindBenchmarkFile(const string &filename, const vector<string> &subdirs) {
	vector<string> candidates;
	// Direct and relative-to-cwd
	candidates.push_back(filename);
	candidates.push_back("./" + filename);
	candidates.push_back("../" + filename);
	candidates.push_back("../../" + filename);
	for (auto &sub : subdirs) {
		candidates.push_back(sub + "/" + filename);
		candidates.push_back("../" + sub + "/" + filename);
		candidates.push_back("../../" + sub + "/" + filename);
	}

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
			for (auto &sub : subdirs) {
				string cand = dir + "/" + sub + "/" + filename;
				if (FileExists(cand)) {
					return cand;
				}
			}
			auto p2 = dir.find_last_of('/');
			if (p2 == string::npos) break;
			dir = dir.substr(0, p2);
		}
	}

	return "";
}

static string FindSchemaFile(const string &filename) {
	return FindBenchmarkFile(filename, {"benchmark", "benchmark/tpch", "benchmark/sqlstorm"});
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

// Try to find the SQLStorm StackOverflow queries directory
static string FindSOQueriesDir() {
	vector<string> candidates = {
		"SQLStorm/v1.0/stackoverflow/queries",
	    "benchmark/sqlstorm/SQLStorm/v1.0/stackoverflow/queries",
	    "../benchmark/sqlstorm/SQLStorm/v1.0/stackoverflow/queries",
	    "../../benchmark/sqlstorm/SQLStorm/v1.0/stackoverflow/queries",
	    "../../../benchmark/sqlstorm/SQLStorm/v1.0/stackoverflow/queries",
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
			string cand = dir + "/benchmark/sqlstorm/SQLStorm/v1.0/stackoverflow/queries";
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

// Find the benchmark/sqlstorm directory (used as base for SO data download)
static string FindSQLStormDir() {
	vector<string> candidates = {
		"benchmark/sqlstorm",
	    "../benchmark/sqlstorm",
	    "../../benchmark/sqlstorm",
	};

	for (auto &c : candidates) {
		if (FileExists(c)) {
			return c;
		}
	}

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
			string cand = dir + "/benchmark/sqlstorm";
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

// Lightweight per-query summary for cross-pass comparison (no strings allocated)
struct QuerySummary {
	double time_ms;
	uint8_t state;      // 0=success, 1=error, 2=timeout, 3=crash
	bool pac_applied;

	bool IsSuccess() const { return state == 0; }
};

static uint8_t StateToInt(const string &s) {
	if (s == "success") return 0;
	if (s == "error") return 1;
	if (s == "timeout") return 2;
	return 3; // crash
}

struct PassStats {
	int success = 0;
	int failed = 0;
	int timed_out = 0;
	int crashed = 0;
	int pac_applied = 0;
	double total_success_time = 0;
	// Error tracking (accumulated during the pass)
	std::map<string, int> error_counts;
	std::map<string, vector<string>> error_queries;
	vector<string> timeout_queries;
	vector<string> crash_queries;
	// Unsupported aggregate tracking
	std::map<string, int> unsupported_agg_counts;
	int unsupported_agg_total = 0;
};

// Execute a SQL file statement-by-statement (PAC parser needs individual statements).
// Handles semicolons inside /* ... */ block comments correctly.
static void ExecuteSQLFile(Connection &con, const string &schema_file) {
	string sql = ReadFileToString(schema_file);
	if (sql.empty()) {
		throw std::runtime_error("SQL file is empty: " + schema_file);
	}

	// Split into statements respecting block comments and strings
	string current_statement;
	bool in_block_comment = false;
	bool in_line_comment = false;
	bool in_string = false;

	for (size_t i = 0; i < sql.size(); ++i) {
		char c = sql[i];
		char next = (i + 1 < sql.size()) ? sql[i + 1] : '\0';

		if (in_line_comment) {
			current_statement += c;
			if (c == '\n') {
				in_line_comment = false;
			}
			continue;
		}
		if (in_block_comment) {
			current_statement += c;
			if (c == '*' && next == '/') {
				current_statement += next;
				++i;
				in_block_comment = false;
			}
			continue;
		}
		if (in_string) {
			current_statement += c;
			if (c == '\'') {
				if (next == '\'') {
					current_statement += next;
					++i; // escaped quote
				} else {
					in_string = false;
				}
			}
			continue;
		}

		// Not inside any special context
		if (c == '-' && next == '-') {
			in_line_comment = true;
			current_statement += c;
			continue;
		}
		if (c == '/' && next == '*') {
			in_block_comment = true;
			current_statement += c;
			current_statement += next;
			++i;
			continue;
		}
		if (c == '\'') {
			in_string = true;
			current_statement += c;
			continue;
		}
		if (c == ';') {
			current_statement += c;
			// Trim and execute
			size_t start = current_statement.find_first_not_of(" \t\r\n");
			if (start != string::npos) {
				auto result = con.Query(current_statement);
				if (result->HasError()) {
					throw std::runtime_error("SQL file error: " + result->GetError() +
					                         "\nStatement: " + current_statement);
				}
			}
			current_statement.clear();
			continue;
		}
		current_statement += c;
	}

	// Execute any trailing statement without a semicolon
	if (!current_statement.empty()) {
		size_t start = current_statement.find_first_not_of(" \t\r\n");
		if (start != string::npos) {
			auto result = con.Query(current_statement);
			if (result->HasError()) {
				throw std::runtime_error("SQL file error: " + result->GetError());
			}
		}
	}
}

static void LoadPACSchema(Connection &con, const string &pac_schema_filename) {
	string schema_file = FindSchemaFile(pac_schema_filename);
	if (schema_file.empty()) {
		throw std::runtime_error("Cannot find " + pac_schema_filename);
	}
	Log("Loading PAC schema from: " + schema_file);
	ExecuteSQLFile(con, schema_file);
	Log("PAC schema loaded successfully");
}

// Persistent worker thread for query execution — avoids creating a new thread per query.
// Main thread submits work and monitors for timeouts; worker thread runs con.Query().
struct QueryWorker {
	std::thread thread;
	std::mutex mtx;
	std::condition_variable cv_work;
	std::condition_variable cv_done;

	Connection *con = nullptr;
	const string *sql = nullptr;
	unique_ptr<MaterializedQueryResult> result;
	std::exception_ptr exception;
	bool has_work = false;
	bool work_done = false;
	bool stop = false;

	QueryWorker() {
		thread = std::thread([this]() { WorkerLoop(); });
	}

	~QueryWorker() {
		{
			std::lock_guard<std::mutex> lk(mtx);
			stop = true;
		}
		cv_work.notify_one();
		if (thread.joinable()) {
			thread.join();
		}
	}

	void WorkerLoop() {
		while (true) {
			std::unique_lock<std::mutex> lk(mtx);
			cv_work.wait(lk, [this]() { return has_work || stop; });
			if (stop && !has_work) {
				return;
			}

			Connection *local_con = con;
			const string &local_sql = *sql;
			lk.unlock();

			unique_ptr<MaterializedQueryResult> local_result;
			std::exception_ptr local_exception;
			try {
				local_result = local_con->Query(local_sql);
			} catch (...) {
				local_exception = std::current_exception();
			}

			lk.lock();
			result = std::move(local_result);
			exception = local_exception;
			work_done = true;
			has_work = false;
			lk.unlock();
			cv_done.notify_one();
		}
	}

	// Submit a query; blocks until the query completes or timeout triggers an interrupt.
	void Submit(Connection &c, const string &q, double timeout_s) {
		{
			std::lock_guard<std::mutex> lk(mtx);
			con = &c;
			sql = &q;
			result.reset();
			exception = nullptr;
			has_work = true;
			work_done = false;
		}
		cv_work.notify_one();

		auto deadline = std::chrono::steady_clock::now() + std::chrono::duration<double>(timeout_s);
		std::unique_lock<std::mutex> lk(mtx);
		if (!cv_done.wait_until(lk, deadline, [this]() { return work_done; })) {
			// Timed out — interrupt and wait for worker to finish
			lk.unlock();
			c.Interrupt();
			lk.lock();
			cv_done.wait(lk, [this]() { return work_done; });
		}
	}
};

// Run a single query with timeout via persistent worker thread.
static BenchmarkQueryResult RunQuery(QueryWorker &worker, Connection &con, const string &name, const string &sql, double timeout_s) {
	BenchmarkQueryResult qr;
	qr.name = name;
	qr.rows = 0;
	qr.pac_applied = false;

	try {
		auto start = std::chrono::steady_clock::now();
		worker.Submit(con, sql, timeout_s);
		auto end = std::chrono::steady_clock::now();
		qr.time_ms = std::chrono::duration<double, std::milli>(end - start).count();

		if (worker.exception) {
			std::rethrow_exception(worker.exception);
		}

		if (worker.result && worker.result->HasError()) {
			qr.error = worker.result->GetError();
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
		}
		// Free result memory immediately (match OLAPBench fetch_result=false)
		worker.result.reset();
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
		if (err.find("FATAL") != string::npos ||
		    err.find("database has been invalidated") != string::npos) {
			qr.state = "crash";
		} else if (err.find("INTERRUPT") != string::npos ||
		           err.find("Interrupted") != string::npos) {
			qr.state = "timeout";
		} else {
			qr.state = "error";
		}
		worker.result.reset();
	} catch (...) {
		qr.state = "crash";
		qr.time_ms = 0;
		qr.error = "unexpected crash (non-std::exception)";
		worker.result.reset();
	}

	return qr;
}

// Track error/crash info for a query result into PassStats
static void TrackQueryErrors(PassStats &stats, const BenchmarkQueryResult &qr) {
	if (qr.state == "error" || qr.state == "crash") {
		stats.error_counts[qr.error]++;
		stats.error_queries[qr.error].push_back(qr.name);
	}
	if (qr.state == "timeout") {
		stats.timeout_queries.push_back(qr.name);
	} else if (qr.state == "crash") {
		stats.crash_queries.push_back(qr.name);
	}
	// Track unsupported aggregates
	if (qr.state == "error" || qr.state == "crash") {
		const string marker = "unsupported aggregate function ";
		auto pos = qr.error.find(marker);
		if (pos != string::npos) {
			string func_name = qr.error.substr(pos + marker.size());
			while (!func_name.empty() && (func_name.back() == ' ' || func_name.back() == '\n' || func_name.back() == '\r')) {
				func_name.pop_back();
			}
			if (!func_name.empty()) {
				stats.unsupported_agg_counts[func_name]++;
				stats.unsupported_agg_total++;
			}
		}
	}
}

// Run all queries in a single pass, reconnecting on crashes.
// Returns lightweight summaries for cross-pass comparison (no strings allocated).
static vector<QuerySummary> RunPass(const string &label, vector<string> &query_files,
                                    unique_ptr<DuckDB> &db, unique_ptr<Connection> &con,
                                    const string &db_path, double timeout_s,
                                    PassStats &stats, bool check_pac = false,
                                    const string &pac_schema = "") {
	int total = static_cast<int>(query_files.size());
	int log_interval = std::max(1, total / 10);
	vector<QuerySummary> summaries;
	summaries.reserve(total);
	// Track previous query name+state for crash attribution
	string prev_name;
	uint8_t prev_state_int = 1; // error
	auto worker = make_uniq<QueryWorker>();

	auto reconnect = [&]() {
		con.reset();
		db.reset();
		db = make_uniq<DuckDB>(db_path.c_str());
		con = make_uniq<Connection>(*db);
		con->Query("PRAGMA threads=16");
		con->Query("INSTALL icu");
		con->Query("LOAD icu");
		con->Query("INSTALL tpch");
		con->Query("LOAD tpch");
		auto r = con->Query("LOAD pac");
		if (r->HasError()) {
			throw std::runtime_error("Failed to load PAC extension: " + r->GetError());
		}
		// Reload PAC schema after crash so subsequent queries still get PAC transforms
		if (!pac_schema.empty()) {
			try {
				LoadPACSchema(*con, pac_schema);
				Log("Reloaded PAC schema after crash recovery");
			} catch (std::exception &e) {
				Log("WARNING: Failed to reload PAC schema after crash: " + string(e.what()));
			}
		}
	};

	Log("=== " + label + " pass: running " + std::to_string(total) + " queries ===");

	for (int i = 0; i < total; ++i) {
		auto &qf = query_files[i];
		string name = QueryName(qf);
		string sql = ReadFileToString(qf);
		if (sql.empty()) {
			BenchmarkQueryResult qr_empty;
			qr_empty.name = name;
			qr_empty.state = "error";
			qr_empty.time_ms = 0;
			qr_empty.rows = 0;
			qr_empty.error = "empty query file";
			qr_empty.pac_applied = false;
			TrackQueryErrors(stats, qr_empty);
			summaries.push_back({0, StateToInt("error"), false});
			stats.failed++;
			prev_name = name;
			prev_state_int = 1;
			continue;
		}

		auto qr = RunQuery(*worker, *con, name, sql, timeout_s);

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
			if (qr.error.find("database has been invalidated") != string::npos && !summaries.empty()) {
				Log("Database crash caused by query " + prev_name + " (detected on query " + name + ")");
				// Reclassify previous query as crash
				auto &prev_summary = summaries.back();
				if (prev_summary.state != 3) { // not already crash
					if (prev_summary.state == 1) { // was error
						stats.failed--;
					} else if (prev_summary.state == 0) { // was success
						stats.success--;
						stats.total_success_time -= prev_summary.time_ms;
					}
					prev_summary.state = 3; // crash
					stats.crashed++;
				}
				Log("  reconnecting...");
				reconnect();
				qr = RunQuery(*worker, *con, name, sql, timeout_s);
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

		TrackQueryErrors(stats, qr);
		summaries.push_back({qr.time_ms, StateToInt(qr.state), qr.pac_applied});
		prev_name = name;
		prev_state_int = StateToInt(qr.state);

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

		// Periodically force checkpoint to reclaim DuckDB memory
		if ((i + 1) % 100 == 0) {
			try { con->Query("FORCE CHECKPOINT"); } catch (...) {}
		}

		// Periodically reconnect to fully reclaim memory from interrupted queries
		// (DuckDB's soft memory limit can be exceeded by temp allocations during
		// query execution, and interrupted queries may not fully release memory)
		if ((i + 1) % 1000 == 0) {
			Log("[" + label + "] periodic reconnect to reclaim memory (" + std::to_string(i + 1) + "/" + std::to_string(total) + ")");
			worker.reset();
			reconnect();
			worker = make_uniq<QueryWorker>();
		}
	}

	return summaries;
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

static void PrintUnsupportedAggregateBreakdown(const string &label, const PassStats &stats, int total) {
	if (stats.unsupported_agg_counts.empty()) return;

	vector<std::pair<string, int>> sorted(stats.unsupported_agg_counts.begin(), stats.unsupported_agg_counts.end());
	std::sort(sorted.begin(), sorted.end(),
	          [](const std::pair<string, int> &a, const std::pair<string, int> &b) {
		          return a.second > b.second;
	          });

	Log("=== " + label + " Unsupported Aggregate Breakdown (" + std::to_string(stats.unsupported_agg_total) +
	    " queries, " + FormatNumber(100.0 * stats.unsupported_agg_total / total) + "% of total) ===");
	for (auto &e : sorted) {
		string pct_total = FormatNumber(100.0 * e.second / total);
		string pct_agg = FormatNumber(100.0 * e.second / stats.unsupported_agg_total);
		Log("  " + e.first + ": " + std::to_string(e.second) +
		    " (" + pct_agg + "% of unsupported aggs, " + pct_total + "% of total)");
	}
}

static void PrintErrorBreakdown(const string &label, const PassStats &stats, int total) {
	if (stats.error_counts.empty()) return;

	int total_errors = 0;
	for (auto &e : stats.error_counts) {
		total_errors += e.second;
	}

	vector<std::pair<string, int>> sorted_errors(stats.error_counts.begin(), stats.error_counts.end());
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
			auto it = stats.error_queries.find(e.first);
			if (it != stats.error_queries.end()) {
				internal_groups[e.first] = it->second;
			}
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

	if (!stats.timeout_queries.empty()) {
		Log("=== " + label + " Timeouts (" + std::to_string(stats.timeout_queries.size()) + ") ===");
		Log("  queries: " + FormatQueryList(stats.timeout_queries, 20));
	}
	if (!stats.crash_queries.empty()) {
		Log("=== " + label + " Crashes (" + std::to_string(stats.crash_queries.size()) + ") ===");
		Log("  queries: " + FormatQueryList(stats.crash_queries, 20));
	}
}


static double Median(vector<double> v) {
	if (v.empty()) return 0.0;
	std::sort(v.begin(), v.end());
	size_t n = v.size();
	if (n % 2 == 1) {
		return v[n / 2];
	}
	return (v[n / 2 - 1] + v[n / 2]) / 2.0;
}

static string StateIntToString(uint8_t s) {
	switch (s) {
		case 0: return "success";
		case 1: return "error";
		case 2: return "timeout";
		default: return "crash";
	}
}

// Write per-query degradation CSV and compute/log degradation score.
// Returns the absolute path to the written CSV (empty on failure).
static string WriteDegradationCSV(const vector<string> &query_files,
                                  const vector<QuerySummary> &baseline_summaries,
                                  const vector<QuerySummary> &pac_summaries,
                                  const string &csv_path) {
	std::ofstream out(csv_path);
	if (!out.is_open()) {
		Log("ERROR: Cannot open CSV for writing: " + csv_path);
		return "";
	}
	out << "query_index,query,mode,time_ms,state,pac_applied\n";
	size_t n = std::min({query_files.size(), baseline_summaries.size(), pac_summaries.size()});
	for (size_t i = 0; i < n; ++i) {
		string name = QueryName(query_files[i]);
		int idx = static_cast<int>(i) + 1;
		out << idx << "," << name << ",DuckDB,"
		    << std::fixed << std::setprecision(3) << baseline_summaries[i].time_ms << ","
		    << StateIntToString(baseline_summaries[i].state) << ","
		    << (baseline_summaries[i].pac_applied ? "true" : "false") << "\n";
		out << idx << "," << name << ",SIMD-PAC,"
		    << std::fixed << std::setprecision(3) << pac_summaries[i].time_ms << ","
		    << StateIntToString(pac_summaries[i].state) << ","
		    << (pac_summaries[i].pac_applied ? "true" : "false") << "\n";
	}
	out.close();
	Log("Degradation CSV written to: " + csv_path);

	// --- Degradation score ---
	// Collect times for queries where both passes succeeded
	vector<double> baseline_times, pac_times;
	for (size_t i = 0; i < n; ++i) {
		if (baseline_summaries[i].IsSuccess() && pac_summaries[i].IsSuccess()) {
			baseline_times.push_back(baseline_summaries[i].time_ms);
			pac_times.push_back(pac_summaries[i].time_ms);
		}
	}
	if (baseline_times.size() >= 2) {
		size_t half = baseline_times.size() / 2;
		vector<double> b_first(baseline_times.begin(), baseline_times.begin() + half);
		vector<double> b_second(baseline_times.begin() + half, baseline_times.end());
		vector<double> p_first(pac_times.begin(), pac_times.begin() + half);
		vector<double> p_second(pac_times.begin() + half, pac_times.end());

		double b_med1 = Median(b_first), b_med2 = Median(b_second);
		double p_med1 = Median(p_first), p_med2 = Median(p_second);
		double b_ratio = (b_med1 > 0) ? b_med2 / b_med1 : 0;
		double p_ratio = (p_med1 > 0) ? p_med2 / p_med1 : 0;

		Log("--- Degradation Score ---");
		Log("  DuckDB:   first-half median=" + FormatNumber(b_med1) + "ms, second-half median=" +
		    FormatNumber(b_med2) + "ms, ratio=" + FormatNumber(b_ratio) + "x");
		Log("  SIMD-PAC: first-half median=" + FormatNumber(p_med1) + "ms, second-half median=" +
		    FormatNumber(p_med2) + "ms, ratio=" + FormatNumber(p_ratio) + "x");
	} else {
		Log("--- Degradation Score ---");
		Log("  Not enough successful queries to compute degradation score.");
	}

	// Return absolute path
	char rbuf[PATH_MAX];
	if (realpath(csv_path.c_str(), rbuf)) {
		return string(rbuf);
	}
	return csv_path;
}

// Find Rscript executable via 'which'
static string FindRscriptAbsolute() {
	FILE *pipe = popen("which Rscript 2>/dev/null", "r");
	if (!pipe) { return string(); }
	char buf[PATH_MAX];
	string out;
	while (fgets(buf, sizeof(buf), pipe)) {
		out += string(buf);
	}
	pclose(pipe);
	while (!out.empty() && (out.back() == '\n' || out.back() == '\r' || out.back() == ' ')) {
		out.pop_back();
	}
	return out;
}

// Find the degradation plot R script
static string FindDegradationPlotScript() {
	vector<string> candidates = {
	    "benchmark/sqlstorm/plot_sqlstorm_degradation.R",
	    "../benchmark/sqlstorm/plot_sqlstorm_degradation.R",
	    "../../benchmark/sqlstorm/plot_sqlstorm_degradation.R",
	};

	char cwd[PATH_MAX];
	if (getcwd(cwd, sizeof(cwd))) {
		string p = string(cwd) + "/benchmark/sqlstorm/plot_sqlstorm_degradation.R";
		if (FileExists(p)) {
			char rbuf[PATH_MAX];
			if (realpath(p.c_str(), rbuf)) return string(rbuf);
			return p;
		}
	}

	for (auto &c : candidates) {
		if (FileExists(c)) {
			char rbuf[PATH_MAX];
			if (realpath(c.c_str(), rbuf)) return string(rbuf);
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
		if (pos != string::npos) dir = dir.substr(0, pos);
		for (int i = 0; i < 6; ++i) {
			string cand = dir + "/benchmark/sqlstorm/plot_sqlstorm_degradation.R";
			if (FileExists(cand)) {
				char rbuf[PATH_MAX];
				if (realpath(cand.c_str(), rbuf)) return string(rbuf);
				return cand;
			}
			auto p2 = dir.find_last_of('/');
			if (p2 == string::npos) break;
			dir = dir.substr(0, p2);
		}
	}

	return "";
}

// Invoke the degradation plot script
static void InvokeDegradationPlotScript(const string &abs_csv_path) {
	string script = FindDegradationPlotScript();
	if (script.empty()) {
		Log("Degradation plot script not found. Skipping plotting.");
		return;
	}
	string rscript = FindRscriptAbsolute();
	if (rscript.empty()) {
		Log("Rscript not found on PATH. Skipping degradation plot.");
		return;
	}
	string cmd = rscript + " --vanilla \"" + script + "\" \"" + abs_csv_path + "\" 2>&1";
	Log("Calling degradation plot script: " + cmd);
	FILE *pipe = popen(cmd.c_str(), "r");
	if (!pipe) {
		Log("popen failed for degradation plot script.");
		return;
	}
	char buffer[4096];
	string output;
	while (true) {
		size_t n = fread(buffer, 1, sizeof(buffer), pipe);
		if (n > 0) output.append(buffer, buffer + n);
		if (n < sizeof(buffer)) break;
	}
	int rc = pclose(pipe);
	int exit_code = rc;
	if (rc != -1 && WIFEXITED(rc)) exit_code = WEXITSTATUS(rc);
	if (!output.empty()) {
		string out_log = output;
		if (out_log.size() > 4000) out_log = out_log.substr(0, 4000) + "\n...[truncated]...";
		Log("Degradation plot output:\n" + out_log);
	}
	if (exit_code == 0) {
		Log("Degradation plot script completed successfully.");
	} else {
		Log("Degradation plot script failed (exit code " + std::to_string(exit_code) + ").");
	}
}

static string FormatSF(double sf) {
	std::ostringstream oss;
	// Use enough precision to distinguish small scale factors like 0.0001
	oss << std::fixed << std::setprecision(4) << sf;
	// Strip trailing zeros (but keep at least one decimal)
	string s = oss.str();
	auto dot = s.find('.');
	if (dot != string::npos) {
		auto last_nonzero = s.find_last_not_of('0');
		if (last_nonzero != string::npos && last_nonzero > dot) {
			s = s.substr(0, last_nonzero + 1);
		} else {
			s = s.substr(0, dot + 2); // keep e.g. "1.0"
		}
	}
	return s;
}

int RunSQLStormBenchmark(const string &queries_dir, const string &out_csv, double timeout_s,
                         double tpch_sf, const string &benchmark) {
	try {
		bool run_tpch = (benchmark == "tpch" || benchmark == "both");
		bool run_stackoverflow = (benchmark == "stackoverflow" || benchmark == "both");

		Log("SQLStorm benchmark (two-pass: baseline + PAC)");
		Log("Benchmark: " + benchmark);
		Log("Timeout: " + FormatNumber(timeout_s) + "s");

		// ===== TPC-H benchmark =====
		if (run_tpch) {
			string sf_str = FormatSF(tpch_sf);
			Log("TPC-H scale factor: " + sf_str);

			// Resolve queries directory (only use --queries for tpch when not running both)
			string qdir = run_stackoverflow ? "" : queries_dir;
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

			Log("Queries directory: " + qdir);
			Log("Found " + std::to_string(query_files.size()) + " queries");

			// Open file-backed DuckDB database
			string db_path = "tpch_sf" + sf_str + "_sqlstorm.db";
			bool db_exists = FileExists(db_path);

			unique_ptr<DuckDB> db;
			unique_ptr<Connection> con;

			auto reconnect = [&]() {
				con.reset();
				db.reset();
				db = make_uniq<DuckDB>(db_path.c_str());
				con = make_uniq<Connection>(*db);
				con->Query("PRAGMA threads=16");
				con->Query("INSTALL icu");
				con->Query("LOAD icu");
				auto ri = con->Query("INSTALL tpch");
				if (ri->HasError()) {
					Log("INSTALL tpch error: " + ri->GetError());
				}
				auto rl = con->Query("LOAD tpch");
				if (rl->HasError()) {
					Log("LOAD tpch error: " + rl->GetError());
				}
				auto r = con->Query("LOAD pac");
				if (r->HasError()) {
					throw std::runtime_error("Failed to load PAC extension: " + r->GetError());
				}
			};

			reconnect();

			// Generate TPC-H data if database doesn't exist yet
			if (!db_exists) {
				Log("Generating TPC-H SF" + sf_str + " data...");
				auto t0 = std::chrono::steady_clock::now();
				auto r_dbgen = con->Query("CALL dbgen(sf=" + sf_str + ");");
				auto t1 = std::chrono::steady_clock::now();
				double gen_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
				if (r_dbgen && r_dbgen->HasError()) {
					Log("CALL dbgen error: " + r_dbgen->GetError());
				}
				Log("TPC-H data generated in " + FormatNumber(gen_ms) + " ms");
			} else {
				Log("Using existing database: " + db_path);
			}

			// ===== PASS 1: baseline (no PAC) =====
			// Clear any existing PAC metadata so queries run without PAC transforms
			con->Query("PRAGMA clear_pac_metadata;");
			Log("Cleared PAC metadata for baseline pass");

			PassStats baseline_stats;
			auto baseline_summaries = RunPass("baseline", query_files, db, con, db_path, timeout_s, baseline_stats);

			// ===== Load PAC schema =====
			LoadPACSchema(*con, "pac_tpch_schema.sql");

			// ===== PASS 2: PAC =====
			PassStats pac_stats;
			auto pac_summaries = RunPass("PAC", query_files, db, con, db_path, timeout_s, pac_stats, true,
			                             "pac_tpch_schema.sql");

			// ===== Print statistics =====
			int total = static_cast<int>(query_files.size());
			Log("========================================");
			Log("=== TPC-H RESULTS ===");
			Log("========================================");
			PrintPassStats("baseline", baseline_stats, total);
			PrintPassStats("PAC", pac_stats, total);

			// Comparison: queries that changed state between passes
			int baseline_only_success = 0, pac_only_success = 0, both_success = 0;
			for (size_t i = 0; i < baseline_summaries.size() && i < pac_summaries.size(); ++i) {
				bool b_ok = baseline_summaries[i].IsSuccess();
				bool p_ok = pac_summaries[i].IsSuccess();
				if (b_ok && p_ok) both_success++;
				else if (b_ok && !p_ok) baseline_only_success++;
				else if (!b_ok && p_ok) pac_only_success++;
			}
			Log("--- Comparison ---");
			Log("  Both succeeded: " + std::to_string(both_success));
			Log("  baseline only:  " + std::to_string(baseline_only_success));
			Log("  PAC only:       " + std::to_string(pac_only_success));

			// Timing comparison for queries that succeeded in both passes
			if (both_success > 0) {
				double sum_baseline = 0, sum_pac = 0;
				double sum_pac_applied_baseline = 0, sum_pac_applied_pac = 0;
				int pac_applied_both = 0;
				for (size_t i = 0; i < baseline_summaries.size() && i < pac_summaries.size(); ++i) {
					if (baseline_summaries[i].IsSuccess() && pac_summaries[i].IsSuccess()) {
						sum_baseline += baseline_summaries[i].time_ms;
						sum_pac += pac_summaries[i].time_ms;
						if (pac_summaries[i].pac_applied) {
							sum_pac_applied_baseline += baseline_summaries[i].time_ms;
							sum_pac_applied_pac += pac_summaries[i].time_ms;
							pac_applied_both++;
						}
					}
				}
				Log("--- Timing (queries succeeding in both passes) ---");
				Log("  baseline total: " + FormatNumber(sum_baseline) + " ms");
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
					Log("    baseline: " + FormatNumber(sum_pac_applied_baseline) + " ms, PAC: " + FormatNumber(sum_pac_applied_pac) + " ms");
					if (overhead_applied >= 0) {
						Log("    PAC is " + FormatNumber(overhead_applied) + "% slower than DuckDB");
					} else {
						Log("    PAC is " + FormatNumber(-overhead_applied) + "% faster than DuckDB");
					}
				}
			}

			PrintErrorBreakdown("baseline", baseline_stats, total);
			PrintErrorBreakdown("PAC", pac_stats, total);
			PrintUnsupportedAggregateBreakdown("PAC", pac_stats, total);

			// Write degradation CSV
			string tpch_csv = out_csv;
			if (tpch_csv.empty()) {
				string sqlstorm_dir = FindSQLStormDir();
				if (!sqlstorm_dir.empty()) {
					tpch_csv = sqlstorm_dir + "/sqlstorm_degradation_tpch_sf" + sf_str + ".csv";
				} else {
					tpch_csv = "sqlstorm_degradation_tpch_sf" + sf_str + ".csv";
				}
			}
			string tpch_abs_csv = WriteDegradationCSV(query_files, baseline_summaries, pac_summaries, tpch_csv);
			if (!tpch_abs_csv.empty()) {
				InvokeDegradationPlotScript(tpch_abs_csv);
			}

			// NOTE: PAC metadata is intentionally NOT cleared at the end
		}

		// ===== StackOverflow benchmark =====
		if (run_stackoverflow) {
			Log("========================================");
			Log("=== StackOverflow benchmark ===");
			Log("========================================");

			// Resolve queries directory (only use --queries for SO when not running both)
			string so_qdir = run_tpch ? "" : queries_dir;
			if (so_qdir.empty()) {
				so_qdir = FindSOQueriesDir();
			}
			if (so_qdir.empty() || !FileExists(so_qdir)) {
				Log("ERROR: Cannot find SQLStorm StackOverflow queries directory.");
				Log("Provide it with --queries <path>");
				return 1;
			}

			auto so_query_files = CollectQueryFiles(so_qdir);
			if (so_query_files.empty()) {
				Log("ERROR: No .sql files found in " + so_qdir);
				return 1;
			}

			Log("Queries directory: " + so_qdir);
			Log("Found " + std::to_string(so_query_files.size()) + " queries");

			// Locate benchmark/sqlstorm base directory for data paths
			string sqlstorm_dir = FindSQLStormDir();
			if (sqlstorm_dir.empty()) {
				throw std::runtime_error("Cannot find benchmark/sqlstorm directory");
			}

			string so_db_path = "stackoverflow_dba_sqlstorm.db";
			bool so_db_exists = FileExists(so_db_path);

			unique_ptr<DuckDB> so_db;
			unique_ptr<Connection> so_con;

			auto so_reconnect = [&]() {
				so_con.reset();
				so_db.reset();
				so_db = make_uniq<DuckDB>(so_db_path.c_str());
				so_con = make_uniq<Connection>(*so_db);
				so_con->Query("PRAGMA threads=16");
				so_con->Query("INSTALL icu; LOAD icu;");
				auto r = so_con->Query("LOAD pac");
				if (r->HasError()) {
					throw std::runtime_error("Failed to load PAC extension: " + r->GetError());
				}
			};

			so_reconnect();

			// Set up the database if it doesn't exist
			if (!so_db_exists) {
				// Look for data in benchmark/sqlstorm/data/stackoverflow_dba/ first,
				// then fall back to stackoverflow_dba/ in CWD
				string data_dir;
				string sqlstorm_data_dir = sqlstorm_dir + "/data/stackoverflow_dba";
				if (FileExists(sqlstorm_data_dir + "/Users.csv")) {
					data_dir = sqlstorm_data_dir;
				} else if (FileExists("stackoverflow_dba/Users.csv")) {
					data_dir = "stackoverflow_dba";
				}

				// Download the dataset if CSVs are not present anywhere
				if (data_dir.empty()) {
					data_dir = sqlstorm_data_dir;
					string tarball = sqlstorm_dir + "/data/stackoverflow_dba.tar.gz";

					// Skip download if tarball already exists
					if (!FileExists(tarball)) {
						Log("StackOverflow DBA dataset not found, downloading...");
						string dl_cmd = "wget -q -O '" + tarball +
						                "' 'https://db.in.tum.de/~schmidt/data/stackoverflow_dba.tar.gz'";
						int dl_ret = system(dl_cmd.c_str());
						if (dl_ret != 0) {
							dl_cmd = "curl -sL -o '" + tarball +
							         "' 'https://db.in.tum.de/~schmidt/data/stackoverflow_dba.tar.gz'";
							dl_ret = system(dl_cmd.c_str());
						}
						if (dl_ret != 0) {
							throw std::runtime_error("Failed to download StackOverflow dataset. "
							                         "Install wget or curl, or manually download "
							                         "https://db.in.tum.de/~schmidt/data/stackoverflow_dba.tar.gz");
						}
						Log("Download complete.");
					} else {
						Log("Using existing tarball: " + tarball);
					}

					// Extract if CSVs still not present
					if (!FileExists(data_dir + "/Users.csv")) {
						Log("Extracting " + tarball + " ...");
						string extract_cmd = "tar -xzf '" + tarball + "' -C '" + sqlstorm_dir + "/data/'";
						int ext_ret = system(extract_cmd.c_str());
						if (ext_ret != 0) {
							throw std::runtime_error("Failed to extract StackOverflow dataset tarball");
						}
						if (!FileExists(data_dir + "/Users.csv")) {
							throw std::runtime_error("StackOverflow CSV files not found after extraction. "
							                         "Expected at: " + data_dir);
						}
						Log("Dataset extracted to: " + data_dir);
					}
				} else {
					Log("Using existing StackOverflow data at: " + data_dir);
				}

				// Create schema and load data; remove the db file on failure
				try {
					string schema_file = FindBenchmarkFile("schema_nofk.sql",
					    {"benchmark/sqlstorm/SQLStorm/v1.0/stackoverflow",
					     "SQLStorm/v1.0/stackoverflow"});
					if (schema_file.empty()) {
						throw std::runtime_error("Cannot find stackoverflow schema_nofk.sql");
					}
					Log("Creating StackOverflow schema from: " + schema_file);
					ExecuteSQLFile(*so_con, schema_file);
					Log("Schema created successfully");

					// Load data from CSVs
					Log("Loading StackOverflow data from CSVs...");
					auto t0 = std::chrono::steady_clock::now();

					vector<string> tables = {
					    "PostHistoryTypes", "LinkTypes", "PostTypes", "CloseReasonTypes",
					    "VoteTypes", "Users", "Badges", "Posts", "Comments",
					    "PostHistory", "PostLinks", "Tags", "Votes"
					};
					for (auto &tbl : tables) {
						string csv_path = data_dir + "/" + tbl + ".csv";
						if (!FileExists(csv_path)) {
							Log("WARNING: CSV not found for table " + tbl + ": " + csv_path);
							continue;
						}
						string copy_sql = "COPY " + tbl + " FROM '" + csv_path +
						                  "' WITH (DELIMITER ',', FORMAT csv, NULL '')";
						auto r = so_con->Query(copy_sql);
						if (r->HasError()) {
							Log("WARNING: COPY failed for " + tbl + ": " + r->GetError());
						}
					}

					auto t1 = std::chrono::steady_clock::now();
					double load_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
					Log("StackOverflow data loaded in " + FormatNumber(load_ms) + " ms");
				} catch (...) {
					Log("ERROR: Failed to set up StackOverflow database, removing " + so_db_path);
					so_con.reset();
					so_db.reset();
					std::remove(so_db_path.c_str());
					throw;
				}
			} else {
				Log("Using existing database: " + so_db_path);
			}

			// ===== PASS 1: baseline (no PAC) =====
			so_con->Query("PRAGMA clear_pac_metadata;");
			Log("Cleared PAC metadata for baseline pass");

			PassStats so_baseline_stats;
			auto so_baseline_summaries = RunPass("DuckDB baseline", so_query_files, so_db, so_con,
			                                     so_db_path, timeout_s, so_baseline_stats);

			// ===== Load PAC schema =====
			LoadPACSchema(*so_con, "pac_stackoverflow_schema.sql");

			// ===== PASS 2: PAC =====
			PassStats so_pac_stats;
			auto so_pac_summaries = RunPass("SO-PAC", so_query_files, so_db, so_con,
			                                so_db_path, timeout_s, so_pac_stats, true,
			                                "pac_stackoverflow_schema.sql");

			// ===== Print statistics =====
			int so_total = static_cast<int>(so_query_files.size());
			Log("========================================");
			Log("=== StackOverflow RESULTS ===");
			Log("========================================");
			PrintPassStats("SO-baseline", so_baseline_stats, so_total);
			PrintPassStats("SO-PAC", so_pac_stats, so_total);

			int so_baseline_only = 0, so_pac_only = 0, so_both = 0;
			for (size_t i = 0; i < so_baseline_summaries.size() && i < so_pac_summaries.size(); ++i) {
				bool b_ok = so_baseline_summaries[i].IsSuccess();
				bool p_ok = so_pac_summaries[i].IsSuccess();
				if (b_ok && p_ok) so_both++;
				else if (b_ok && !p_ok) so_baseline_only++;
				else if (!b_ok && p_ok) so_pac_only++;
			}
			Log("--- Comparison ---");
			Log("  Both succeeded: " + std::to_string(so_both));
			Log("  baseline only:  " + std::to_string(so_baseline_only));
			Log("  PAC only:       " + std::to_string(so_pac_only));

			if (so_both > 0) {
				double sum_baseline = 0, sum_pac = 0;
				double sum_pac_applied_baseline = 0, sum_pac_applied_pac = 0;
				int pac_applied_both = 0;
				for (size_t i = 0; i < so_baseline_summaries.size() && i < so_pac_summaries.size(); ++i) {
					if (so_baseline_summaries[i].IsSuccess() && so_pac_summaries[i].IsSuccess()) {
						sum_baseline += so_baseline_summaries[i].time_ms;
						sum_pac += so_pac_summaries[i].time_ms;
						if (so_pac_summaries[i].pac_applied) {
							sum_pac_applied_baseline += so_baseline_summaries[i].time_ms;
							sum_pac_applied_pac += so_pac_summaries[i].time_ms;
							pac_applied_both++;
						}
					}
				}
				Log("--- Timing (queries succeeding in both passes) ---");
				Log("  baseline total: " + FormatNumber(sum_baseline) + " ms");
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
					Log("    baseline: " + FormatNumber(sum_pac_applied_baseline) + " ms, PAC: " + FormatNumber(sum_pac_applied_pac) + " ms");
					if (overhead_applied >= 0) {
						Log("    PAC is " + FormatNumber(overhead_applied) + "% slower than DuckDB");
					} else {
						Log("    PAC is " + FormatNumber(-overhead_applied) + "% faster than DuckDB");
					}
				}
			}

			PrintErrorBreakdown("SO-baseline", so_baseline_stats, so_total);
			PrintErrorBreakdown("SO-PAC", so_pac_stats, so_total);
			PrintUnsupportedAggregateBreakdown("SO-PAC", so_pac_stats, so_total);

			// Write degradation CSV
			string so_csv = out_csv;
			if (so_csv.empty()) {
				string so_sqlstorm_dir = FindSQLStormDir();
				if (!so_sqlstorm_dir.empty()) {
					so_csv = so_sqlstorm_dir + "/sqlstorm_degradation_stackoverflow.csv";
				} else {
					so_csv = "sqlstorm_degradation_stackoverflow.csv";
				}
			}
			string so_abs_csv = WriteDegradationCSV(so_query_files, so_baseline_summaries, so_pac_summaries, so_csv);
			if (!so_abs_csv.empty()) {
				InvokeDegradationPlotScript(so_abs_csv);
			}
		}

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
	          << "  --queries <dir>       SQLStorm queries directory\n"
	          << "                        (default: auto-detect benchmark/SQLStorm/v1.0/tpch/queries)\n"
	          << "  --out <csv>           Output CSV path (default: benchmark/sqlstorm/sqlstorm_benchmark_results_*.csv)\n"
	          << "  --timeout <sec>       Per-query timeout in seconds (default: 10)\n"
	          << "  --tpch_sf <float>     TPC-H scale factor (default: 1.00)\n"
	          << "  --benchmark <name>    Benchmark to run: tpch, stackoverflow, or both (default: both)\n"
	          << "  -h, --help            Show this help message\n";
}

int main(int argc, char **argv) {
	std::string queries_dir;
	std::string out_csv;
	double timeout_s = 0.1;
	double tpch_sf = 1.0;
	std::string benchmark = "both";

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
		} else if (arg == "--tpch_sf" && i + 1 < argc) {
			tpch_sf = std::stod(argv[++i]);
		} else if (arg == "--benchmark" && i + 1 < argc) {
			benchmark = argv[++i];
			if (benchmark != "tpch" && benchmark != "stackoverflow" && benchmark != "both") {
				std::cerr << "Invalid benchmark: " << benchmark << " (must be tpch, stackoverflow, or both)\n";
				PrintUsage();
				return 1;
			}
		} else {
			std::cerr << "Unknown option: " << arg << "\n";
			PrintUsage();
			return 1;
		}
	}

	return duckdb::RunSQLStormBenchmark(queries_dir, out_csv, timeout_s, tpch_sf, benchmark);
}
