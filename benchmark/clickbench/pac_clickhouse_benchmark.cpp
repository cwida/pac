//
// Created by ila on 02/12/26.
//

// ClickBench benchmark runner with fork-based process isolation.
// The parent never opens DuckDB for benchmarking; a child process handles
// all DB access. If the child is killed (OOM, crash), the parent survives
// and spawns a new one.

#include "pac_clickhouse_benchmark.hpp"

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/printer.hpp"
#include <cstdio>
#include <cstdlib>

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdexcept>
#include <ctime>
#include <algorithm>
#include <iomanip>
#include <unistd.h>
#include <limits.h>
#include <regex>
#include <poll.h>
#include <signal.h>

namespace duckdb {

static string Timestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    return string(buf);
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

static void Log(const string &msg) {
    Printer::Print("[" + Timestamp() + "] " + msg);
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

static vector<string> SplitLines(const string &content) {
    vector<string> lines;
    std::istringstream iss(content);
    string line;
    while (std::getline(iss, line)) {
        size_t start = line.find_first_not_of(" \t\r\n");
        size_t end = line.find_last_not_of(" \t\r\n");
        if (start != string::npos && end != string::npos) {
            string trimmed = line.substr(start, end - start + 1);
            if (!trimmed.empty() && trimmed[0] != '-' && trimmed.substr(0, 2) != "--") {
                lines.push_back(trimmed);
            }
        }
    }
    return lines;
}

static int ExecuteCommand(const string &cmd) {
    Log(string("Executing: ") + cmd);
    int ret = system(cmd.c_str());
    if (ret == -1) {
        return -1;
    }
    if (WIFEXITED(ret)) {
        return WEXITSTATUS(ret);
    }
    return ret;
}

static string FindFile(const string &filename) {
    vector<string> candidates = {
        filename,
        "./" + filename,
        "../" + filename,
        "../../" + filename,
        "benchmark/" + filename,
        "../benchmark/" + filename,
        "../../benchmark/" + filename
    };

    for (auto &cand : candidates) {
        if (FileExists(cand)) {
            return cand;
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
            string cand = dir + "/" + filename;
            if (FileExists(cand)) {
                return cand;
            }
            cand = dir + "/benchmark/" + filename;
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

// =====================================================================
// IPC helpers
// =====================================================================

static bool WriteAllBytes(int fd, const void *buf, size_t n) {
    const char *p = static_cast<const char *>(buf);
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w <= 0) return false;
        p += w;
        n -= static_cast<size_t>(w);
    }
    return true;
}

static bool ReadAllBytes(int fd, void *buf, size_t n) {
    char *p = static_cast<char *>(buf);
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r <= 0) return false;
        p += r;
        n -= static_cast<size_t>(r);
    }
    return true;
}

// =====================================================================
// Result structure
// =====================================================================

struct BenchmarkQueryResult {
    int query_num;
    string mode;  // "baseline" or "PAC"
    int run;
    double time_ms;
    bool success;
    string error_msg;
};

// =====================================================================
// Child worker process
// =====================================================================
// Protocol (parent->child):
//   [uint8_t cmd]
//     cmd=0: stop
//     cmd=1: run query — [uint32_t sql_len][sql_bytes]
//     cmd=2: setup PAC — [uint32_t count][for each: uint32_t len, bytes]
//     cmd=3: unset PAC
// Protocol (child->parent) for cmd=1:
//   [double time_ms][uint8_t ok][uint32_t err_len][err_bytes]

static constexpr uint8_t CMD_STOP = 0;
static constexpr uint8_t CMD_QUERY = 1;
static constexpr uint8_t CMD_SETUP_PAC = 2;
static constexpr uint8_t CMD_UNSET_PAC = 3;

[[noreturn]] static void ChildWorkerMain(int read_fd, int write_fd,
                                          const string &db_path) {
    try {
        DuckDB db(db_path);
        Connection con(db);
        auto r = con.Query("LOAD pac");
        if (r->HasError()) {
            _exit(2);
        }
        con.Query("SET memory_limit='60GB'");
        con.Query("SET temp_directory='" + db_path + ".tmp'");

        while (true) {
            uint8_t cmd = 0;
            if (!ReadAllBytes(read_fd, &cmd, sizeof(cmd))) break;

            if (cmd == CMD_STOP) {
                break;
            } else if (cmd == CMD_UNSET_PAC) {
                con.Query("ALTER TABLE hits UNSET PU;");
            } else if (cmd == CMD_SETUP_PAC) {
                uint32_t count = 0;
                if (!ReadAllBytes(read_fd, &count, sizeof(count))) break;
                for (uint32_t i = 0; i < count; i++) {
                    uint32_t len = 0;
                    if (!ReadAllBytes(read_fd, &len, sizeof(len))) goto done;
                    string stmt(len, '\0');
                    if (!ReadAllBytes(read_fd, &stmt[0], len)) goto done;
                    con.Query(stmt);
                }
            } else if (cmd == CMD_QUERY) {
                uint32_t sql_len = 0;
                if (!ReadAllBytes(read_fd, &sql_len, sizeof(sql_len))) break;
                string sql(sql_len, '\0');
                if (!ReadAllBytes(read_fd, &sql[0], sql_len)) break;

                auto start = std::chrono::steady_clock::now();
                unique_ptr<MaterializedQueryResult> result;
                std::exception_ptr exc;
                try {
                    result = con.Query(sql);
                    if (result && !result->HasError()) {
                        while (result->Fetch()) {}
                    }
                } catch (...) {
                    exc = std::current_exception();
                }
                auto end = std::chrono::steady_clock::now();
                double time_ms = std::chrono::duration<double, std::milli>(end - start).count();

                string error;
                if (exc) {
                    try { std::rethrow_exception(exc); }
                    catch (std::exception &e) { error = e.what(); }
                    catch (...) { error = "unknown exception"; }
                } else if (result && result->HasError()) {
                    error = result->GetError();
                }

                if (!error.empty()) {
                    auto sp = error.find("\n\nStack Trace");
                    if (sp != string::npos) error = error.substr(0, sp);
                    if (error.size() > 300) error = error.substr(0, 300);
                }
                result.reset();

                uint8_t ok = error.empty() ? 1 : 0;
                uint32_t err_len = static_cast<uint32_t>(error.size());
                WriteAllBytes(write_fd, &time_ms, sizeof(time_ms));
                WriteAllBytes(write_fd, &ok, sizeof(ok));
                WriteAllBytes(write_fd, &err_len, sizeof(err_len));
                if (err_len > 0) WriteAllBytes(write_fd, error.data(), err_len);

                // If DB is invalidated, child must exit
                if (!error.empty() &&
                    (error.find("FATAL") != string::npos ||
                     error.find("database has been invalidated") != string::npos)) {
                    _exit(1);
                }
            }
        }
    } catch (...) {
        _exit(2);
    }
done:
    _exit(0);
}

// =====================================================================
// Fork-based worker
// =====================================================================

struct ForkWorker {
    pid_t child_pid = -1;
    int to_child_fd = -1;
    int from_child_fd = -1;
    string db_path;

    double result_time_ms = 0;
    bool result_ok = false;
    string result_error;

    enum SubmitResult { SR_OK, SR_TIMEOUT, SR_CHILD_DIED };

    void Start() {
        Stop();
        int to_child[2], from_child[2];
        if (pipe(to_child) != 0 || pipe(from_child) != 0) {
            throw std::runtime_error("pipe() failed");
        }

        child_pid = fork();
        if (child_pid < 0) {
            close(to_child[0]); close(to_child[1]);
            close(from_child[0]); close(from_child[1]);
            throw std::runtime_error("fork() failed");
        }

        if (child_pid == 0) {
            close(to_child[1]);
            close(from_child[0]);
            ChildWorkerMain(to_child[0], from_child[1], db_path);
            _exit(0);
        }

        close(to_child[0]);
        close(from_child[1]);
        to_child_fd = to_child[1];
        from_child_fd = from_child[0];
    }

    void Stop() {
        if (child_pid > 0) {
            uint8_t cmd = CMD_STOP;
            WriteAllBytes(to_child_fd, &cmd, sizeof(cmd));

            int status;
            for (int i = 0; i < 4; i++) {
                int w = waitpid(child_pid, &status, WNOHANG);
                if (w != 0) break;
                usleep(50000);
            }
            int w = waitpid(child_pid, &status, WNOHANG);
            if (w == 0) {
                kill(child_pid, SIGKILL);
                waitpid(child_pid, &status, 0);
            }
            child_pid = -1;
        }
        if (to_child_fd >= 0) { close(to_child_fd); to_child_fd = -1; }
        if (from_child_fd >= 0) { close(from_child_fd); from_child_fd = -1; }
    }

    size_t GetChildRSS() {
        if (child_pid <= 0) return 0;
        char path[64];
        snprintf(path, sizeof(path), "/proc/%d/statm", child_pid);
        FILE *f = fopen(path, "r");
        if (!f) return 0;
        long pages = 0, rss = 0;
        if (fscanf(f, "%ld %ld", &pages, &rss) != 2) { rss = 0; }
        fclose(f);
        return static_cast<size_t>(rss) * static_cast<size_t>(sysconf(_SC_PAGESIZE));
    }

    // Send a "setup PAC" command with the given statements
    bool SendSetupPAC(const vector<string> &stmts) {
        if (child_pid <= 0) return false;
        uint8_t cmd = CMD_SETUP_PAC;
        if (!WriteAllBytes(to_child_fd, &cmd, sizeof(cmd))) return false;
        uint32_t count = static_cast<uint32_t>(stmts.size());
        if (!WriteAllBytes(to_child_fd, &count, sizeof(count))) return false;
        for (auto &s : stmts) {
            uint32_t len = static_cast<uint32_t>(s.size());
            if (!WriteAllBytes(to_child_fd, &len, sizeof(len))) return false;
            if (!WriteAllBytes(to_child_fd, s.data(), len)) return false;
        }
        return true;
    }

    // Send an "unset PAC" command
    bool SendUnsetPAC() {
        if (child_pid <= 0) return false;
        uint8_t cmd = CMD_UNSET_PAC;
        return WriteAllBytes(to_child_fd, &cmd, sizeof(cmd));
    }

    // Submit a query and wait for the result with timeout
    SubmitResult SubmitQuery(const string &sql, double timeout_s) {
        result_time_ms = 0;
        result_ok = false;
        result_error.clear();

        if (child_pid <= 0) return SR_CHILD_DIED;

        // Send query command
        uint8_t cmd = CMD_QUERY;
        uint32_t sql_len = static_cast<uint32_t>(sql.size());
        if (!WriteAllBytes(to_child_fd, &cmd, sizeof(cmd)) ||
            !WriteAllBytes(to_child_fd, &sql_len, sizeof(sql_len)) ||
            !WriteAllBytes(to_child_fd, sql.data(), sql_len)) {
            int status;
            waitpid(child_pid, &status, 0);
            child_pid = -1;
            result_error = "child process died (write failed)";
            return SR_CHILD_DIED;
        }

        // Wait for result with timeout + memory monitoring
        auto deadline = std::chrono::steady_clock::now() +
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                std::chrono::duration<double>(timeout_s));

        while (true) {
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                kill(child_pid, SIGKILL);
                waitpid(child_pid, nullptr, 0);
                child_pid = -1;
                result_error = "timeout";
                return SR_TIMEOUT;
            }

            int remaining_ms = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
            int poll_ms = std::min(remaining_ms, 500);

            struct pollfd pfd;
            pfd.fd = from_child_fd;
            pfd.events = POLLIN;
            pfd.revents = 0;

            int ret = poll(&pfd, 1, poll_ms);

            if (ret > 0 && (pfd.revents & POLLIN)) {
                // Read result
                uint8_t ok_byte = 0;
                uint32_t err_len = 0;
                if (!ReadAllBytes(from_child_fd, &result_time_ms, sizeof(result_time_ms)) ||
                    !ReadAllBytes(from_child_fd, &ok_byte, sizeof(ok_byte)) ||
                    !ReadAllBytes(from_child_fd, &err_len, sizeof(err_len))) {
                    int status;
                    waitpid(child_pid, &status, 0);
                    child_pid = -1;
                    result_error = "child process died (read failed)";
                    return SR_CHILD_DIED;
                }
                result_ok = (ok_byte != 0);
                if (err_len > 0) {
                    result_error.resize(err_len);
                    if (!ReadAllBytes(from_child_fd, &result_error[0], err_len)) {
                        int status;
                        waitpid(child_pid, &status, 0);
                        child_pid = -1;
                        result_error = "child process died (read failed)";
                        return SR_CHILD_DIED;
                    }
                }

                // If child reported fatal error, it will exit
                if (!result_error.empty() &&
                    (result_error.find("FATAL") != string::npos ||
                     result_error.find("database has been invalidated") != string::npos)) {
                    int status;
                    waitpid(child_pid, &status, 0);
                    child_pid = -1;
                    return SR_CHILD_DIED;
                }

                return SR_OK;
            }

            if (ret > 0 && (pfd.revents & (POLLHUP | POLLERR))) {
                int status;
                waitpid(child_pid, &status, 0);
                child_pid = -1;
                result_error = "child process died unexpectedly";
                return SR_CHILD_DIED;
            }

            // Check if child still alive
            {
                int status;
                int w = waitpid(child_pid, &status, WNOHANG);
                if (w > 0) {
                    child_pid = -1;
                    result_error = "child process died unexpectedly";
                    return SR_CHILD_DIED;
                }
            }

            // Check child memory usage (kill if > 25GB)
            size_t rss = GetChildRSS();
            if (rss > 25ULL * 1024 * 1024 * 1024) {
                kill(child_pid, SIGKILL);
                waitpid(child_pid, nullptr, 0);
                child_pid = -1;
                result_error = "memory limit exceeded (RSS > 25GB)";
                return SR_TIMEOUT;
            }
        }
    }

    ~ForkWorker() { Stop(); }
};

// =====================================================================
// Main benchmark logic
// =====================================================================

int RunClickHouseBenchmark(const string &db_path, const string &queries_dir, const string &out_csv, bool micro) {
    // Ignore SIGPIPE so writing to a dead child's pipe returns EPIPE instead of killing us
    signal(SIGPIPE, SIG_IGN);

    try {
        Log(string("Starting ClickBench benchmark (fork-based)"));
        Log(string("micro flag: ") + (micro ? string("true") : string("false")));

        char cwd[PATH_MAX];
        if (!getcwd(cwd, sizeof(cwd))) {
            throw std::runtime_error("Failed to get current working directory");
        }
        string work_dir = string(cwd);

        // Dataset path
        string parquet_path = work_dir + "/hits.parquet";

        if (!FileExists(parquet_path)) {
            Log("Downloading ClickHouse hits dataset (parquet format)...");
            string download_cmd = "wget -O \"" + parquet_path + "\" https://datasets.clickhouse.com/hits_compatible/hits.parquet";
            int ret = ExecuteCommand(download_cmd);
            if (ret != 0) {
                Log("wget failed, trying curl...");
                download_cmd = "curl -L -o \"" + parquet_path + "\" https://datasets.clickhouse.com/hits_compatible/hits.parquet";
                ret = ExecuteCommand(download_cmd);
                if (ret != 0) {
                    throw std::runtime_error("Failed to download hits.parquet");
                }
            }
            Log("Download complete.");
        } else {
            Log("hits.parquet already exists, skipping download.");
        }

        // Determine database path
        string db_actual;
        if (!db_path.empty()) {
            db_actual = db_path;
        } else if (micro) {
            db_actual = "clickbench_micro.db";
        } else {
            db_actual = "clickbench.db";
        }

        // Find query files
        string create_sql_path = queries_dir + "/create.sql";
        string load_sql_path = queries_dir + "/load.sql";
        string queries_sql_path = queries_dir + "/queries.sql";
        string setup_sql_path = queries_dir + "/setup.sql";

        string create_file = FindFile(create_sql_path);
        string load_file = FindFile(load_sql_path);
        string queries_file = FindFile(queries_sql_path);
        string setup_file = FindFile(setup_sql_path);

        if (create_file.empty()) throw std::runtime_error("Cannot find create.sql in " + queries_dir);
        if (load_file.empty()) throw std::runtime_error("Cannot find load.sql in " + queries_dir);
        if (queries_file.empty()) throw std::runtime_error("Cannot find queries.sql in " + queries_dir);
        if (setup_file.empty()) throw std::runtime_error("Cannot find setup.sql in " + queries_dir);

        Log(string("Using create.sql: ") + create_file);
        Log(string("Using load.sql: ") + load_file);
        Log(string("Using queries.sql: ") + queries_file);
        Log(string("Using setup.sql: ") + setup_file);

        string create_sql = ReadFileToString(create_file);
        string load_sql = ReadFileToString(load_file);
        string queries_content = ReadFileToString(queries_file);
        vector<string> setup_stmts = SplitLines(ReadFileToString(setup_file));

        if (create_sql.empty()) throw std::runtime_error("create.sql is empty or unreadable");
        if (load_sql.empty()) throw std::runtime_error("load.sql is empty or unreadable");

        vector<string> queries = SplitLines(queries_content);
        if (queries.empty()) throw std::runtime_error("No queries found in queries.sql");

        Log(string("Found ") + std::to_string(queries.size()) + " queries to benchmark");

        // =====================================================================
        // Setup phase: create table and load data (in a temporary process)
        // We open the DB briefly to check/load, then close it so the fork
        // worker can open it exclusively.
        // =====================================================================
        {
            bool db_exists = FileExists(db_actual);
            if (db_exists) {
                Log(string("Connecting to existing DuckDB database: ") + db_actual);
            } else {
                Log(string("Creating new DuckDB database: ") + db_actual);
            }

            DuckDB db(db_actual.c_str());
            Connection con(db);

            auto check_result = con.Query("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'hits'");
            bool table_exists = false;
            if (check_result && !check_result->HasError()) {
                auto chunk = check_result->Fetch();
                if (chunk && chunk->size() > 0) {
                    auto count = chunk->GetValue(0, 0).GetValue<int64_t>();
                    table_exists = (count > 0);
                }
            }

            bool needs_reload = false;
            if (table_exists) {
                auto row_count_result = con.Query("SELECT COUNT(*) FROM hits");
                if (row_count_result && !row_count_result->HasError()) {
                    auto chunk = row_count_result->Fetch();
                    if (chunk && chunk->size() > 0) {
                        auto row_count = chunk->GetValue(0, 0).GetValue<int64_t>();
                        Log(string("hits table has ") + std::to_string(row_count) + " rows");
                        if (row_count == 0) {
                            Log("Table is empty, will reload data");
                            needs_reload = true;
                        }
                    }
                }
            }

            if (!table_exists || needs_reload) {
                if (!table_exists) {
                    Log("Creating hits table...");
                    auto create_result = con.Query(create_sql);
                    if (create_result && create_result->HasError()) {
                        throw std::runtime_error("Failed to create table: " + create_result->GetError());
                    }
                }

                Log("Loading data into hits table... (this may take a while)");
                string actual_load_sql = load_sql;

                std::regex parquet_regex("'hits\\.parquet'");
                actual_load_sql = std::regex_replace(actual_load_sql, parquet_regex, "'" + parquet_path + "'");

                if (micro) {
                    Log("Micro mode: limiting data load to 5m rows");
                    std::regex from_parquet_regex("(FROM\\s+read_parquet\\([^)]+\\)[^;]*)");
                    actual_load_sql = std::regex_replace(actual_load_sql, from_parquet_regex, "$1 LIMIT 5000000");
                }

                Log(string("Executing load SQL: ") + actual_load_sql.substr(0, 200) + "...");

                auto load_result = con.Query(actual_load_sql);
                if (load_result && load_result->HasError()) {
                    throw std::runtime_error("Failed to load data: " + load_result->GetError());
                }
                Log("Data loading complete.");
            } else {
                Log("hits table already exists, skipping creation and loading.");
            }

            // Ensure PAC is disabled
            con.Query("ALTER TABLE hits UNSET PU;");

            // Checkpoint to flush WAL before child opens DB
            con.Query("CHECKPOINT;");
        }
        // DB is now closed — child process will open it

        // Prepare output CSV
        string actual_out = out_csv;
        if (actual_out.empty()) {
            if (micro) {
                actual_out = "benchmark/clickbench_micro_results.csv";
            } else {
                actual_out = "benchmark/clickbench_results.csv";
            }
        }

        std::ofstream csv(actual_out, std::ofstream::out | std::ofstream::trunc);
        if (!csv.is_open()) {
            throw std::runtime_error("Failed to open output CSV: " + actual_out);
        }

        vector<BenchmarkQueryResult> all_results;

        // =====================================================================
        // Main benchmark loop using fork worker
        // =====================================================================
        Log("=== Starting per-run benchmark (fork-based) ===");

        int num_runs = 3;
        double timeout_s = 120.0;  // 2 minute timeout per query

        for (int run = 1; run <= num_runs; ++run) {
            Log(string("=== Run ") + std::to_string(run) + " of " + std::to_string(num_runs) + " ===");

            ForkWorker worker;
            worker.db_path = db_actual;
            worker.Start();

            for (size_t q = 0; q < queries.size(); ++q) {
                int qnum = static_cast<int>(q + 1);
                const string &query = queries[q];

                // Ensure worker is alive
                if (worker.child_pid <= 0) {
                    Log("Restarting child worker...");
                    worker.Start();
                }

                // ----------------------------------------------------------
                // Cold run (baseline, not recorded)
                // ----------------------------------------------------------
                {
                    worker.SendUnsetPAC();
                    Log(string("Q") + std::to_string(qnum) + " cold run");
                    auto res = worker.SubmitQuery(query, timeout_s);
                    if (res == ForkWorker::SR_CHILD_DIED) {
                        Log(string("Q") + std::to_string(qnum) + " cold run: child died, restarting");
                        worker.Start();
                    } else if (res == ForkWorker::SR_TIMEOUT) {
                        Log(string("Q") + std::to_string(qnum) + " cold run: timeout, restarting");
                        worker.Start();
                    }
                }

                // Ensure worker is alive for warm run
                if (worker.child_pid <= 0) {
                    worker.Start();
                }

                // ----------------------------------------------------------
                // Warm run (baseline, recorded)
                // ----------------------------------------------------------
                {
                    worker.SendUnsetPAC();

                    auto res = worker.SubmitQuery(query, timeout_s);

                    BenchmarkQueryResult result;
                    result.query_num = qnum;
                    result.mode = "baseline";
                    result.run = run;
                    result.time_ms = worker.result_time_ms;
                    result.success = (res == ForkWorker::SR_OK && worker.result_ok);
                    result.error_msg = worker.result_error;

                    all_results.push_back(result);

                    if (res == ForkWorker::SR_CHILD_DIED) {
                        Log(string("Q") + std::to_string(qnum) + " baseline run " + std::to_string(run) +
                            " CRASHED: " + worker.result_error);
                        worker.Start();
                    } else if (res == ForkWorker::SR_TIMEOUT) {
                        Log(string("Q") + std::to_string(qnum) + " baseline run " + std::to_string(run) +
                            " TIMEOUT: " + worker.result_error);
                        worker.Start();
                    } else if (result.success) {
                        Log(string("Q") + std::to_string(qnum) + " baseline run " + std::to_string(run) +
                            " time: " + FormatNumber(result.time_ms) + " ms");
                    } else {
                        Log(string("Q") + std::to_string(qnum) + " baseline run " + std::to_string(run) +
                            " ERROR: " + result.error_msg);
                    }
                }

                // Ensure worker is alive for PAC run
                if (worker.child_pid <= 0) {
                    worker.Start();
                }

                // ----------------------------------------------------------
                // PAC run (recorded)
                // ----------------------------------------------------------
                {
                    worker.SendSetupPAC(setup_stmts);

                    auto res = worker.SubmitQuery(query, timeout_s);

                    BenchmarkQueryResult result;
                    result.query_num = qnum;
                    result.mode = "PAC";
                    result.run = run;
                    result.time_ms = worker.result_time_ms;
                    result.success = (res == ForkWorker::SR_OK && worker.result_ok);
                    result.error_msg = worker.result_error;

                    all_results.push_back(result);

                    if (res == ForkWorker::SR_CHILD_DIED) {
                        Log(string("Q") + std::to_string(qnum) + " PAC run " + std::to_string(run) +
                            " CRASHED: " + worker.result_error);
                        worker.Start();
                    } else if (res == ForkWorker::SR_TIMEOUT) {
                        Log(string("Q") + std::to_string(qnum) + " PAC run " + std::to_string(run) +
                            " TIMEOUT: " + worker.result_error);
                        worker.Start();
                    } else if (result.success) {
                        Log(string("Q") + std::to_string(qnum) + " PAC run " + std::to_string(run) +
                            " time: " + FormatNumber(result.time_ms) + " ms");
                    } else {
                        if (result.error_msg.find("PAC") != string::npos ||
                            result.error_msg.find("privacy") != string::npos ||
                            result.error_msg.find("aggregat") != string::npos ||
                            result.error_msg.find("protected") != string::npos) {
                            Log(string("Q") + std::to_string(qnum) + " PAC run " + std::to_string(run) +
                                " REJECTED: " + result.error_msg);
                        } else {
                            Log(string("Q") + std::to_string(qnum) + " PAC run " + std::to_string(run) +
                                " ERROR: " + result.error_msg);
                        }
                    }

                    // Disable PAC after the run
                    if (worker.child_pid > 0) {
                        worker.SendUnsetPAC();
                    }
                }
            }

            // Worker is stopped automatically when it goes out of scope
        }

        // =====================================================================
        // Post-process: Detect unstable queries
        // =====================================================================
        std::set<int> unstable_queries;
        for (const auto &r : all_results) {
            if (r.mode == "PAC" && !r.success) {
                if (r.error_msg.find("sample diversity") != string::npos) {
                    unstable_queries.insert(r.query_num);
                }
            }
        }
        if (!unstable_queries.empty()) {
            Log("Detected unstable queries (sample diversity errors):");
            for (int qnum : unstable_queries) {
                Log(string("  Q") + std::to_string(qnum));
            }
            for (auto &r : all_results) {
                if (r.mode == "PAC" && unstable_queries.count(r.query_num) > 0) {
                    if (r.success) {
                        r.success = false;
                        r.error_msg = "Marked failed due to sample diversity instability";
                    }
                }
            }
        }

        // =====================================================================
        // Write CSV
        // =====================================================================
        csv << "query,mode,run,time_ms,success,error\n";
        for (const auto &r : all_results) {
            csv << r.query_num << "," << r.mode << "," << r.run << ","
                << FormatNumber(r.time_ms) << ","
                << (r.success ? "true" : "false") << ",\""
                << (r.error_msg.empty() ? "" : r.error_msg) << "\"\n";
        }
        csv.close();
        Log(string("Results written to: ") + actual_out);

        // =====================================================================
        // Print Statistics
        // =====================================================================
        Log("=== Benchmark Statistics ===");

        int total_queries = static_cast<int>(queries.size());

        int baseline_success = 0, baseline_failed = 0;
        double baseline_total_time = 0;
        for (const auto &r : all_results) {
            if (r.mode == "baseline") {
                if (r.success) { baseline_success++; baseline_total_time += r.time_ms; }
                else { baseline_failed++; }
            }
        }

        int pac_success = 0, pac_rejected = 0, pac_crashed = 0;
        double pac_total_time = 0;
        std::map<string, int> error_counts;

        for (const auto &r : all_results) {
            if (r.mode == "PAC") {
                if (r.success) {
                    pac_success++;
                    pac_total_time += r.time_ms;
                } else {
                    string error_category;
                    if (r.error_msg.find("sample diversity") != string::npos) {
                        error_category = "sample diversity instability";
                        pac_rejected++;
                    } else if (r.error_msg.find("protected column") != string::npos) {
                        error_category = "protected column access";
                        pac_rejected++;
                    } else if (r.error_msg.find("pac_min not implemented") != string::npos ||
                               r.error_msg.find("pac_max not implemented") != string::npos) {
                        error_category = "pac_min/max not implemented for VARCHAR";
                        pac_crashed++;
                    } else if (r.error_msg.find("PAC") != string::npos ||
                               r.error_msg.find("privacy") != string::npos ||
                               r.error_msg.find("aggregat") != string::npos) {
                        error_category = "other PAC rejection";
                        pac_rejected++;
                    } else if (r.error_msg.find("timeout") != string::npos ||
                               r.error_msg.find("memory limit") != string::npos) {
                        error_category = "timeout/OOM";
                        pac_crashed++;
                    } else if (r.error_msg.find("child process died") != string::npos) {
                        error_category = "child crash (OOM/signal)";
                        pac_crashed++;
                    } else {
                        error_category = "other error";
                        pac_crashed++;
                    }
                    error_counts[error_category]++;
                }
            }
        }

        double baseline_avg_success_per_run = static_cast<double>(baseline_success) / num_runs;
        double baseline_avg_failed_per_run = static_cast<double>(baseline_failed) / num_runs;
        double baseline_avg_time_per_run = baseline_total_time / num_runs;

        double pac_avg_success_per_run = static_cast<double>(pac_success) / num_runs;
        double pac_avg_rejected_per_run = static_cast<double>(pac_rejected) / num_runs;
        double pac_avg_crashed_per_run = static_cast<double>(pac_crashed) / num_runs;
        double pac_avg_time_per_run = pac_total_time / num_runs;

        Log(string("Total queries: ") + std::to_string(total_queries));
        Log(string("Number of runs: ") + std::to_string(num_runs));

        Log("--- Baseline (per run average) ---");
        Log(string("  Successful queries: ") + FormatNumber(baseline_avg_success_per_run));
        Log(string("  Failed queries: ") + FormatNumber(baseline_avg_failed_per_run));
        Log(string("  Total time: ") + FormatNumber(baseline_avg_time_per_run) + " ms");
        if (baseline_success > 0) {
            Log(string("  Avg time per successful query: ") +
                FormatNumber(baseline_total_time / baseline_success) + " ms");
        }

        Log("--- PAC (per run average) ---");
        Log(string("  Successful queries: ") + FormatNumber(pac_avg_success_per_run));
        Log(string("  Rejected (privacy violations): ") + FormatNumber(pac_avg_rejected_per_run));
        Log(string("  Crashed/errors: ") + FormatNumber(pac_avg_crashed_per_run));
        Log(string("  Total time (successful): ") + FormatNumber(pac_avg_time_per_run) + " ms");
        if (pac_success > 0) {
            Log(string("  Avg time per successful query: ") +
                FormatNumber(pac_total_time / pac_success) + " ms");
        }

        if (!error_counts.empty()) {
            Log("--- Error breakdown (per run average) ---");
            for (const auto &ec : error_counts) {
                double avg_count = static_cast<double>(ec.second) / num_runs;
                Log(string("  ") + ec.first + ": " + FormatNumber(avg_count));
            }
        }

        // =====================================================================
        // Per-query slowdown
        // =====================================================================
        {
            std::map<int, double> baseline_total_per_q, pac_total_per_q;
            std::map<int, int> baseline_count_per_q, pac_count_per_q;
            for (const auto &r : all_results) {
                if (!r.success) continue;
                if (r.mode == "baseline") {
                    baseline_total_per_q[r.query_num] += r.time_ms;
                    baseline_count_per_q[r.query_num]++;
                } else if (r.mode == "PAC") {
                    pac_total_per_q[r.query_num] += r.time_ms;
                    pac_count_per_q[r.query_num]++;
                }
            }

            Log("--- Per-query slowdown (PAC / baseline) ---");
            double sum_slowdown = 0;
            int n_compared = 0;
            double worst_slowdown = 0;
            int worst_query = 0;
            for (auto &kv : baseline_total_per_q) {
                int qnum = kv.first;
                if (pac_count_per_q.count(qnum) == 0 || baseline_count_per_q[qnum] == 0) continue;
                double baseline_mean = kv.second / baseline_count_per_q[qnum];
                double pac_mean = pac_total_per_q[qnum] / pac_count_per_q[qnum];
                double slowdown = (baseline_mean > 0) ? pac_mean / baseline_mean : 0;
                Log(string("  Q") + std::to_string(qnum) + ": " +
                    FormatNumber(baseline_mean) + " ms -> " + FormatNumber(pac_mean) + " ms (" +
                    FormatNumber(slowdown) + "x)");
                sum_slowdown += slowdown;
                n_compared++;
                if (slowdown > worst_slowdown) {
                    worst_slowdown = slowdown;
                    worst_query = qnum;
                }
            }
            if (n_compared > 0) {
                double avg_slowdown = sum_slowdown / n_compared;
                Log(string("  Average slowdown: ") + FormatNumber(avg_slowdown) + "x over " +
                    std::to_string(n_compared) + " queries");
                Log(string("  Worst slowdown: ") + FormatNumber(worst_slowdown) + "x (Q" +
                    std::to_string(worst_query) + ")");
            }
        }

        Log("=== Benchmark Complete ===");
        return 0;

    } catch (std::exception &ex) {
        Log(string("Error running benchmark: ") + ex.what());
        return 2;
    }
}

} // namespace duckdb

// Helper for printing usage
static void PrintUsageMain() {
    std::cout << "Usage: pac_clickhouse_benchmark [options]\n"
              << "Options:\n"
              << "  --micro           Run with a smaller dataset (5000000 rows) for quick testing\n"
              << "                    Uses clickbench_micro.db by default\n"
              << "  --db <path>       DuckDB database file (default: clickbench.db or clickbench_micro.db)\n"
              << "  --queries <dir>   Directory containing create.sql, load.sql, queries.sql\n"
              << "                    (default: benchmark/clickbench/clickbench_queries)\n"
              << "  --out <csv>       Output CSV path (default: benchmark/clickbench_results.csv)\n"
              << "  -h, --help        Show this help message\n";
}

int main(int argc, char **argv) {
    bool micro = false;
    std::string db_path;
    std::string queries_dir = "benchmark/clickbench/clickbench_queries";
    std::string out_csv;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            PrintUsageMain();
            return 0;
        } else if (arg == "--micro") {
            micro = true;
        } else if (arg == "--db" && i + 1 < argc) {
            db_path = argv[++i];
        } else if (arg == "--queries" && i + 1 < argc) {
            queries_dir = argv[++i];
        } else if (arg == "--out" && i + 1 < argc) {
            out_csv = argv[++i];
        } else {
            std::cerr << "Unknown option: " << arg << "\n";
            PrintUsageMain();
            return 1;
        }
    }

    return duckdb::RunClickHouseBenchmark(db_path, queries_dir, out_csv, micro);
}
