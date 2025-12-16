//
// Created by ila on 12/12/25.
//

#include "include/pac_compiler.hpp"
#include "include/pac_privacy_unit.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>

namespace duckdb {

static std::string NormalizeQueryForHash(const std::string &query) {
    std::string s = query;
    // replace newlines and carriage returns with spaces
    std::replace(s.begin(), s.end(), '\n', ' ');
    std::replace(s.begin(), s.end(), '\r', ' ');
    // convert to lowercase
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    // collapse multiple whitespace into single space
    std::string out;
    out.reserve(s.size());
    bool in_space = false;
    for (char c : s) {
        if (std::isspace((unsigned char)c)) {
            if (!in_space) {
                out.push_back(' ');
                in_space = true;
            }
        } else {
            out.push_back(c);
            in_space = false;
        }
    }
    // trim leading/trailing spaces
    if (!out.empty() && out.front() == ' ') { out.erase(out.begin()); }
    if (!out.empty() && out.back() == ' ') { out.pop_back(); }
    return out;
}

void CompilePACQuery(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                     const std::string &privacy_unit) {
    // The optimizer is responsible for detecting the privacy unit(s) and ensuring only one is present.
    // This function assumes `privacy_unit` is a single non-empty privacy unit name. If it's empty, nothing to do.
    if (privacy_unit.empty()) {
        return;
    }

    // Obtain original query text
    const std::string &orig_query = input.context.GetCurrentQuery();
    std::string normalized = NormalizeQueryForHash(orig_query);

    // compute a simple hash (std::hash) and turn into hex
    size_t h = std::hash<std::string>{}(normalized);
    std::stringstream ss;
    ss << std::hex << h;
    std::string hash_str = ss.str();

    // Call CreateSampleCTE to emit the CTE SQL file for the provided privacy unit
    CreateSampleCTE(input.context, privacy_unit, normalized, hash_str);
}

std::string CreateSampleCTE(ClientContext &context, const std::string &privacy_unit,
                                       const std::string &query_normalized, const std::string &query_hash) {
    // Read pac_m setting
    int64_t m_cfg = 128;
    Value m_val;
    if (context.TryGetCurrentSetting("pac_m", m_val) && !m_val.IsNull()) {
        m_cfg = m_val.GetValue<int64_t>();
        if (m_cfg <= 0) { m_cfg = 128; }
    }
    // Read compiled path setting
    std::string compiled_path = ".";
    Value path_val;
    if (context.TryGetCurrentSetting("pac_compiled_path", path_val) && !path_val.IsNull()) {
        compiled_path = path_val.ToString();
    }
    if (!compiled_path.empty() && compiled_path.back() != '/') { compiled_path.push_back('/'); }

    std::string filename = compiled_path + privacy_unit + "_" + query_hash + ".sql";

    std::ofstream ofs(filename);
    if (!ofs) {
        throw ParserException("PAC compilation: failed to open file for writing: " + filename);
    }

    // Emit commented original normalized query for reference and a sample CTE template
    ofs << "-- PAC compiled sample CTE for privacy unit: " << privacy_unit << "\n";
    ofs << "-- normalized query: " << query_normalized << "\n\n";

    ofs << "WITH pac_sample AS (\n";
    ofs << "  -- Generated sample CTE (template). This will need adaptation for your schema.\n";
    ofs << "  SELECT src.*, s.sample_id\n";
    ofs << "  FROM " << privacy_unit << " AS src\n";
    ofs << "  CROSS JOIN generate_series(1," << m_cfg << ") AS s(sample_id)\n";
    ofs << ")\n";
    ofs << "SELECT * FROM pac_sample;\n";

    ofs.close();
    return filename;
}

} // namespace duckdb
