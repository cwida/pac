//
// Created by ila on 12/12/25.
//

#include "include/pac_compiler.hpp"
#include "include/pac_privacy_unit.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <unordered_set>

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

    // Build output filename from configured pac_compiled_path and privacy unit + hash
    std::string compiled_path = ".";
    Value path_val;
    if (input.context.TryGetCurrentSetting("pac_compiled_path", path_val) && !path_val.IsNull()) {
        compiled_path = path_val.ToString();
    }
    if (!compiled_path.empty() && compiled_path.back() != '/') { compiled_path.push_back('/'); }

    std::string filename = compiled_path + privacy_unit + "_" + hash_str + ".sql";

    // Call CreateSampleCTE to emit the CTE SQL file for the provided privacy unit (now takes filename)
    CreateSampleCTE(input.context, privacy_unit, filename, normalized);

    // Also rewrite the logical plan to join with the generated per-sample table so the plan includes pac_sample
    JoinWithSampleTable(input.context, plan);
}

void CreateSampleCTE(ClientContext &context, const std::string &privacy_unit,
                     const std::string &filename, const std::string &query_normalized) {

    // TODO - for now we assume native DuckDB SQL for the sample CTE
    // If we want to support other backends, we need to either figure out their row id, or compile a ROW_NUMBER() version

    // Read pac_m setting
    int64_t m_cfg = 128;
    Value m_val;
    if (context.TryGetCurrentSetting("pac_m", m_val) && !m_val.IsNull()) {
        m_cfg = m_val.GetValue<int64_t>();
        if (m_cfg <= 0) { m_cfg = 128; }
    }

    std::ofstream ofs(filename);
    if (!ofs) {
        throw ParserException("PAC compilation: failed to open file for writing: " + filename);
    }

    ofs << "-- PAC compiled sample CTE for privacy unit: " << privacy_unit << "\n";
    ofs << "-- normalized query: " << query_normalized << "\n\n";

    ofs << "WITH pac_sample AS (\n";
    ofs << "  SELECT src.rowid, s.sample_id\n";
    ofs << "  FROM " << privacy_unit << " AS src\n";
    ofs << "  CROSS JOIN generate_series(1," << m_cfg << ") AS s(sample_id)\n";
    ofs << ")\n";
    // ofs << "SELECT * FROM pac_sample;\n";

    ofs.close();

}

// Helper: create a minimal LogicalGet node that represents the pac_sample table
// If an internal helper table (_pac_internal_sample_<sanitized>) exists, bind to it using its scan function
static unique_ptr<LogicalGet> CreatePacSampleLogicalGet(ClientContext &context, idx_t table_index, const string &privacy_table_name) {
    // Sanitize privacy table name
    auto SanitizeName = [&](const string &in) {
        string out;
        out.reserve(in.size());
        for (char c : in) {
            if (std::isalnum((unsigned char)c) || c == '_') {
                out.push_back(c);
            } else {
                out.push_back('_');
            }
        }
        return out;
    };
    string internal_name = "_pac_internal_sample_" + SanitizeName(privacy_table_name);

    // Try to find the internal table in the catalog via the current search path
    Catalog &catalog = Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));
    CatalogSearchPath search_path(context);
    for (auto &schema_entry : search_path.Get()) {
        auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema_entry.schema, internal_name, OnEntryNotFound::RETURN_NULL);
        if (entry) {
            // Found the table catalog entry; construct a bound LogicalGet for it
            auto &table = entry->Cast<TableCatalogEntry>();
            // Build a simple LogicalGet using the catalog column metadata. We avoid calling
            // the table's scan function and virtual column handlers here because for the
            // purposes of plan rewriting / explain we only need the column names/types.
            vector<LogicalType> return_types;
            vector<string> return_names;
            for (auto &col : table.GetColumns().Logical()) {
                return_types.push_back(col.Type());
                return_names.push_back(col.Name());
            }
            // Use the public LogicalGet constructor: supply a default TableFunction and nullptr bind_data
            TableFunction table_function; // default-initialized placeholder
            unique_ptr<FunctionData> bind_data = nullptr;
            virtual_column_map_t virtual_columns; // empty
            auto logical_get = make_uniq<LogicalGet>(table_index, table_function, std::move(bind_data), std::move(return_types), std::move(return_names), std::move(virtual_columns));
            return logical_get;
        }
    }

    // The internal helper table must exist. Signal an error if not found so callers
    // (e.g., PRAGMA add_privacy_unit) must create it before compilation.
    throw ParserException("PAC compilation: internal helper table not found: " + internal_name + " - call PRAGMA add_pac_privacy_unit('<table>') to create it");
}

// Recursive transformer: returns the pac_sample table_index found in this subtree, or DConstants::INVALID_INDEX if none
static idx_t TransformAddSampleJoin(ClientContext &context, unique_ptr<LogicalOperator> &op,
                                    const unordered_set<string> &pac_tables, idx_t &next_table_index) {
    if (!op) {
        return DConstants::INVALID_INDEX;
    }

    idx_t found_pac_index = DConstants::INVALID_INDEX;
    // Iterate children by reference so we can replace them
    for (auto &child : op->children) {
        if (!child) {
            continue;
        }
        // If the child is a LogicalGet, check if it scans a PAC table
        if (child->type == LogicalOperatorType::LOGICAL_GET) {
            auto &lg = child->Cast<LogicalGet>();
            bool is_pac = false;
            optional_ptr<TableCatalogEntry> table_entry = lg.GetTable();
            if (table_entry) {
                if (pac_tables.count(table_entry->name) > 0) {
                    is_pac = true;
                }
            } else {
                for (auto &n : lg.names) {
                    if (pac_tables.count(n) > 0) {
                        is_pac = true;
                        break;
                    }
                }
            }
            string privacy_table_name;
            if (table_entry) {
                privacy_table_name = table_entry->name;
            } else {
                // fallback: find which name in the LogicalGet matched pac_tables
                for (auto &n : lg.names) {
                    if (pac_tables.count(n) > 0) {
                        privacy_table_name = n;
                        break;
                    }
                }
            }
            if (is_pac) {
                // Capture left child (the original scan)
                // capture the table index before moving the child unique_ptr
                idx_t left_table_idx = lg.table_index;
                unique_ptr<LogicalOperator> left_child = std::move(child);
                // Create right child: a LogicalGet representing pac_sample
                idx_t pac_idx = next_table_index++;
                // privacy_table_name must be set here
                auto right_get = CreatePacSampleLogicalGet(context, pac_idx, privacy_table_name);

                // Build join condition: left.rowid = pac_sample.rowid
                JoinCondition cond;
                cond.comparison = ExpressionType::COMPARE_EQUAL;
                // left: use the special ROW_ID identifier as column index
                cond.left = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT,
                    ColumnBinding(left_table_idx, COLUMN_IDENTIFIER_ROW_ID));
                // right: pac_sample.rowid is column 0
                cond.right = make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, ColumnBinding(pac_idx, 0));

                vector<JoinCondition> conditions;
                conditions.push_back(std::move(cond));

                // Create the comparison join
                vector<unique_ptr<Expression>> arbitrary_expressions;
                auto join_op = LogicalComparisonJoin::CreateJoin(context, JoinType::INNER, JoinRefType::REGULAR,
                                                                 std::move(left_child), std::move(right_get),
                                                                 std::move(conditions), std::move(arbitrary_expressions));
                // Replace the child with the new join operator
                child = std::move(join_op);

                // mark that this subtree contains pac_sample with the new index
                found_pac_index = pac_idx;
                continue; // move to next child
            }
        }
        // Recurse into child
        idx_t child_pac_index = TransformAddSampleJoin(context, child, pac_tables, next_table_index);
        if (child_pac_index != DConstants::INVALID_INDEX) {
            // propagate upwards: if we haven't yet recorded a pac index for this operator, capture it
            if (found_pac_index == DConstants::INVALID_INDEX) {
                found_pac_index = child_pac_index;
            }
        }
    }

    // If this operator is an aggregate and one of its children contains pac_sample, extend the grouping keys
    if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && found_pac_index != DConstants::INVALID_INDEX) {
        auto &aggr = op->Cast<LogicalAggregate>();
        // Check if sample_id is already present as a group
        bool already = false;
        for (auto &g : aggr.groups) {
            if (!g) {
                continue;
            }
            if (g->type == ExpressionType::BOUND_COLUMN_REF) {
                auto &bcr = g->Cast<BoundColumnRefExpression>();
                if (bcr.binding.table_index == found_pac_index && bcr.binding.column_index == 1) {
                    already = true;
                    break;
                }
            }
        }
        if (!already) {
            // Add sample_id as a grouping column (BIGINT)
            aggr.groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, ColumnBinding(found_pac_index, 1)));
        }
    }

    return found_pac_index;
}

void JoinWithSampleTable(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
    if (!plan) {
        return;
    }

    // Load pac tables file (same logic as PACRewriteRule)
    string pac_privacy_file = "pac_tables.csv";
    Value pac_privacy_file_value;
    context.TryGetCurrentSetting("pac_privacy_file", pac_privacy_file_value);
    if (!pac_privacy_file_value.IsNull()) {
        pac_privacy_file = pac_privacy_file_value.ToString();
    }
    auto pac_tables = ReadPacTablesFile(pac_privacy_file);
    if (pac_tables.empty()) {
        return;
    }

    // Determine next available table index by scanning existing operators
    idx_t max_index = DConstants::INVALID_INDEX;
    vector<unique_ptr<LogicalOperator> *> stack;
    stack.push_back(&plan);
    while (!stack.empty()) {
        auto cur_ptr = stack.back();
        stack.pop_back();
        auto &cur = *cur_ptr;
        if (!cur) {
            continue;
        }
        // collect table indexes
        auto tbls = cur->GetTableIndex();
        for (auto t : tbls) {
            if (t != DConstants::INVALID_INDEX && (max_index == DConstants::INVALID_INDEX || t > max_index)) {
                max_index = t;
            }
        }
        for (auto &c : cur->children) {
            stack.push_back(&c);
        }
    }
    idx_t next_index = (max_index == DConstants::INVALID_INDEX) ? 0 : (max_index + 1);

    // Run transformation
    TransformAddSampleJoin(context, plan, pac_tables, next_index);
	plan->Verify(context);
	Printer::Print("optimized plan after JoinWithSampleTable:\n");
}

} // namespace duckdb
