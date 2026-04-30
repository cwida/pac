#include "utils/pac_helpers.hpp"

#include <sstream>
#include <functional>
#include <cctype>
#include <duckdb/optimizer/column_binding_replacer.hpp>
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <queue>

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "core/pac_optimizer.hpp"
#include "parser/pac_parser.hpp"
#include "duckdb/planner/binder.hpp"

using idx_set = std::unordered_set<idx_t>;

namespace duckdb {

string Sanitize(const string &in) {
	string out;
	for (char c : in) {
		out.push_back(std::isalnum(static_cast<unsigned char>(c)) || c == '_' ? c : '_');
	}
	return out;
}

string NormalizeQueryForHash(const string &query) {
	string s = query;
	std::replace(s.begin(), s.end(), '\n', ' ');
	std::replace(s.begin(), s.end(), '\r', ' ');
	std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
	string out;
	out.reserve(s.size());
	bool in_space = false;
	for (char c : s) {
		if (std::isspace(static_cast<unsigned char>(c))) {
			if (!in_space) {
				out.push_back(' ');
				in_space = true;
			}
		} else {
			out.push_back(c);
			in_space = false;
		}
	}
	// trim
	if (!out.empty() && out.front() == ' ') {
		out.erase(out.begin());
	}
	if (!out.empty() && out.back() == ' ') {
		out.pop_back();
	}
	return out;
}

string HashStringToHex(const string &input) {
	size_t h = std::hash<string> {}(input);
	std::ostringstream ss;
	ss << std::hex << h;
	return ss.str();
}

idx_t GetNextTableIndex(unique_ptr<LogicalOperator> &plan) {
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
	return (max_index == DConstants::INVALID_INDEX) ? 0 : (max_index + 1);
}

void CollectTableIndicesRecursive(LogicalOperator *node, idx_set &out) {
	if (!node) {
		return;
	}
	auto tbls = node->GetTableIndex();
	for (auto t : tbls) {
		if (t != DConstants::INVALID_INDEX) {
			out.insert(t);
		}
	}
	for (auto &c : node->children) {
		CollectTableIndicesRecursive(c.get(), out);
	}
}

static void CollectTableIndicesExcluding(LogicalOperator *node, LogicalOperator *skip, idx_set &out) {
	if (!node) {
		return;
	}
	if (node == skip) {
		return; // skip this subtree entirely
	}
	auto tbls = node->GetTableIndex();
	for (auto t : tbls) {
		if (t != DConstants::INVALID_INDEX) {
			out.insert(t);
		}
	}
	for (auto &c : node->children) {
		CollectTableIndicesExcluding(c.get(), skip, out);
	}
}

void ApplyIndexMapToSubtree(LogicalOperator *node, const std::unordered_map<idx_t, idx_t> &map) {
	if (!node) {
		return;
	}
	// Update operator-specific index fields where applicable
	if (auto get_ptr = dynamic_cast<LogicalGet *>(node)) {
		if (map.find(get_ptr->table_index) != map.end()) {
			get_ptr->table_index = map.at(get_ptr->table_index);
		}
	} else if (auto proj_ptr = dynamic_cast<LogicalProjection *>(node)) {
		if (map.find(proj_ptr->table_index) != map.end()) {
			proj_ptr->table_index = map.at(proj_ptr->table_index);
		}
	} else if (auto setop_ptr = dynamic_cast<LogicalSetOperation *>(node)) {
		if (map.find(setop_ptr->table_index) != map.end()) {
			setop_ptr->table_index = map.at(setop_ptr->table_index);
		}
	} else if (auto insert_ptr = dynamic_cast<LogicalInsert *>(node)) {
		if (map.find(insert_ptr->table_index) != map.end()) {
			insert_ptr->table_index = map.at(insert_ptr->table_index);
		}
	} else if (auto dummy_ptr = dynamic_cast<LogicalDummyScan *>(node)) {
		if (map.find(dummy_ptr->table_index) != map.end()) {
			dummy_ptr->table_index = map.at(dummy_ptr->table_index);
		}
	} else if (auto coldata_ptr = dynamic_cast<LogicalColumnDataGet *>(node)) {
		if (map.find(coldata_ptr->table_index) != map.end()) {
			coldata_ptr->table_index = map.at(coldata_ptr->table_index);
		}
	} else if (auto upd_ptr = dynamic_cast<LogicalUpdate *>(node)) {
		if (map.find(upd_ptr->table_index) != map.end()) {
			upd_ptr->table_index = map.at(upd_ptr->table_index);
		}
	} else if (auto del_ptr = dynamic_cast<LogicalDelete *>(node)) {
		if (map.find(del_ptr->table_index) != map.end()) {
			del_ptr->table_index = map.at(del_ptr->table_index);
		}
	} else if (auto cte_ptr = dynamic_cast<LogicalCTE *>(node)) {
		if (map.find(cte_ptr->table_index) != map.end()) {
			cte_ptr->table_index = map.at(cte_ptr->table_index);
		}
	} else if (auto rcte_ptr = dynamic_cast<LogicalRecursiveCTE *>(node)) {
		if (map.find(rcte_ptr->table_index) != map.end()) {
			rcte_ptr->table_index = map.at(rcte_ptr->table_index);
		}
	} else if (auto eg_ptr = dynamic_cast<LogicalExpressionGet *>(node)) {
		if (map.find(eg_ptr->table_index) != map.end()) {
			eg_ptr->table_index = map.at(eg_ptr->table_index);
		}
	} else if (auto cteref_ptr = dynamic_cast<LogicalCTERef *>(node)) {
		if (map.find(cteref_ptr->table_index) != map.end()) {
			cteref_ptr->table_index = map.at(cteref_ptr->table_index);
		}
	} else if (auto dg_ptr = dynamic_cast<LogicalDelimGet *>(node)) {
		if (map.find(dg_ptr->table_index) != map.end()) {
			dg_ptr->table_index = map.at(dg_ptr->table_index);
		}
	} else if (auto unnest_ptr = dynamic_cast<LogicalUnnest *>(node)) {
		if (map.find(unnest_ptr->unnest_index) != map.end()) {
			unnest_ptr->unnest_index = map.at(unnest_ptr->unnest_index);
		}
	}

	// Special handling for LogicalAggregate: multiple indices
	if (auto agg_ptr = dynamic_cast<LogicalAggregate *>(node)) {
		if (map.find(agg_ptr->aggregate_index) != map.end()) {
			agg_ptr->aggregate_index = map.at(agg_ptr->aggregate_index);
		}
		if (map.find(agg_ptr->group_index) != map.end()) {
			agg_ptr->group_index = map.at(agg_ptr->group_index);
		}
		if (agg_ptr->groupings_index != DConstants::INVALID_INDEX && map.find(agg_ptr->groupings_index) != map.end()) {
			agg_ptr->groupings_index = map.at(agg_ptr->groupings_index);
		}
	}

	// Recurse
	for (auto &c : node->children) {
		ApplyIndexMapToSubtree(c.get(), map);
	}
}

void ReplaceNode(unique_ptr<LogicalOperator> &root, unique_ptr<LogicalOperator> &old_node,
                 unique_ptr<LogicalOperator> &new_node, Binder *binder) {
	// Validate inputs
	if (!old_node) {
		throw InternalException("ReplaceNode: old_node must not be null");
	}
	if (!new_node) {
		throw InternalException("ReplaceNode: new_node must not be null");
	}

	// Keep pointer to the old subtree (before destruction)
	LogicalOperator *old_subtree_ptr = old_node.get();
	if (!old_subtree_ptr) {
		throw InternalException("ReplaceNode: referenced old subtree is null");
	}

	// Collect top-level bindings from the old subtree (these are the bindings advertised by the old child)
	vector<ColumnBinding> old_top_bindings = old_subtree_ptr->GetColumnBindings();

	// Collect all table indices present in the old subtree so we can exclude them when computing external indices
	idx_set old_subtree_indices;
	CollectTableIndicesRecursive(old_subtree_ptr, old_subtree_indices);

	// Compute external indices (everything in the plan except the old subtree)
	idx_set external_indices;
	CollectTableIndicesExcluding(root.get(), old_subtree_ptr, external_indices);

	// Replace the slot with the new node (this destroys the old subtree)
	old_node = std::move(new_node);
	LogicalOperator *subtree_root = old_node.get();
	if (!subtree_root) {
		throw InternalException("ReplaceNode: inserted subtree is null after move");
	}

	// Collect table indices present in the newly inserted subtree
	idx_set new_subtree_indices;
	CollectTableIndicesRecursive(subtree_root, new_subtree_indices);

	// Build index remapping for any new-subtree indices that collide with external indices
	std::unordered_map<idx_t, idx_t> index_map;
	if (!new_subtree_indices.empty()) {
		idx_t next_idx = binder ? binder->GenerateTableIndex() : GetNextTableIndex(root);
		for (auto idx : new_subtree_indices) {
			if (idx == DConstants::INVALID_INDEX) {
				continue;
			}
			if (external_indices.find(idx) != external_indices.end()) {
				// find a fresh index not in external_indices and not in new_subtree_indices
				while (external_indices.find(next_idx) != external_indices.end() ||
				       new_subtree_indices.find(next_idx) != new_subtree_indices.end()) {
					next_idx = binder ? binder->GenerateTableIndex() : next_idx + 1;
				}
				index_map[idx] = next_idx;
				external_indices.insert(next_idx);
				next_idx = binder ? binder->GenerateTableIndex() : next_idx + 1;
			}
		}
	}

	// Apply index remapping to the inserted subtree if necessary
	if (!index_map.empty()) {
		ApplyIndexMapToSubtree(subtree_root, index_map);
	}

	// Build binding replacements based on index_map (table index remappings).
	// Only bindings whose table_index was remapped need replacement.
	// This is correct regardless of binding order in the new subtree
	// (e.g., when join children are swapped, positional matching would be wrong).
	ColumnBindingReplacer replacer;
	if (!index_map.empty()) {
		for (auto &old_b : old_top_bindings) {
			auto it = index_map.find(old_b.table_index);
			if (it != index_map.end()) {
				ColumnBinding new_b(it->second, old_b.column_index);
				replacer.replacement_bindings.emplace_back(old_b, new_b);
			}
		}
	}

	if (!replacer.replacement_bindings.empty()) {
		replacer.stop_operator = subtree_root;
		replacer.VisitOperator(*root);
	}
}

// Find PAC_KEY column names for the given table.
// Only uses PAC metadata (no database PRIMARY KEY detection).
// Returns empty vector when no PAC_KEY is defined.
vector<string> FindPacKey(ClientContext &context, const string &table_name) {
	Catalog &catalog = Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));

	// Helper that checks a vector of column names exist and are numeric; returns the vector if valid,
	// otherwise returns empty vector.
	auto check_and_return_numeric_columns = [&](TableCatalogEntry &table_entry,
	                                            const vector<string> &cols) -> vector<string> {
		vector<string> out;
		out.reserve(cols.size());
		for (auto &col_name : cols) {
			string tmp = col_name;
			try {
				auto col_idx = table_entry.GetColumnIndex(tmp);
				auto &col = table_entry.GetColumn(col_idx);
				if (!col.Type().IsNumeric()) {
					return {};
				}
				out.push_back(col.GetName());
			} catch (...) {
				return {};
			}
		}
		return out;
	};

	// Extract unqualified table name for PAC metadata lookup
	string unqualified = table_name;
	auto dot_pos = table_name.find('.');
	if (dot_pos != string::npos) {
		unqualified = table_name.substr(dot_pos + 1);
	}

	auto &metadata_mgr = PACMetadataManager::Get();
	auto *pac_meta = metadata_mgr.GetTableMetadata(unqualified);
	if (!pac_meta || pac_meta->primary_key_columns.empty()) {
		return {};
	}

	// Validate columns exist and are numeric by looking up the table in the catalog
	if (dot_pos != string::npos) {
		string schema = table_name.substr(0, dot_pos);
		auto entry =
		    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema, unqualified, OnEntryNotFound::RETURN_NULL);
		if (entry) {
			return check_and_return_numeric_columns(entry->Cast<TableCatalogEntry>(), pac_meta->primary_key_columns);
		}
	} else {
		CatalogSearchPath path(context);
		for (auto &entry_path : path.Get()) {
			auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, entry_path.schema, table_name,
			                              OnEntryNotFound::RETURN_NULL);
			if (entry) {
				return check_and_return_numeric_columns(entry->Cast<TableCatalogEntry>(),
				                                        pac_meta->primary_key_columns);
			}
		}
	}

	return {};
}

// Find PAC_LINK relationships declared on the given table.
// Returns a vector of (referenced_table_name, local_column_names) pairs.
// Only uses PAC metadata (no database FOREIGN KEY detection).
vector<std::pair<string, vector<string>>> FindPacLinks(ClientContext &context, const string &table_name) {
	vector<std::pair<string, vector<string>>> result;

	// Extract unqualified table name for PAC metadata lookup
	string unqualified_table_name = table_name;
	auto dot_pos = table_name.find('.');
	if (dot_pos != string::npos) {
		unqualified_table_name = table_name.substr(dot_pos + 1);
	}

	auto &metadata_mgr = PACMetadataManager::Get();
	auto *pac_metadata = metadata_mgr.GetTableMetadata(unqualified_table_name);

	if (pac_metadata) {
		for (auto &link : pac_metadata->links) {
			result.emplace_back(link.referenced_table, link.local_columns);
		}
	}

	return result;
}

// Find the referenced columns on the parent table for a specific PAC_LINK relationship.
// Looks up PAC_LINK metadata on `table_name` that references `ref_table` and returns
// the referenced_columns from that link.
vector<string> FindReferencedPKColumns(ClientContext &context, const string &table_name, const string &ref_table) {
	string ref_lower = StringUtil::Lower(ref_table);

	// Extract unqualified table name for PAC metadata lookup
	string unqualified = table_name;
	auto dot_pos = table_name.find('.');
	if (dot_pos != string::npos) {
		unqualified = table_name.substr(dot_pos + 1);
	}

	auto &metadata_mgr = PACMetadataManager::Get();
	auto *pac_meta = metadata_mgr.GetTableMetadata(unqualified);
	if (pac_meta) {
		for (auto &link : pac_meta->links) {
			if (StringUtil::Lower(link.referenced_table) == ref_lower) {
				return link.referenced_columns;
			}
		}
	}

	return {};
}

// Find PAC_LINK path(s) from any of `table_names` to any of `privacy_units`.
// Performs a BFS over outgoing PAC_LINK edges to find the shortest path from each start
// table to any privacy unit. Returns a map from the original start table string to the path
// (vector of table names from start to privacy unit inclusive).
std::unordered_map<string, vector<string>> FindPacLinkPath(ClientContext &context, const vector<string> &privacy_units,
                                                           const vector<string> &table_names) {
	Catalog &catalog = Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));

	auto ResolveQualified = [&](const string &tbl_name) -> string {
		// If schema-qualified name is provided (schema.table), prefer that exact lookup but return
		// only the unqualified table name (tbl).
		auto dot_pos = tbl_name.find('.');
		if (dot_pos != string::npos) {
			string schema = tbl_name.substr(0, dot_pos);
			string tbl = tbl_name.substr(dot_pos + 1);
			auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema, tbl, OnEntryNotFound::RETURN_NULL);
			if (entry) {
				return StringUtil::Lower(tbl); // return unqualified table name, normalized to lowercase
			}
			return StringUtil::Lower(tbl_name); // fallback to original, normalized
		}
		// Non-qualified: walk the search path; if found return unqualified table name
		CatalogSearchPath path(context);
		for (auto &entry_path : path.Get()) {
			auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, entry_path.schema, tbl_name,
			                              OnEntryNotFound::RETURN_NULL);
			if (entry) {
				return StringUtil::Lower(tbl_name); // already unqualified, normalized to lowercase
			}
		}
		return StringUtil::Lower(tbl_name); // fallback, normalized to lowercase
	};

	// canonicalize privacy units (use unqualified names, normalized to lowercase)
	std::unordered_set<string> privacy_set;
	for (auto &pu : privacy_units) {
		privacy_set.insert(ResolveQualified(pu));
	}

	std::unordered_map<string, vector<string>> result;

	for (auto &start : table_names) {
		string start_name = ResolveQualified(start);
		// BFS queue of unqualified table names (all lowercase)
		std::queue<string> q;
		std::unordered_set<string> visited;
		std::unordered_map<string, string> parent;

		visited.insert(start_name);
		q.push(start_name);

		bool found = false;
		string found_target;

		while (!q.empty() && !found) {
			string cur = q.front();
			q.pop();
			// Find outgoing FK edges from cur
			auto fks = FindPacLinks(context, cur);
			for (auto &p : fks) {
				string neighbor = p.first; // referenced table name (unqualified now)
				string neighbor_name = StringUtil::Lower(ResolveQualified(neighbor)); // normalize to lowercase
				if (visited.find(neighbor_name) != visited.end()) {
					continue;
				}
				visited.insert(neighbor_name);
				parent[neighbor_name] = cur;
				if (privacy_set.find(neighbor_name) != privacy_set.end()) {
					found = true;
					found_target = neighbor_name;
					break;
				}
				q.push(neighbor_name);
			}
		}

		if (found) {
			// reconstruct path from start_name to found_target
			vector<string> path;
			string cur = found_target;
			while (true) {
				path.push_back(cur);
				if (cur == start_name) {
					break;
				}
				auto it = parent.find(cur);
				if (it == parent.end()) {
					break; // safety
				}
				cur = it->second;
			}
			reverse(path.begin(), path.end());
			result[start] = std::move(path);
		}
	}

	return result;
}

// ReplanGuard implementation
ReplanGuard::ReplanGuard(PACOptimizerInfo *info) : info(info), prev(false) {
	if (info) {
		prev = info->replan_in_progress.load(std::memory_order_acquire);
		info->replan_in_progress.store(true, std::memory_order_release);
	}
}

ReplanGuard::~ReplanGuard() {
	if (info) {
		info->replan_in_progress.store(prev, std::memory_order_release);
	}
}

// Configuration helpers
string GetPacCompiledPath(ClientContext &context, const string &default_path) {
	Value v;
	context.TryGetCurrentSetting("pac_compiled_path", v);
	string path = v.IsNull() ? default_path : v.ToString();
	if (!path.empty() && path.back() != '/') {
		path.push_back('/');
	}
	return path;
}

int64_t GetPacM(ClientContext &context, int64_t default_m) {
	Value v;
	context.TryGetCurrentSetting("pac_m", v);
	if (!v.IsNull()) {
		try {
			int64_t m = v.GetValue<int64_t>();
			return std::max<int64_t>(1, m);
		} catch (...) {
			// fallback to default if conversion fails
			return default_m;
		}
	}
	return default_m;
}

bool IsPacNoiseEnabled(ClientContext &context, bool default_value) {
	Value v;
	context.TryGetCurrentSetting("pac_noise", v);
	if (v.IsNull()) {
		return default_value;
	}
	try {
		return v.GetValue<bool>();
	} catch (...) {
		return default_value;
	}
}

string GetPrivacyMode(ClientContext &context) {
	Value v;
	if (!context.TryGetCurrentSetting("privacy_mode", v) || v.IsNull()) {
		return "pac";
	}
	string s = v.ToString();
	std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
	auto begin = s.find_first_not_of(" \t\n\r");
	auto end = s.find_last_not_of(" \t\n\r");
	if (begin == string::npos) {
		return "pac";
	}
	return s.substr(begin, end - begin + 1);
}

double GetDpEpsilon(ClientContext &context, double default_value) {
	Value v;
	if (!context.TryGetCurrentSetting("dp_epsilon", v) || v.IsNull()) {
		return default_value;
	}
	try {
		return v.GetValue<double>();
	} catch (...) {
		return default_value;
	}
}

bool TryGetDpSumBound(ClientContext &context, double &out) {
	Value v;
	if (!context.TryGetCurrentSetting("dp_sum_bound", v) || v.IsNull()) {
		return false;
	}
	try {
		out = v.GetValue<double>();
		return true;
	} catch (...) {
		return false;
	}
}

bool TryGetDpDelta(ClientContext &context, double &out) {
	Value v;
	if (!context.TryGetCurrentSetting("dp_delta", v) || v.IsNull()) {
		return false;
	}
	try {
		out = v.GetValue<double>();
		return true;
	} catch (...) {
		return false;
	}
}

// Add implementation for GetPacCompileMethod
string GetPacCompileMethod(ClientContext &context, const string &default_method) {
	Value v;
	context.TryGetCurrentSetting("pac_compile_method", v);
	if (v.IsNull()) {
		return default_method;
	}
	try {
		string s = v.ToString();
		std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
		if (s == "standard" || s == "bitslice") {
			return s;
		}
		// fallback
		return default_method;
	} catch (...) {
		return default_method;
	}
}

// Helper to safely retrieve boolean settings with defaults
bool GetBooleanSetting(ClientContext &context, const string &setting_name, bool default_value) {
	Value val;
	if (context.TryGetCurrentSetting(setting_name, val) && !val.IsNull()) {
		return val.GetValue<bool>();
	}
	return default_value;
}

// Collect a map from table_index to the operator that produces it.
static void CollectOperatorsByTableIndex(LogicalOperator &op,
                                         std::unordered_map<idx_t, LogicalOperator *> &table_index_to_op) {
	auto bindings = op.GetColumnBindings();
	if (!bindings.empty()) {
		idx_t op_table_index = bindings[0].table_index;
		table_index_to_op[op_table_index] = &op;
	}
	for (auto &child : op.children) {
		if (child) {
			CollectOperatorsByTableIndex(*child, table_index_to_op);
		}
	}
}

// Helper to trace a binding back through the plan to find which LogicalGet it originates from
LogicalGet *TraceBindingToSource(LogicalOperator &plan, const ColumnBinding &binding, idx_t *out_column_index) {
	// Build a map from table_index to the operator that produces it
	std::unordered_map<idx_t, LogicalOperator *> table_index_to_op;
	CollectOperatorsByTableIndex(plan, table_index_to_op);

	// Start from the binding's table_index and trace backwards
	idx_t current_table_index = binding.table_index;
	idx_t current_column_index = binding.column_index;

	// Limit iterations to prevent infinite loops
	int max_iterations = 100;
	int iterations = 0;

	while (iterations++ < max_iterations) {
		auto it = table_index_to_op.find(current_table_index);
		if (it == table_index_to_op.end()) {
			return nullptr;
		}

		auto *op = it->second;

		// If we found a LogicalGet, we've reached the source
		if (op->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op->Cast<LogicalGet>();
			if (out_column_index) {
				*out_column_index = current_column_index;
			}
			return &get;
		}

		// For PROJECTION nodes, we need to look at the expression for this column
		if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &proj = op->Cast<LogicalProjection>();

			// Check if this column_index is within the projection's expressions
			if (current_column_index >= proj.expressions.size()) {
				return nullptr;
			}

			// Get the expression for this column
			auto &expr = proj.expressions[current_column_index];

			// If it's a BoundColumnRefExpression, follow it
			if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = expr->Cast<BoundColumnRefExpression>();
				current_table_index = col_ref.binding.table_index;
				current_column_index = col_ref.binding.column_index;
				continue;
			}

			// For other expression types, we can't trace further
			return nullptr;
		}

		// For AGGREGATE nodes, check if the column comes from groups
		if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			auto &agg = op->Cast<LogicalAggregate>();

			// Groups come first in the output column bindings
			if (current_column_index < agg.groups.size()) {
				// This is a group column, trace through the group expression
				auto &group_expr = agg.groups[current_column_index];

				if (group_expr->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &col_ref = group_expr->Cast<BoundColumnRefExpression>();
					current_table_index = col_ref.binding.table_index;
					current_column_index = col_ref.binding.column_index;
					continue;
				}
				return nullptr;
			}
			// Aggregate expressions - we can't trace further
			return nullptr;
		}

		// For joins, we need to figure out which child the column comes from
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op->type == LogicalOperatorType::LOGICAL_JOIN ||
		    op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
		    op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			if (op->children.empty()) {
				return nullptr;
			}

			// Get left and right child bindings
			auto left_count = op->children[0] ? op->children[0]->GetColumnBindings().size() : 0;
			// Note: right_count could be used for validation but isn't needed for this logic

			// Check if column comes from left child
			if (current_column_index < left_count && op->children[0]) {
				auto left_bindings = op->children[0]->GetColumnBindings();
				if (current_column_index < left_bindings.size()) {
					current_table_index = left_bindings[current_column_index].table_index;
					current_column_index = left_bindings[current_column_index].column_index;
					continue;
				}
			}

			// Check if column comes from right child
			if (op->children.size() > 1 && op->children[1]) {
				idx_t right_col_idx = current_column_index - left_count;
				auto right_bindings = op->children[1]->GetColumnBindings();
				if (right_col_idx < right_bindings.size()) {
					current_table_index = right_bindings[right_col_idx].table_index;
					current_column_index = right_bindings[right_col_idx].column_index;
					continue;
				}
			}

			return nullptr;
		}

		// For other operators (filters, etc.), trace through the child
		if (!op->children.empty() && op->children[0]) {
			auto child_bindings = op->children[0]->GetColumnBindings();
			if (current_column_index < child_bindings.size()) {
				current_table_index = child_bindings[current_column_index].table_index;
				current_column_index = child_bindings[current_column_index].column_index;
			} else {
				return nullptr;
			}
		} else {
			return nullptr;
		}
	}

	return nullptr;
}

// Helper to collect all equi-join key equivalences from the plan.
// Returns pairs of column bindings that are equal due to equi-join conditions.
static void CollectJoinKeyEquivalences(LogicalOperator &op,
                                       vector<std::pair<ColumnBinding, ColumnBinding>> &equivalences) {
	// Check if this is a comparison join with equi-join conditions
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();

		for (auto &cond : join.conditions) {
			// Only consider equality conditions
			if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
				continue;
			}

			// Check if both sides are column references
			if (cond.left && cond.right && cond.left->type == ExpressionType::BOUND_COLUMN_REF &&
			    cond.right->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &left_ref = cond.left->Cast<BoundColumnRefExpression>();
				auto &right_ref = cond.right->Cast<BoundColumnRefExpression>();
				equivalences.emplace_back(left_ref.binding, right_ref.binding);
			}
		}
	}

	// Recursively process children
	for (auto &child : op.children) {
		if (child) {
			CollectJoinKeyEquivalences(*child, equivalences);
		}
	}
}

// Helper to find all bindings equivalent to the given binding via join key equivalences
static void FindEquivalentBindings(const ColumnBinding &binding,
                                   const vector<std::pair<ColumnBinding, ColumnBinding>> &equivalences,
                                   std::unordered_set<uint64_t> &visited, vector<ColumnBinding> &result) {
	// Create a unique key for the binding
	uint64_t key = (static_cast<uint64_t>(binding.table_index) << 32) | binding.column_index;
	if (visited.find(key) != visited.end()) {
		return;
	}
	visited.insert(key);
	result.push_back(binding);

	// Find all equivalences involving this binding
	for (auto &eq : equivalences) {
		if (eq.first.table_index == binding.table_index && eq.first.column_index == binding.column_index) {
			FindEquivalentBindings(eq.second, equivalences, visited, result);
		}
		if (eq.second.table_index == binding.table_index && eq.second.column_index == binding.column_index) {
			FindEquivalentBindings(eq.first, equivalences, visited, result);
		}
	}
}

bool ColumnBelongsToTable(LogicalOperator &plan, const string &table_name, const ColumnBinding &binding) {
	// First, collect all join key equivalences from the plan
	vector<std::pair<ColumnBinding, ColumnBinding>> equivalences;
	CollectJoinKeyEquivalences(plan, equivalences);

	// Find all bindings equivalent to the given binding (including itself)
	std::unordered_set<uint64_t> visited;
	vector<ColumnBinding> equivalent_bindings;
	FindEquivalentBindings(binding, equivalences, visited, equivalent_bindings);

	// Check if any equivalent binding traces back to the target table
	for (auto &eq_binding : equivalent_bindings) {
		auto *source_get = TraceBindingToSource(plan, eq_binding);
		if (!source_get) {
			continue;
		}

		auto table_entry = source_get->GetTable();
		if (!table_entry) {
			continue;
		}

		if (table_entry->name == table_name) {
			return true;
		}
	}

	return false;
}

// Recursively remap BoundColumnRefExpression bindings in an expression tree.
static void RemapBindingsInExpr(Expression &e, const std::unordered_map<idx_t, idx_t> &map) {
	if (e.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = e.Cast<BoundColumnRefExpression>();
		auto it = map.find(col_ref.binding.table_index);
		if (it != map.end()) {
			col_ref.binding.table_index = it->second;
		}
	}
	ExpressionIterator::EnumerateChildren(e, [&map](Expression &child) { RemapBindingsInExpr(child, map); });
}

void RemapBindingsInSubtree(LogicalOperator &op, const std::unordered_map<idx_t, idx_t> &map) {
	// Remap expressions owned by this operator
	auto remap_expr = [&](unique_ptr<Expression> &expr) {
		if (!expr) {
			return;
		}
		RemapBindingsInExpr(*expr, map);
	};

	// Remap all expression vectors in the operator
	for (auto &expr : op.expressions) {
		remap_expr(expr);
	}

	// Operator-specific expressions
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op.Cast<LogicalAggregate>();
		for (auto &g : agg.groups) {
			remap_expr(g);
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		auto &join = op.Cast<LogicalAnyJoin>();
		remap_expr(join.condition);
	}

	// LogicalComparisonJoin (and subclasses like DELIM_JOIN): remap conditions + duplicate_eliminated_columns
	if (auto *cj = dynamic_cast<LogicalComparisonJoin *>(&op)) {
		for (auto &cond : cj->conditions) {
			remap_expr(cond.left);
			remap_expr(cond.right);
		}
		for (auto &expr : cj->duplicate_eliminated_columns) {
			remap_expr(expr);
		}
	}

	// LogicalDistinct: remap distinct_targets
	if (op.type == LogicalOperatorType::LOGICAL_DISTINCT) {
		auto &distinct = op.Cast<LogicalDistinct>();
		for (auto &expr : distinct.distinct_targets) {
			remap_expr(expr);
		}
	}

	// LogicalOrder: remap order expressions
	if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op.Cast<LogicalOrder>();
		for (auto &node : order.orders) {
			remap_expr(node.expression);
		}
	}

	// Recurse into children
	for (auto &child : op.children) {
		if (child) {
			RemapBindingsInSubtree(*child, map);
		}
	}
}

std::unordered_map<idx_t, idx_t> RemapSubtreeIndices(LogicalOperator *subtree, Binder &binder, const idx_set &avoid) {
	// 1. Collect all indices in subtree
	idx_set subtree_indices;
	CollectTableIndicesRecursive(subtree, subtree_indices);

	// 2. Build mapping: for every index in subtree, generate a fresh one not in avoid or subtree_indices
	std::unordered_map<idx_t, idx_t> map;
	idx_set used = avoid;
	// Also consider existing subtree indices as used to avoid self-collision during remap
	for (auto idx : subtree_indices) {
		used.insert(idx);
	}

	for (auto old_idx : subtree_indices) {
		if (old_idx == DConstants::INVALID_INDEX) {
			continue;
		}
		idx_t fresh = binder.GenerateTableIndex();
		while (used.find(fresh) != used.end()) {
			fresh = binder.GenerateTableIndex();
		}
		map[old_idx] = fresh;
		used.insert(fresh);
	}

	// 3. Apply both operator-level and expression-level remapping
	ApplyIndexMapToSubtree(subtree, map);
	RemapBindingsInSubtree(*subtree, map);

	return map;
}

} // namespace duckdb
