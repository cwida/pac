#include "compiler/pac_bitslice_add_fkjoins.hpp"
#include "pac_debug.hpp"
#include "utils/pac_helpers.hpp"
#include "metadata/pac_compatibility_check.hpp"
#include "compiler/pac_compiler_helpers.hpp"
#include "query_processing/pac_plan_traversal.hpp"
#include "metadata/pac_metadata_manager.hpp"
#include "parser/pac_parser.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

// ============================================================================
// TablesShareJoinSubtree
// ============================================================================
// Check if two tables share a join subtree (connected by a JOIN, not a CROSS_PRODUCT).
// Walks from the root to find the smallest common ancestor containing both table indices.
static bool TablesShareJoinSubtree(LogicalOperator *op, idx_t table_idx_a, idx_t table_idx_b) {
	bool has_a = HasTableIndexInSubtree(op, table_idx_a);
	bool has_b = HasTableIndexInSubtree(op, table_idx_b);

	if (has_a && has_b) {
		for (auto &child : op->children) {
			bool child_has_a = HasTableIndexInSubtree(child.get(), table_idx_a);
			bool child_has_b = HasTableIndexInSubtree(child.get(), table_idx_b);
			if (child_has_a && child_has_b) {
				return TablesShareJoinSubtree(child.get(), table_idx_a, table_idx_b);
			}
		}
		// Tables are in different children — check if this is a real JOIN
		return op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		       op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN;
	}
	return false;
}

// ============================================================================
// GetRequiredColumnsForTable
// ============================================================================
// Compute the minimal set of columns needed when creating a new LogicalGet for a
// missing FK table: PK columns, FK columns to path/PU tables, and columns
// referenced by other tables' FKs.
static vector<string> GetRequiredColumnsForTable(const PACCompatibilityResult &check, const string &table_name,
                                                 const vector<string> &fk_path, const vector<string> &privacy_units) {
	vector<string> required_columns;
	std::unordered_set<string> added_columns;

	auto it = check.table_metadata.find(table_name);
	if (it == check.table_metadata.end()) {
		return {};
	}
	const auto &meta = it->second;

	// 1. PK columns (for incoming joins)
	for (auto &pk : meta.pks) {
		if (added_columns.insert(StringUtil::Lower(pk)).second) {
			required_columns.push_back(pk);
		}
	}

	// 2. FK columns to tables in the FK path or PU (for outgoing joins + hashing)
	for (auto &fk_pair : meta.fks) {
		const string &referenced_table = fk_pair.first;
		const vector<string> &fk_cols = fk_pair.second;

		bool is_relevant = false;
		for (auto &path_table : fk_path) {
			if (StringUtil::Lower(path_table) == StringUtil::Lower(referenced_table)) {
				is_relevant = true;
				break;
			}
		}
		if (!is_relevant) {
			for (auto &pu : privacy_units) {
				if (StringUtil::Lower(pu) == StringUtil::Lower(referenced_table)) {
					is_relevant = true;
					break;
				}
			}
		}
		if (is_relevant) {
			for (auto &fk_col : fk_cols) {
				if (added_columns.insert(StringUtil::Lower(fk_col)).second) {
					required_columns.push_back(fk_col);
				}
			}
		}
	}

	// 3. Columns referenced by other tables' FKs (for incoming joins)
	string table_name_lower = StringUtil::Lower(table_name);
	for (auto &path_table : fk_path) {
		if (StringUtil::Lower(path_table) == table_name_lower) {
			continue;
		}
		auto other_it = check.table_metadata.find(path_table);
		if (other_it == check.table_metadata.end()) {
			continue;
		}
		for (auto &other_fk_pair : other_it->second.fks) {
			if (StringUtil::Lower(other_fk_pair.first) == table_name_lower) {
				auto &metadata_mgr = PACMetadataManager::Get();
				auto *other_pac_metadata = metadata_mgr.GetTableMetadata(path_table);
				if (other_pac_metadata) {
					for (auto &link : other_pac_metadata->links) {
						if (StringUtil::Lower(link.referenced_table) == table_name_lower &&
						    link.local_columns == other_fk_pair.second && !link.referenced_columns.empty()) {
							for (auto &ref_col : link.referenced_columns) {
								if (added_columns.insert(StringUtil::Lower(ref_col)).second) {
									required_columns.push_back(ref_col);
								}
							}
							break;
						}
					}
				}
			}
		}
	}
#if PAC_DEBUG
	if (!required_columns.empty()) {
		string cols_str;
		for (auto &col : required_columns) {
			if (!cols_str.empty()) {
				cols_str += ", ";
			}
			cols_str += col;
		}
		PAC_DEBUG_PRINT("GetRequiredColumnsForTable: Table " + table_name + " requires columns: [" + cols_str + "]");
	}
#endif
	return required_columns;
}

// ============================================================================
// ChainJoinsFromGetMap
// ============================================================================
// Takes a map of table_name → LogicalGet, chains them onto a left node via CreateLogicalJoin.
static unique_ptr<LogicalOperator> ChainJoinsFromGetMap(const PACCompatibilityResult &check, ClientContext &context,
                                                        unique_ptr<LogicalOperator> left_node,
                                                        std::unordered_map<string, unique_ptr<LogicalGet>> &get_map,
                                                        const vector<string> &tables_to_join) {
	if (tables_to_join.empty()) {
		return left_node;
	}

	unique_ptr<LogicalOperator> result = std::move(left_node);
	for (auto &tbl_name : tables_to_join) {
		unique_ptr<LogicalGet> right_op = std::move(get_map[tbl_name]);
		if (!right_op) {
			throw InternalException("PAC compiler: failed to transfer ownership of LogicalGet for " + tbl_name);
		}
		result = CreateLogicalJoin(check, context, std::move(result), std::move(right_op));
	}
	return result;
}

// ============================================================================
// CreateGetsAndChainJoins  (Helper 1)
// ============================================================================
// Create LogicalGet nodes for each table in `tables_to_join`, then chain them
// onto `existing_node` via FK-based joins.  Works for single or multiple tables.
static unique_ptr<LogicalOperator>
CreateGetsAndChainJoins(const PACCompatibilityResult &check, OptimizerExtensionInput &input,
                        unique_ptr<LogicalOperator> &plan, unique_ptr<LogicalOperator> existing_node,
                        const vector<string> &tables_to_join, const vector<string> &fk_path,
                        const vector<string> &privacy_units) {
	auto &binder = input.optimizer.binder;
	std::unordered_map<string, unique_ptr<LogicalGet>> local_get_map;

	for (auto &table : tables_to_join) {
		auto it = check.table_metadata.find(table);
		if (it == check.table_metadata.end()) {
			throw InvalidInputException("PAC compiler: missing table metadata for table: " + table);
		}
		auto required_cols = GetRequiredColumnsForTable(check, table, fk_path, privacy_units);
		idx_t local_idx = binder.GenerateTableIndex();
		auto get = CreateLogicalGet(input.context, plan, table, local_idx, required_cols);
		local_get_map[table] = std::move(get);
	}
	return ChainJoinsFromGetMap(check, input.context, std::move(existing_node), local_get_map, tables_to_join);
}

// ============================================================================
// FindAccessibleAlternativeAndJoin  (Helper 2)
// ============================================================================
// Search `candidate_tables` for an accessible table scan, then join the tables
// in `tables_to_join` onto it.  Returns true if a join was created.
// `skip_table` is excluded from candidates.  `skip_table_index` is an
// additional table_index to skip (e.g. the blocked connecting table).
static bool FindAccessibleAlternativeAndJoin(const PACCompatibilityResult &check, OptimizerExtensionInput &input,
                                             unique_ptr<LogicalOperator> &plan, const vector<string> &candidate_tables,
                                             const string &skip_table, idx_t skip_table_index,
                                             const vector<string> &tables_to_join, const vector<string> &fk_path,
                                             const vector<string> &privacy_units) {
	auto &binder = input.optimizer.binder;

	for (auto &candidate : candidate_tables) {
		if (candidate == skip_table) {
			continue;
		}
		vector<unique_ptr<LogicalOperator> *> table_nodes;
		FindAllNodesByTable(&plan, candidate, table_nodes);

		for (auto *table_node : table_nodes) {
			auto &table_get = table_node->get()->Cast<LogicalGet>();
			if (table_get.table_index == skip_table_index) {
				continue;
			}
			if (!AreTableColumnsAccessible(plan.get(), table_get.table_index)) {
				continue;
			}
#if PAC_DEBUG
			PAC_DEBUG_PRINT("FindAccessibleAlternativeAndJoin: Found accessible table " + candidate + " #" +
			                std::to_string(table_get.table_index) + " - joining " +
			                std::to_string(tables_to_join.size()) + " tables");
#endif
			unique_ptr<LogicalOperator> current_node = (*table_node)->Copy(input.context);
			auto joined = CreateGetsAndChainJoins(check, input, plan, std::move(current_node), tables_to_join, fk_path,
			                                      privacy_units);
			ReplaceNode(plan, *table_node, joined, &binder);
			return true;
		}
	}
	return false;
}

// ============================================================================
// AddMissingFKJoins
// ============================================================================
void AddMissingFKJoins(const PACCompatibilityResult &check, OptimizerExtensionInput &input,
                       unique_ptr<LogicalOperator> &plan, const vector<string> &gets_missing,
                       const vector<string> &gets_present, const vector<string> &fk_path,
                       const vector<string> &privacy_units, const CTETableMap &cte_map) {
	bool join_elimination = GetBooleanSetting(input.context, "pac_join_elimination", false);

	std::unordered_set<string> missing_set(gets_missing.begin(), gets_missing.end());

	// If join elimination is enabled, skip the PU tables themselves
	if (join_elimination) {
		for (auto &pu : privacy_units) {
			missing_set.erase(pu);
		}
	}

	// Check if any "missing" tables are actually already present in the plan
	std::unordered_set<string> actually_present;
	for (auto &table : missing_set) {
		if (FindNodeRefByTable(&plan, table) != nullptr) {
			actually_present.insert(table);
		}
	}
	for (auto &table : actually_present) {
		missing_set.erase(table);
	}

	std::unordered_map<string, unique_ptr<LogicalGet>> get_map;
	vector<string> ordered_table_names;
	vector<string> actually_present_in_fk_order;
	auto &binder = input.optimizer.binder;

	// Build ordered_table_names based on fk_path order, only including missing tables
	for (auto &table : fk_path) {
		if (missing_set.find(table) != missing_set.end()) {
			auto it = check.table_metadata.find(table);
			if (it == check.table_metadata.end()) {
				throw InvalidInputException("PAC compiler: missing table metadata for missing GET: " + table);
			}
			auto required_cols = GetRequiredColumnsForTable(check, table, fk_path, privacy_units);
			idx_t idx = binder.GenerateTableIndex();
			auto get = CreateLogicalGet(input.context, plan, table, idx, required_cols);
			get_map[table] = std::move(get);
			ordered_table_names.push_back(table);
		} else if (actually_present.find(table) != actually_present.end()) {
			actually_present_in_fk_order.push_back(table);
		}
	}

	// Find the connecting table — the existing table that serves as attachment point
	string connecting_table_for_joins;
	string connecting_table_for_missing;

	if (!fk_path.empty()) {
		for (auto &table_in_path : fk_path) {
			bool is_present = false;
			for (auto &present : gets_present) {
				if (table_in_path == present) {
					is_present = true;
					break;
				}
			}
			if (is_present) {
				if (connecting_table_for_joins.empty()) {
					connecting_table_for_joins = table_in_path;
				}
				connecting_table_for_missing = table_in_path;
			}
		}
	}

	if (connecting_table_for_joins.empty() && !gets_present.empty()) {
		connecting_table_for_joins = gets_present[0];
	}
	if (connecting_table_for_missing.empty() && !gets_present.empty()) {
		connecting_table_for_missing = gets_present[0];
	}
	if (connecting_table_for_joins.empty() && !check.scanned_non_pu_tables.empty()) {
		connecting_table_for_joins = check.scanned_non_pu_tables[0];
	}
	if (connecting_table_for_missing.empty() && !check.scanned_non_pu_tables.empty()) {
		connecting_table_for_missing = check.scanned_non_pu_tables[0];
	}

	string connecting_table = ordered_table_names.empty() ? connecting_table_for_joins : connecting_table_for_missing;

	// Find ALL instances of the connecting table (for correlated subqueries, there may be multiple)
	vector<unique_ptr<LogicalOperator> *> all_connecting_nodes;
	if (!connecting_table.empty()) {
		FindAllNodesByTable(&plan, connecting_table, all_connecting_nodes);
	}
	if (all_connecting_nodes.empty() && !gets_present.empty()) {
		for (auto &present : gets_present) {
			FindAllNodesByTable(&plan, present, all_connecting_nodes);
			if (!all_connecting_nodes.empty()) {
				connecting_table = present;
				break;
			}
		}
	}
	if (all_connecting_nodes.empty() && !connecting_table.empty()) {
		throw InvalidInputException("PAC compiler: could not find any LogicalGet for table " + connecting_table);
	}
	if (all_connecting_nodes.empty()) {
		throw InvalidInputException("PAC compiler: could not find any connecting table in the plan");
	}

	// Find the FK table that has FK to PU (e.g., orders -> customer)
	string fk_table_with_pu_reference;
	for (auto &table : fk_path) {
		if (!FindFKColumnsToPU(check, table, privacy_units).empty()) {
			fk_table_with_pu_reference = table;
			break;
		}
	}

	// Iterate over each instance of the connecting table and add joins
	for (auto *target_ref : all_connecting_nodes) {
		auto &target_op = (*target_ref)->Cast<LogicalGet>();
		idx_t connecting_table_idx = target_op.table_index;
		bool is_in_subquery = IsInDelimJoinSubqueryBranch(&plan, target_ref->get());
		vector<string> tables_to_join_for_instance = ordered_table_names;

		// SUBQUERY SPECIAL CASE
		if (is_in_subquery && !fk_table_with_pu_reference.empty() &&
		    std::find(ordered_table_names.begin(), ordered_table_names.end(), fk_table_with_pu_reference) ==
		        ordered_table_names.end()) {
			if (connecting_table == fk_table_with_pu_reference) {
#if PAC_DEBUG
				PAC_DEBUG_PRINT("AddMissingFKJoins: Connecting table #" + std::to_string(connecting_table_idx) +
				                " is already the FK table (" + fk_table_with_pu_reference + "), no join needed");
#endif
			} else {
				vector<unique_ptr<LogicalOperator> *> fk_nodes;
				FindAllNodesByTable(&plan, fk_table_with_pu_reference, fk_nodes);

				bool fk_in_outer_query = false;
				for (auto *fk_node : fk_nodes) {
					if (!IsInDelimJoinSubqueryBranch(&plan, fk_node->get())) {
						fk_in_outer_query = true;
						break;
					}
				}

				bool connecting_table_in_outer = false;
				for (auto *conn_node : all_connecting_nodes) {
					if (!IsInDelimJoinSubqueryBranch(&plan, conn_node->get())) {
						connecting_table_in_outer = true;
						break;
					}
				}

				bool can_use_delim_get = fk_in_outer_query && !connecting_table_in_outer;

				if (!can_use_delim_get) {
					tables_to_join_for_instance.push_back(fk_table_with_pu_reference);
#if PAC_DEBUG
					PAC_DEBUG_PRINT("AddMissingFKJoins: Adding " + fk_table_with_pu_reference +
					                " join for subquery instance #" + std::to_string(connecting_table_idx));
#endif
				}
#if PAC_DEBUG
				else {
					PAC_DEBUG_PRINT("AddMissingFKJoins: FK table " + fk_table_with_pu_reference +
					                " accessible via DELIM_GET for subquery instance #" +
					                std::to_string(connecting_table_idx));
				}
#endif
			}
		}

		// PRIMARY PATH: create joins for missing tables
		if (!tables_to_join_for_instance.empty()) {
			unique_ptr<LogicalOperator> existing_node = (*target_ref)->Copy(input.context);
			auto final_join = CreateGetsAndChainJoins(check, input, plan, std::move(existing_node),
			                                          tables_to_join_for_instance, fk_path, privacy_units);
			ReplaceNode(plan, *target_ref, final_join, &binder);
			continue;
		}

		// FALLBACK: all FK tables already present — ensure they're accessible
		vector<string> all_present_tables;
		all_present_tables.insert(all_present_tables.end(), gets_present.begin(), gets_present.end());
		all_present_tables.insert(all_present_tables.end(), actually_present.begin(), actually_present.end());

		bool found_accessible_fk_table = false;

		for (auto &present_table : all_present_tables) {
			if (!FindFKColumnsToPU(check, present_table, privacy_units).empty()) {
				vector<unique_ptr<LogicalOperator> *> fk_table_nodes;
				FindAllNodesByTable(&plan, present_table, fk_table_nodes);
				if (fk_table_nodes.empty()) {
					continue;
				}
				for (auto *fk_node : fk_table_nodes) {
					auto &fk_table_get = fk_node->get()->Cast<LogicalGet>();
					idx_t fk_table_idx = fk_table_get.table_index;

					if (fk_table_idx == connecting_table_idx) {
						if (AreTableColumnsAccessible(plan.get(), fk_table_idx)) {
#if PAC_DEBUG
							PAC_DEBUG_PRINT("AddMissingFKJoins: Connecting table #" +
							                std::to_string(connecting_table_idx) +
							                " IS the FK table - mapping to itself");
#endif
							found_accessible_fk_table = true;
							break;
						}
						continue;
					}

					bool shares_subtree = TablesShareJoinSubtree(plan.get(), connecting_table_idx, fk_table_idx);
					if (shares_subtree && AreTableColumnsAccessible(plan.get(), fk_table_idx)) {
#if PAC_DEBUG
						PAC_DEBUG_PRINT("AddMissingFKJoins: Mapped connecting table #" +
						                std::to_string(connecting_table_idx) + " to FK table " + present_table + " #" +
						                std::to_string(fk_table_idx) + " for hashing (same subtree)");
#endif
						found_accessible_fk_table = true;
						break;
					}
				}
				if (found_accessible_fk_table) {
					break;
				}
			}
		}

		if (found_accessible_fk_table || fk_table_with_pu_reference.empty()) {
			continue;
		}

		// No accessible FK table — need to add joins
		string connecting_table_name;
		auto table_ptr = target_op.GetTable();
		if (table_ptr) {
			connecting_table_name = table_ptr->name;
		}

		// Case 1: connecting table IS the FK table but blocked (SEMI/ANTI join)
		if (connecting_table_name == fk_table_with_pu_reference) {
			if (AreTableColumnsAccessible(plan.get(), connecting_table_idx)) {
#if PAC_DEBUG
				PAC_DEBUG_PRINT("AddMissingFKJoins: Connecting table #" + std::to_string(connecting_table_idx) + " (" +
				                connecting_table_name +
				                ") IS the FK table with PU reference and columns are accessible, no join needed");
#endif
				continue;
			}
#if PAC_DEBUG
			PAC_DEBUG_PRINT("AddMissingFKJoins: Connecting table #" + std::to_string(connecting_table_idx) + " (" +
			                connecting_table_name +
			                ") IS the FK table but columns are NOT accessible (blocked by SEMI/ANTI join)");
#endif
			// Find accessible alternative with direct FK to the blocked table
			vector<string> candidates;
			candidates.insert(candidates.end(), gets_present.begin(), gets_present.end());
			candidates.insert(candidates.end(), check.scanned_non_pu_tables.begin(), check.scanned_non_pu_tables.end());

			// Filter to candidates that have FK to the blocked table
			vector<string> filtered_candidates;
			for (auto &cand : candidates) {
				if (cand == fk_table_with_pu_reference) {
					continue;
				}
				auto cand_it = check.table_metadata.find(cand);
				if (cand_it == check.table_metadata.end()) {
					continue;
				}
				for (auto &fk : cand_it->second.fks) {
					if (fk.first == fk_table_with_pu_reference) {
						filtered_candidates.push_back(cand);
						break;
					}
				}
			}

			vector<string> single_table = {fk_table_with_pu_reference};
			if (FindAccessibleAlternativeAndJoin(check, input, plan, filtered_candidates, fk_table_with_pu_reference,
			                                     DConstants::INVALID_INDEX, single_table, fk_path, privacy_units)) {
				continue;
			}
#if PAC_DEBUG
			PAC_DEBUG_PRINT("AddMissingFKJoins: No accessible alternative found for blocked FK table " +
			                fk_table_with_pu_reference + " - skipping this connecting table instance");
#endif
			continue;
		}

		// Case 2: connecting table is NOT the FK table — build join chain to reach it
		vector<string> tables_to_add;
		bool found_connecting = false;
		bool found_fk_table = false;

		for (auto &table_in_path : fk_path) {
			if (table_in_path == connecting_table_name) {
				found_connecting = true;
				continue;
			}
			if (found_connecting && !found_fk_table) {
				tables_to_add.push_back(table_in_path);
				if (table_in_path == fk_table_with_pu_reference) {
					found_fk_table = true;
				}
			}
		}

#if PAC_DEBUG
		PAC_DEBUG_PRINT("AddMissingFKJoins: No accessible FK table found, adding join chain for " +
		                std::to_string(tables_to_add.size()) + " tables to connecting table #" +
		                std::to_string(connecting_table_idx));
		for (auto &t : tables_to_add) {
			PAC_DEBUG_PRINT("  - " + t);
		}
#endif
		if (tables_to_add.empty()) {
			continue;
		}

		// If connecting table is blocked, find an accessible alternative
		if (!AreTableColumnsAccessible(plan.get(), connecting_table_idx)) {
#if PAC_DEBUG
			PAC_DEBUG_PRINT("AddMissingFKJoins: Connecting table #" + std::to_string(connecting_table_idx) + " (" +
			                connecting_table_name +
			                ") columns are NOT accessible - looking for accessible alternative");
#endif
			// Build candidate list from scanned tables that have FK paths to the PU
			vector<string> candidates;
			for (auto &scanned_table : check.scanned_non_pu_tables) {
				if (scanned_table == connecting_table_name) {
					continue;
				}
				auto scanned_fk_path_it = check.fk_paths.find(scanned_table);
				if (scanned_fk_path_it == check.fk_paths.end() || scanned_fk_path_it->second.empty()) {
					continue;
				}
				candidates.push_back(scanned_table);
			}

			// For each candidate, compute the join chain from candidate to FK table
			bool found_alternative = false;
			for (auto &candidate : candidates) {
				vector<unique_ptr<LogicalOperator> *> table_nodes;
				FindAllNodesByTable(&plan, candidate, table_nodes);

				for (auto *table_node : table_nodes) {
					auto &table_get = table_node->get()->Cast<LogicalGet>();
					if (table_get.table_index == connecting_table_idx) {
						continue;
					}
					if (!AreTableColumnsAccessible(plan.get(), table_get.table_index)) {
						continue;
					}

					auto scanned_fk_path_it = check.fk_paths.find(candidate);
					const vector<string> &scanned_fk_path = scanned_fk_path_it->second;

					vector<string> full_tables_to_add;
					bool found_start = false;
					bool found_end = false;
					for (auto &path_table : scanned_fk_path) {
						if (path_table == candidate) {
							found_start = true;
							continue;
						}
						if (found_start && !found_end) {
							full_tables_to_add.push_back(path_table);
							if (path_table == fk_table_with_pu_reference) {
								found_end = true;
							}
						}
					}
					if (full_tables_to_add.empty()) {
						continue;
					}
#if PAC_DEBUG
					PAC_DEBUG_PRINT("AddMissingFKJoins: Found accessible table " + candidate + " #" +
					                std::to_string(table_get.table_index) + " - adding full join chain:");
					for (auto &t : full_tables_to_add) {
						PAC_DEBUG_PRINT("  - " + t);
					}
#endif
					unique_ptr<LogicalOperator> current_node = (*table_node)->Copy(input.context);
					auto joined = CreateGetsAndChainJoins(check, input, plan, std::move(current_node),
					                                      full_tables_to_add, fk_path, privacy_units);
					ReplaceNode(plan, *table_node, joined, &binder);
					found_alternative = true;
					break;
				}
				if (found_alternative) {
					break;
				}
			}
			if (found_alternative) {
				continue;
			}
#if PAC_DEBUG
			PAC_DEBUG_PRINT("AddMissingFKJoins: No accessible alternative found - skipping this instance");
#endif
			continue;
		}

		// Connecting table is accessible — create join chain directly
		unique_ptr<LogicalOperator> existing_node = (*target_ref)->Copy(input.context);
		auto final_join = CreateGetsAndChainJoins(check, input, plan, std::move(existing_node), tables_to_add, fk_path,
		                                          privacy_units);
		ReplaceNode(plan, *target_ref, final_join, &binder);
	}
}

} // namespace duckdb
