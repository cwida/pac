//
// PAC Bitslice Join Chain Builder
//
// Phase 1 of PAC bitslice compilation: when the PU table is NOT scanned directly,
// this module inserts join chains for missing FK tables and builds the
// connecting_table -> fk_table index mapping.
//
// Created by refactoring from pac_bitslice_compiler.cpp
//

#include "compiler/pac_bitslice_join_chain.hpp"
#include "privacy_debug.hpp"
#include "utils/privacy_helpers.hpp"
#include "parser/privacy_parser.hpp"
#include "metadata/privacy_metadata_manager.hpp"
#include "compiler/pac_compiler_helpers.hpp"
#include "query_processing/pac_join_builder.hpp"
#include "query_processing/pac_plan_traversal.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

// Check if two tables share a join subtree (connected by a JOIN, not a CROSS_PRODUCT).
// Walks from the root to find the smallest common ancestor containing both table indices.
static bool TablesShareJoinSubtree(LogicalOperator *op, idx_t table_idx_a, idx_t table_idx_b) {
	if (!op) {
		return false;
	}

	bool has_a = HasTableIndexInSubtree(op, table_idx_a);
	bool has_b = HasTableIndexInSubtree(op, table_idx_b);

	if (has_a && has_b) {
		// Both tables are in this subtree - check if they're in the SAME child
		for (auto &child : op->children) {
			bool child_has_a = HasTableIndexInSubtree(child.get(), table_idx_a);
			bool child_has_b = HasTableIndexInSubtree(child.get(), table_idx_b);

			if (child_has_a && child_has_b) {
				// Both in same child - recurse to find tighter common ancestor
				return TablesShareJoinSubtree(child.get(), table_idx_a, table_idx_b);
			}
		}
		// Tables are in different children of this operator
		// Check if this is a JOIN that connects them (valid) vs CROSS_PRODUCT (separate branches)
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			return true;
		}
		return false;
	}
	return false;
}

// Helper function to compute required columns for a table in the FK path
// Returns the columns needed for joining and for the PU FK reference (for hashing)
static vector<string> GetRequiredColumnsForTable(const PrivacyCompatibilityResult &check, const string &table_name,
                                                 const vector<string> &fk_path, const vector<string> &privacy_units) {
	vector<string> required_columns;
	std::unordered_set<string> added_columns;

	auto it = check.table_metadata.find(table_name);
	if (it == check.table_metadata.end()) {
#if PRIVACY_DEBUG
		PRIVACY_DEBUG_PRINT("GetRequiredColumnsForTable WARNING: No metadata for table " + table_name);
#endif
		return {}; // Empty = project all columns (fallback)
	}

	const auto &meta = it->second;

	// 1. Add PK columns (needed for joins FROM other tables TO this table)
	for (auto &pk : meta.pks) {
		if (added_columns.insert(StringUtil::Lower(pk)).second) {
			required_columns.push_back(pk);
		}
	}

	// 2. Add FK columns (needed for joins FROM this table TO other tables, and for PU hash)
	for (auto &fk_pair : meta.fks) {
		const string &referenced_table = fk_pair.first;
		const vector<string> &fk_cols = fk_pair.second;

		// Check if this FK references a table in the FK path or a privacy unit
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

	// 3. Add columns that are REFERENCED BY other tables' FKs (needed for incoming joins)
	// For example, if lineitem has FK l_orderkey -> orders.o_orderkey, then orders needs o_orderkey
	string table_name_lower = StringUtil::Lower(table_name);
	for (auto &path_table : fk_path) {
		if (StringUtil::Lower(path_table) == table_name_lower) {
			continue; // Skip self
		}
		auto other_it = check.table_metadata.find(path_table);
		if (other_it == check.table_metadata.end()) {
			continue;
		}
		const auto &other_meta = other_it->second;
		for (auto &other_fk_pair : other_meta.fks) {
			// Check if this FK from path_table references our table
			if (StringUtil::Lower(other_fk_pair.first) == table_name_lower) {
				// The FK columns from the other table reference columns in our table
				// We need to find which columns in our table they reference
				// This info is in PAC LINK metadata (referenced_columns)
				auto &metadata_mgr = PrivacyMetadataManager::Get();
				auto *other_pac_metadata = metadata_mgr.GetTableMetadata(path_table);
				if (other_pac_metadata) {
					for (auto &link : other_pac_metadata->links) {
						if (StringUtil::Lower(link.referenced_table) == table_name_lower &&
						    link.local_columns == other_fk_pair.second && !link.referenced_columns.empty()) {
							// Add the referenced columns from our table
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

#if PRIVACY_DEBUG
	if (!required_columns.empty()) {
		string cols_str;
		for (auto &col : required_columns) {
			if (!cols_str.empty()) {
				cols_str += ", ";
			}
			cols_str += col;
		}
		PRIVACY_DEBUG_PRINT("GetRequiredColumnsForTable: Table " + table_name + " requires columns: [" + cols_str +
		                    "]");
	}
#endif

	return required_columns;
}

ConnectingTableMap EnsureFKTablesInPlan(const PrivacyCompatibilityResult &check, OptimizerExtensionInput &input,
                                        unique_ptr<LogicalOperator> &plan, const vector<string> &gets_missing,
                                        const vector<string> &gets_present, const vector<string> &fk_path,
                                        const vector<string> &privacy_units, const CTETableMap &cte_map) {
	ConnectingTableMap connecting_table_to_orders_table;

	// Note: all PU tables must have PRIVACY_KEY defined

	// Check if join elimination is enabled
	bool join_elimination = GetBooleanSetting(input.context, "pac_join_elimination", false);

	// Create the necessary LogicalGets for missing tables
	// IMPORTANT: We need to preserve the FK path ordering when creating joins
	// Use fk_path as the canonical ordering, filter to only missing tables
	std::unordered_set<string> missing_set(gets_missing.begin(), gets_missing.end());

	// If join elimination is enabled, skip the PU tables themselves
	// We only need the FK-linked table (e.g., orders), not the PU (e.g., customer)
	if (join_elimination) {
		for (auto &pu : privacy_units) {
			missing_set.erase(pu);
		}
	}

	// Check if any "missing" tables are actually already present in the plan
	// This can happen with correlated subqueries where the FK path starts from a subquery table
	// but the outer query already has the connecting table
	std::unordered_set<string> actually_present;
	for (auto &table : missing_set) {
		if (FindNodeRefByTable(&plan, table) != nullptr) {
			actually_present.insert(table);
		}
	}

	// Remove actually present tables from missing_set
	for (auto &table : actually_present) {
		missing_set.erase(table);
	}

	std::unordered_map<string, unique_ptr<LogicalGet>> get_map;
	vector<string> ordered_table_names;
	// Track tables that were marked as missing but are actually present - these can serve as connection points
	vector<string> actually_present_in_fk_order;
	auto &binder = input.optimizer.binder;

	// Build ordered_table_names based on fk_path order, only including missing tables
	for (auto &table : fk_path) {
		if (missing_set.find(table) != missing_set.end()) {
			auto it = check.table_metadata.find(table);
			if (it == check.table_metadata.end()) {
				throw InvalidInputException("PAC compiler: missing table metadata for missing GET: " + table);
			}
			vector<string> pks = it->second.pks;
			// Get required columns for this table (PKs and relevant FKs)
			auto required_cols = GetRequiredColumnsForTable(check, table, fk_path, privacy_units);
			idx_t idx = binder.GenerateTableIndex();
			auto get = CreateLogicalGet(input.context, plan, table, idx, required_cols);
			get_map[table] = std::move(get);
			ordered_table_names.push_back(table);
		} else if (actually_present.find(table) != actually_present.end()) {
			// Track the order of already-present tables in the FK path
			actually_present_in_fk_order.push_back(table);
		}
	}

	// Find the unique_ptr reference to the existing table that connects to the missing tables
	// There are TWO different scenarios:
	// 1. When there ARE missing tables to join: use the LAST present table in FK path order
	//    (it's adjacent to the first missing table)
	// 2. When there are NO missing tables: use the FIRST present table in FK path order
	//    (it's the leaf table that may appear in subqueries and need joins to the FK table)

	// Determine which present table should be the "connecting table" for adding joins
	string connecting_table_for_joins;   // First present - for finding nodes that need joins added
	string connecting_table_for_missing; // Last present - for connecting to missing tables

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
					connecting_table_for_joins = table_in_path; // First present table
				}
				connecting_table_for_missing = table_in_path; // Keep updating to get last present
			}
		}
	}

	// Fallback: if no connecting table found in FK path, use any present table
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

	// Decide which connecting table to use based on whether there are missing tables
	// If there are missing tables, we iterate over the LAST present table to add joins to missing tables
	// If there are NO missing tables, we iterate over the FIRST present table to add FK joins for subqueries
	string connecting_table = ordered_table_names.empty() ? connecting_table_for_joins : connecting_table_for_missing;

	// Find ALL instances of the connecting table (for correlated subqueries, there may be multiple)
	// Example: In TPC-H Q17, lineitem appears in both outer query and subquery
	vector<unique_ptr<LogicalOperator> *> all_connecting_nodes;
	if (!connecting_table.empty()) {
		FindAllNodesByTable(&plan, connecting_table, all_connecting_nodes);
	}

	// If no connecting nodes found, we may be in a case where the query scans a leaf table
	// that's not directly in gets_present but is in the FK chain
	if (all_connecting_nodes.empty() && !gets_present.empty()) {
		// Try to find any present table in the plan
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

	// For each instance of the connecting table, add the join chain
	// Store the mapping from each instance to its corresponding FK table (e.g., orders) for hash generation
	// This is critical for correlated subqueries: each instance gets its own FK table join

	// First, find the FK table that has FK to PU (e.g., orders -> customer)
	// This is the table whose FK columns we'll hash
	string fk_table_with_pu_reference;
	for (auto &table : fk_path) {
		if (!FindFKColumnsToPU(check, table, privacy_units).empty()) {
			fk_table_with_pu_reference = table;
			break;
		}
	}

	// Iterate over each instance of the connecting table and add joins
	for (auto *target_ref : all_connecting_nodes) {
		// Get the table index of this instance
		auto &target_op = (*target_ref)->Cast<LogicalGet>();
		idx_t connecting_table_idx = target_op.table_index;

		// Check if this connecting table instance is inside a DELIM_JOIN subquery branch
		// If so, we MAY need to add a join to the FK table - BUT only if the FK table
		// is not already directly accessible in the outer query WITHOUT going through
		// a join with the connecting table
		bool is_in_subquery = IsInDelimJoinSubqueryBranch(&plan, target_ref->get());

		// Determine which tables need to be joined for THIS instance
		vector<string> tables_to_join_for_instance = ordered_table_names;

		// SUBQUERY SPECIAL CASE:
		// For subquery instances, check if we need to add a join to the FK table
		// even though it's "present" in outer query
		if (is_in_subquery && !fk_table_with_pu_reference.empty() &&
		    std::find(ordered_table_names.begin(), ordered_table_names.end(), fk_table_with_pu_reference) ==
		        ordered_table_names.end()) {
			// The FK table is not in the join list (because it's "present" in outer query)
			//
			// We should ONLY skip adding the join if:
			// 1. The FK table is in the outer query AND
			// 2. The connecting table is NOT in the outer query (meaning outer query scans FK table directly)
			//
			// If BOTH the FK table AND connecting table are in the outer query, the subquery
			// still needs its own FK join because:
			// - The outer query has: connecting_table JOIN fk_table
			// - The subquery has: connecting_table (correlated with outer connecting_table)
			// - The subquery needs its own fk_table join to get the FK columns for each row

			// IMPORTANT: If the connecting table IS the FK table, we don't need to add a join
			// (we can't join a table to itself via FK relationship)
			if (connecting_table == fk_table_with_pu_reference) {
				// The connecting table already IS the FK table - no join needed
				// Just map it to itself for hash generation
				connecting_table_to_orders_table[connecting_table_idx] = connecting_table_idx;
#if PRIVACY_DEBUG
				PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: Connecting table #" + std::to_string(connecting_table_idx) +
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

				// Check if the connecting table is ALSO in the outer query
				bool connecting_table_in_outer = false;
				for (auto *conn_node : all_connecting_nodes) {
					if (!IsInDelimJoinSubqueryBranch(&plan, conn_node->get())) {
						connecting_table_in_outer = true;
						break;
					}
				}

				// Only skip adding FK join if:
				// - FK table is in outer query AND
				// - Connecting table is NOT in outer query (outer scans FK table directly)
				// This means the subquery can access FK columns via DELIM_GET
				bool can_use_delim_get = fk_in_outer_query && !connecting_table_in_outer;

				if (!can_use_delim_get) {
					// Need to add FK join for this subquery instance
					tables_to_join_for_instance.push_back(fk_table_with_pu_reference);
#if PRIVACY_DEBUG
					PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: Adding " + fk_table_with_pu_reference +
					                    " join for subquery instance #" + std::to_string(connecting_table_idx));
#endif
				}
#if PRIVACY_DEBUG
				else {
					PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: FK table " + fk_table_with_pu_reference +
					                    " accessible via DELIM_GET for subquery instance #" +
					                    std::to_string(connecting_table_idx));
				}
#endif
			}
		}

		// Only create joins if there are tables to join for this instance
		if (!tables_to_join_for_instance.empty()) {
			unique_ptr<LogicalOperator> existing_node = (*target_ref)->Copy(input.context);

			// Create fresh LogicalGet nodes for this instance
			std::unordered_map<string, unique_ptr<LogicalGet>> local_get_map;

			for (auto &table : tables_to_join_for_instance) {
				auto it = check.table_metadata.find(table);
				if (it == check.table_metadata.end()) {
					throw InvalidInputException("PAC compiler: missing table metadata for table: " + table);
				}
				vector<string> pks = it->second.pks;
				// Get required columns for this table (PKs and relevant FKs)
				auto required_cols = GetRequiredColumnsForTable(check, table, fk_path, privacy_units);
				idx_t local_idx = binder.GenerateTableIndex();
				auto get = CreateLogicalGet(input.context, plan, table, local_idx, required_cols);

				// Remember the table index of the FK table for this instance
				if (table == fk_table_with_pu_reference) {
					connecting_table_to_orders_table[connecting_table_idx] = local_idx;
				}

				local_get_map[table] = std::move(get);
			}
			unique_ptr<LogicalOperator> final_join = ChainJoinsFromGetMap(
			    check, input.context, std::move(existing_node), local_get_map, tables_to_join_for_instance);
			// Replace this instance with the join chain
			ReplaceNode(plan, *target_ref, final_join, &binder);
		} else {
			// No tables to join - the FK-linked table (e.g., orders) must already be in the plan
			// and this instance can access it directly (not in a subquery branch)
			// Find it and map the connecting table to it
			// Search BOTH gets_present AND actually_present tables (tables marked missing but found in plan)
			// IMPORTANT: We must find an FK table that is in the SAME SUBTREE as the connecting table,
			// not just any accessible FK table. This is crucial for queries with multiple branches
			// (e.g., CROSS_PRODUCT with different aggregates in each branch).
			vector<string> all_present_tables;
			all_present_tables.insert(all_present_tables.end(), gets_present.begin(), gets_present.end());
			all_present_tables.insert(all_present_tables.end(), actually_present.begin(), actually_present.end());

			bool found_accessible_fk_table = false;

			// First, find the common ancestor of this connecting table in the plan
			// We need to find an FK table that shares a subtree with this connecting table
			for (auto &present_table : all_present_tables) {
				// Check if this present table has an FK to the PU
				if (!FindFKColumnsToPU(check, present_table, privacy_units).empty()) {
					// This is the table we need for hashing - find its table index
					vector<unique_ptr<LogicalOperator> *> fk_table_nodes;
					FindAllNodesByTable(&plan, present_table, fk_table_nodes);
					if (fk_table_nodes.empty()) {
						continue;
					}
					// Find an FK table instance that is in the same subtree as the connecting table
					// This is done by checking if BOTH tables are reachable from some common ancestor
					for (auto *fk_node : fk_table_nodes) {
						auto &fk_table_get = fk_node->get()->Cast<LogicalGet>();
						idx_t fk_table_idx = fk_table_get.table_index;

						// IMPORTANT: If the FK table IS the connecting table (same index),
						// they trivially share a subtree - just map it to itself
						if (fk_table_idx == connecting_table_idx) {
							if (AreTableColumnsAccessible(plan.get(), fk_table_idx)) {
								connecting_table_to_orders_table[connecting_table_idx] = fk_table_idx;
#if PRIVACY_DEBUG
								PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: Connecting table #" +
								                    std::to_string(connecting_table_idx) +
								                    " IS the FK table - mapping to itself");
#endif
								found_accessible_fk_table = true;
								break;
							}
							continue;
						}

						// Check if this FK table is in the same subtree as the connecting table
						// by finding a common ancestor that has both table indices in its subtree
						bool shares_subtree = TablesShareJoinSubtree(plan.get(), connecting_table_idx, fk_table_idx);

						// Also verify the FK table's columns are accessible (not blocked by MARK/SEMI/ANTI)
						if (shares_subtree && AreTableColumnsAccessible(plan.get(), fk_table_idx)) {
							// Map connecting table to this FK table instance
							connecting_table_to_orders_table[connecting_table_idx] = fk_table_idx;
#if PRIVACY_DEBUG
							PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: Mapped connecting table #" +
							                    std::to_string(connecting_table_idx) + " to FK table " + present_table +
							                    " #" + std::to_string(fk_table_idx) + " for hashing (same subtree)");
#endif
							found_accessible_fk_table = true;
							break;
						}
#if PRIVACY_DEBUG
						else {
							PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: FK table " + present_table + " #" +
							                    std::to_string(fk_table_idx) +
							                    " is NOT in same subtree as connecting table #" +
							                    std::to_string(connecting_table_idx));
						}
#endif
					}
					if (found_accessible_fk_table) {
						break;
					}
				}
			}

			// If no accessible FK table was found, we need to add a join to bring in the FK table
			// on the accessible (left) side of the query
			if (!found_accessible_fk_table && !fk_table_with_pu_reference.empty()) {
				// We need to join through ALL intermediate tables in the FK chain
				// to reach the FK table with PU reference.
				// Example: shipments -> order_items -> orders (FK to users)
				// We can't just join shipments directly to orders - we need order_items in between.

				// Find the connecting table's position in the FK path
				string connecting_table_name;
				auto table_ptr = target_op.GetTable();
				if (table_ptr) {
					connecting_table_name = table_ptr->name;
				}

				// IMPORTANT: If the connecting table IS the FK table with PU reference,
				// we need to check if its columns are actually accessible.
				// If blocked by SEMI/ANTI join, we need to find an accessible table and add a join from there.
				if (connecting_table_name == fk_table_with_pu_reference) {
					// Check if this table's columns are accessible
					if (AreTableColumnsAccessible(plan.get(), connecting_table_idx)) {
						// Columns are accessible - no join needed
						connecting_table_to_orders_table[connecting_table_idx] = connecting_table_idx;
#if PRIVACY_DEBUG
						PRIVACY_DEBUG_PRINT(
						    "EnsureFKTablesInPlan: Connecting table #" + std::to_string(connecting_table_idx) + " (" +
						    connecting_table_name +
						    ") IS the FK table with PU reference and columns are accessible, no join needed");
#endif
						continue; // Skip to next connecting table instance
					}
#if PRIVACY_DEBUG
					PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: Connecting table #" +
					                    std::to_string(connecting_table_idx) + " (" + connecting_table_name +
					                    ") IS the FK table but columns are NOT accessible (blocked by SEMI/ANTI join)");
#endif
					// Columns are NOT accessible - we need to find an accessible table that has
					// a PAC LINK (FK) to the blocked FK table, and add a join from there.
					// Search in ALL scanned tables for tables that have FK to the blocked table.
					bool found_accessible_alternative = false;

					// Build list of all scanned tables to search
					vector<string> all_scanned_tables;
					all_scanned_tables.insert(all_scanned_tables.end(), gets_present.begin(), gets_present.end());
					all_scanned_tables.insert(all_scanned_tables.end(), check.scanned_non_pu_tables.begin(),
					                          check.scanned_non_pu_tables.end());

					for (auto &present_table : all_scanned_tables) {
						if (present_table == fk_table_with_pu_reference) {
							continue; // Skip the blocked table itself
						}

						// Check if this present table has an FK to the blocked FK table
						auto it = check.table_metadata.find(present_table);
						if (it == check.table_metadata.end()) {
							continue;
						}

						bool has_fk_to_blocked = false;
						for (auto &fk : it->second.fks) {
							if (fk.first == fk_table_with_pu_reference) {
								has_fk_to_blocked = true;
								break;
							}
						}

						if (!has_fk_to_blocked) {
							continue;
						}

						// Found a table with FK to the blocked table - check if it's accessible
						vector<unique_ptr<LogicalOperator> *> table_nodes;
						FindAllNodesByTable(&plan, present_table, table_nodes);

						for (auto *table_node : table_nodes) {
							auto &table_get = table_node->get()->Cast<LogicalGet>();
							if (AreTableColumnsAccessible(plan.get(), table_get.table_index)) {
								// Found an accessible table with FK to the blocked FK table
								// Add a join from this table to the FK table
#if PRIVACY_DEBUG
								PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: Found accessible table " + present_table +
								                    " #" + std::to_string(table_get.table_index) +
								                    " with FK to blocked table - adding join to " +
								                    fk_table_with_pu_reference);
#endif
								unique_ptr<LogicalOperator> current_node = (*table_node)->Copy(input.context);
								auto local_idx = binder.GenerateTableIndex();

								// Create a join to the FK table
								// Get required columns for this table (PKs and relevant FKs)
								auto required_cols = GetRequiredColumnsForTable(check, fk_table_with_pu_reference,
								                                                fk_path, privacy_units);
								auto new_get = CreateLogicalGet(input.context, plan, fk_table_with_pu_reference,
								                                local_idx, required_cols);
								idx_t fk_table_idx = local_idx;

								auto join = CreateLogicalJoin(check, input.context, std::move(current_node),
								                              std::move(new_get));

								// Map the accessible table to the new FK table instance
								connecting_table_to_orders_table[table_get.table_index] = fk_table_idx;

								ReplaceNode(plan, *table_node, join, &binder);
								found_accessible_alternative = true;
								break;
							}
						}

						if (found_accessible_alternative) {
							break;
						}
					}

					if (found_accessible_alternative) {
						continue; // Move to next connecting table instance
					}

					// If still not found, this connecting table instance simply cannot be used
					// for PAC transformation - skip it (don't throw an exception, as other
					// code paths may handle this aggregate differently)
#if PRIVACY_DEBUG
					PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: No accessible alternative found for blocked FK table " +
					                    fk_table_with_pu_reference + " - skipping this connecting table instance");
#endif
					continue;
				}

				// Build list of tables we need to join to reach fk_table_with_pu_reference
				vector<string> tables_to_add;
				bool found_connecting = false;
				bool found_fk_table = false;

				for (auto &table_in_path : fk_path) {
					if (table_in_path == connecting_table_name) {
						found_connecting = true;
						continue; // Skip the connecting table itself
					}
					if (found_connecting && !found_fk_table) {
						tables_to_add.push_back(table_in_path);
						if (table_in_path == fk_table_with_pu_reference) {
							found_fk_table = true;
						}
					}
				}

#if PRIVACY_DEBUG
				PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: No accessible FK table found, adding join chain for " +
				                    std::to_string(tables_to_add.size()) + " tables to connecting table #" +
				                    std::to_string(connecting_table_idx));
				for (auto &t : tables_to_add) {
					PRIVACY_DEBUG_PRINT("  - " + t);
				}
#endif

				if (tables_to_add.empty()) {
					continue;
				}

				// IMPORTANT: Before adding the join chain to this connecting table,
				// check if its columns are actually accessible from the plan root.
				// If the connecting table is in the right branch of a SEMI/ANTI join,
				// any columns we add via joins won't be able to propagate to the aggregate.
				// In that case, we need to find an accessible table and add the FULL join chain there.
				if (!AreTableColumnsAccessible(plan.get(), connecting_table_idx)) {
#if PRIVACY_DEBUG
					PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: Connecting table #" +
					                    std::to_string(connecting_table_idx) + " (" + connecting_table_name +
					                    ") columns are NOT accessible - looking for accessible alternative");
#endif
					// Find an accessible table in the FK path and add the full join chain from there
					bool found_accessible_alternative = false;

					// Search all scanned tables for one that is accessible and has an FK path to the PU
					// IMPORTANT: We must check ALL FK paths, not just the single fk_path parameter,
					// because accessible tables might have their own FK paths (e.g., shipments -> order_items -> orders
					// -> users)
					for (auto &scanned_table : check.scanned_non_pu_tables) {
						// Skip the connecting table itself - we already know it's not accessible
						if (scanned_table == connecting_table_name) {
							continue;
						}

						// Check if this table has its own FK path to the PU
						auto scanned_fk_path_it = check.fk_paths.find(scanned_table);
						if (scanned_fk_path_it == check.fk_paths.end() || scanned_fk_path_it->second.empty()) {
							continue; // This table has no FK path to PU
						}
						const vector<string> &scanned_fk_path = scanned_fk_path_it->second;

						// Find all instances of this table and check if any is accessible
						vector<unique_ptr<LogicalOperator> *> table_nodes;
						FindAllNodesByTable(&plan, scanned_table, table_nodes);

						for (auto *table_node : table_nodes) {
							auto &table_get = table_node->get()->Cast<LogicalGet>();
							// Skip if this is actually the same node as our connecting table
							if (table_get.table_index == connecting_table_idx) {
								continue;
							}
							if (AreTableColumnsAccessible(plan.get(), table_get.table_index)) {
								// Found an accessible table with its own FK path
								// Build the full join chain from this table to the FK table with PU reference
								vector<string> full_tables_to_add;
								bool found_start = false;
								bool found_end = false;

								// Use this table's own FK path to build the join chain
								for (auto &path_table : scanned_fk_path) {
									if (path_table == scanned_table) {
										found_start = true;
										continue; // Skip the starting table itself
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

#if PRIVACY_DEBUG
								PRIVACY_DEBUG_PRINT("EnsureFKTablesInPlan: Found accessible table " + scanned_table +
								                    " #" + std::to_string(table_get.table_index) +
								                    " - adding full join chain:");
								for (auto &t : full_tables_to_add) {
									PRIVACY_DEBUG_PRINT("  - " + t);
								}
#endif
								// Create the full join chain
								unique_ptr<LogicalOperator> current_node = (*table_node)->Copy(input.context);
								idx_t fk_table_idx = DConstants::INVALID_INDEX;

								for (auto &table_to_add : full_tables_to_add) {
									auto it = check.table_metadata.find(table_to_add);
									if (it == check.table_metadata.end()) {
										throw InvalidInputException("PAC compiler: missing table metadata for table: " +
										                            table_to_add);
									}

									auto local_idx = binder.GenerateTableIndex();
									auto required_cols =
									    GetRequiredColumnsForTable(check, table_to_add, fk_path, privacy_units);
									auto table_get_new =
									    CreateLogicalGet(input.context, plan, table_to_add, local_idx, required_cols);

									if (table_to_add == fk_table_with_pu_reference) {
										fk_table_idx = local_idx;
									}

									auto join = CreateLogicalJoin(check, input.context, std::move(current_node),
									                              std::move(table_get_new));
									current_node = std::move(join);
								}

								if (fk_table_idx != DConstants::INVALID_INDEX) {
									connecting_table_to_orders_table[table_get.table_index] = fk_table_idx;
								}

								ReplaceNode(plan, *table_node, current_node, &binder);
								found_accessible_alternative = true;
								break;
							}
						}

						if (found_accessible_alternative) {
							continue; // Move to next connecting table instance
						}
					}

					if (found_accessible_alternative) {
						continue; // Move to next connecting table instance
					}

#if PRIVACY_DEBUG
					PRIVACY_DEBUG_PRINT(
					    "EnsureFKTablesInPlan: No accessible alternative found - skipping this instance");
#endif
					continue;
				}

				// Create joins for all intermediate tables
				// IMPORTANT: Use tables_to_add (NOT tables_to_join_for_instance) - this contains the path
				// from connecting_table to fk_table_with_pu_reference that we computed above
				unique_ptr<LogicalOperator> existing_node = (*target_ref)->Copy(input.context);

				// Create fresh LogicalGet nodes for this instance
				std::unordered_map<string, unique_ptr<LogicalGet>> local_get_map;

				for (auto &table : tables_to_add) {
					auto it = check.table_metadata.find(table);
					if (it == check.table_metadata.end()) {
						throw InvalidInputException("PAC compiler: missing table metadata for table: " + table);
					}
					vector<string> pks = it->second.pks;
					// Get required columns for this table (PKs and relevant FKs)
					auto required_cols = GetRequiredColumnsForTable(check, table, fk_path, privacy_units);
					auto local_idx = binder.GenerateTableIndex();
					auto get = CreateLogicalGet(input.context, plan, table, local_idx, required_cols);

					// Remember the table index of the FK table for this instance
					if (table == fk_table_with_pu_reference) {
						connecting_table_to_orders_table[connecting_table_idx] = local_idx;
					}

					local_get_map[table] = std::move(get);
				}
				unique_ptr<LogicalOperator> final_join =
				    ChainJoinsFromGetMap(check, input.context, std::move(existing_node), local_get_map, tables_to_add);
				// Replace this instance with the join chain
				ReplaceNode(plan, *target_ref, final_join, &binder);
			}
		}
	}

	return connecting_table_to_orders_table;
}

} // namespace duckdb
