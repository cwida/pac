//
// PAC Bitslice Compiler
//
// This file implements the bitslice compilation strategy for PAC (Privacy-Augmented Computation).
// The bitslice compiler transforms query plans to add necessary joins and hash expressions for
// computing PAC aggregates over privacy units.
//
// Key Concepts:
// - Privacy Unit (PU): The entity that defines privacy boundaries (e.g., customer)
// - FK Path: The chain of foreign keys from queried tables to the PU (e.g., lineitem -> orders -> customer)
// - Hash Expression: A hash computed from FK columns that reference the PU (e.g., hash(o_custkey))
// - Correlated Subqueries: Inner queries that reference outer query tables (require special handling)
//
// Created by ila on 12/21/25.
//

#include "compiler/pac_bitslice_compiler.hpp"
#include "pac_debug.hpp"
#include "utils/pac_helpers.hpp"
#include "metadata/pac_compatibility_check.hpp"
#include "compiler/pac_compiler_helpers.hpp"
#include "query_processing/pac_projection_propagation.hpp"
#include "query_processing/pac_subquery_handler.hpp"
#include "compiler/pac_bitslice_add_fkjoins.hpp"
#include "categorical/pac_categorical_rewriter.hpp"
#include "parser/pac_parser.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"

#include <cmath>

namespace duckdb {

// --- Helpers ---

// Check if a table scan is directly reachable from an operator without crossing an aggregate boundary.
// Returns false if the table is behind a nested aggregate (e.g., PU-key passthrough pattern).
static bool IsDirectlyReachable(LogicalOperator *from, idx_t table_index, bool is_start = true) {
	if (!from) {
		return false;
	}
	// Stop at nested aggregates - they create a new column scope
	if (!is_start && from->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}
	if (from->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = from->Cast<LogicalGet>();
		if (get.table_index == table_index) {
			return true;
		}
	}
	for (auto &child : from->children) {
		if (IsDirectlyReachable(child.get(), table_index, false)) {
			return true;
		}
	}
	return false;
}

// PU-key passthrough: the inner aggregate exports the PU key in its GROUP BY, so
// it doesn't need noising itself — the outer aggregate on top gets noised instead.
// Insert hash deep in the inner aggregate's subtree and propagate through its GROUP BY
// to the outer (target) aggregate.
// Returns a hash expression usable at the outer aggregate level, or nullptr on failure.
static unique_ptr<Expression> HandlePassthroughDeepHash(OptimizerExtensionInput &input,
                                                        unique_ptr<LogicalOperator> &plan, LogicalAggregate *target_agg,
                                                        LogicalAggregate *inner_agg, LogicalGet &inner_get,
                                                        const vector<string> &key_cols,
                                                        std::unordered_map<idx_t, ColumnBinding> &hash_cache) {
	auto inner_hash_binding = GetOrInsertHashProjection(input, plan, inner_get, key_cols, false, hash_cache);

	auto propagated_to_inner = PropagateSingleBinding(*plan, inner_hash_binding.table_index, inner_hash_binding,
	                                                  LogicalType::UBIGINT, inner_agg);
	if (propagated_to_inner.table_index == DConstants::INVALID_INDEX) {
		return nullptr;
	}
	// Add hash as GROUP BY column in the inner aggregate
	idx_t new_group_idx = inner_agg->groups.size();
	inner_agg->groups.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, propagated_to_inner));
	// Also add to grouping_sets so the physical HASH_GROUP_BY includes this column
	for (auto &gs : inner_agg->grouping_sets) {
		gs.insert(new_group_idx);
	}
	inner_agg->ResolveOperatorTypes();

	ColumnBinding inner_hash_output(inner_agg->group_index, inner_agg->groups.size() - 1);

	// Propagate from inner aggregate output to outer aggregate
	auto propagated_to_outer = PropagateSingleBinding(*plan, inner_hash_output.table_index, inner_hash_output,
	                                                  LogicalType::UBIGINT, target_agg);
	if (propagated_to_outer.table_index == DConstants::INVALID_INDEX) {
		return nullptr;
	}
	return make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, propagated_to_outer);
}

/**
 * ModifyPlanWithPU: Transforms a query plan when the privacy unit (PU) table IS scanned directly
 *
 * Purpose: When the query directly scans the PU table, we build hash expressions from the PU's
 * primary key columns and transform aggregates to use PAC functions.
 *
 * Arguments:
 * @param input - Optimizer extension input containing context and optimizer
 * @param plan - The logical plan to modify
 * @param pu_table_names - List of privacy unit table names that are scanned in the query
 * @param check - Compatibility check result containing table metadata
 *
 * Logic:
 * 1. Find ALL aggregate nodes in the plan
 * 2. Filter to aggregates that have at least one PU table in their subtree
 * 3. For each target aggregate:
 *    - For each PU table in the aggregate's subtree:
 *      * Use PAC_KEY columns for hashing
 *      * Build hash expression: hash(pk) or hash(xor(pk1, pk2, ...)) for composite PAC_KEYs
 *      * Propagate the hash expression through projections to the aggregate level
 *    - Combine all PU hash expressions with AND (for multi-PU queries)
 *    - Transform the aggregate to use PAC functions
 *
 * Correlated Subquery Handling:
 * - If the PU table appears in BOTH outer query and subquery, we transform aggregates in BOTH
 * - Each aggregate gets its own hash expression from the PU instance in its subtree
 * - Example: Query with outer aggregate on customer and subquery aggregate on customer:
 *   * Both aggregates are transformed with pac_sum(hash(c_custkey), value)
 *   * Each uses its respective customer table instance
 *
 * Nested Aggregate Rules (IMPORTANT):
 * - If we have 2 aggregates stacked on top of each other:
 *   * ONLY transform the inner aggregate if it directly operates on PU tables
 *   * The outer aggregate is NOT transformed if it only depends on the inner result
 *   * EXCEPTION: Transform the outer aggregate ONLY if it also has PU tables in its subtree
 *     (meaning it has its own FK path that needs joins and PAC transformation)
 *
 * Example from user's TPC-H Q17-style query:
 *   SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
 *   FROM lineitem, part
 *   WHERE p_partkey = l_partkey AND ... AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE ...)
 *
 * Transformation:
 *   - Inner aggregate (avg in subquery): Has lineitem (PU table) -> pac_avg(hash(pac_key), l_quantity)
 *   - Outer aggregate (sum): Also has lineitem (PU table) -> pac_sum(hash(pac_key), l_extendedprice)
 *   - Division by 7.0 happens AFTER PAC aggregation, not transformed
 *
 * Counter-example where outer is NOT transformed:
 *   SELECT sum(inner_sum) FROM (SELECT sum(customer_col) FROM customer) AS subq
 *   - Inner: Has customer (PU) -> pac_sum(hash(c_custkey), customer_col)
 *   - Outer: No PU tables, only depends on subquery result -> Regular sum(), NOT transformed
 */

// Check if a CTE_SCAN's bound_columns include all the given column names.
static bool CTERefExposesColumns(const LogicalCTERef &ref, const vector<string> &columns) {
	for (auto &col : columns) {
		bool found = false;
		for (auto &bound_col : ref.bound_columns) {
			if (bound_col == col) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

// Result of searching for a CTE_SCAN node that can provide hash columns for a PU.
struct CTEHashMatch {
	LogicalCTERef *cte_ref;
	vector<string> hash_columns; // columns to hash (PU PKs or FK cols)
	CTEHashMatch() : cte_ref(nullptr) {
	}
	CTEHashMatch(LogicalCTERef *r, vector<string> cols) : cte_ref(r), hash_columns(std::move(cols)) {
	}
	explicit operator bool() const {
		return cte_ref != nullptr;
	}
};

// Walk an operator's subtree to find a CTE_SCAN that can provide hash columns for a PU.
// Two paths are tried for each CTE_SCAN:
//   1. CTE directly contains the PU table → hash PU PK columns
//   2. CTE contains an FK-linked table → hash the FK columns that lead to the PU
// CTE_SCAN nodes inside DELIM_JOIN subtrees are skipped because inserting a hash
// projection there corrupts the DELIM_JOIN/DELIM_GET statistics pairing.
static CTEHashMatch FindCTEHashSource(LogicalOperator *op, const string &pu_table_name, const vector<string> &pu_pks,
                                      const CTETableMap &cte_map, const PACCompatibilityResult &check,
                                      bool inside_delim_join = false) {
	if (!op) {
		return CTEHashMatch();
	}

	if (!inside_delim_join && op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = op->Cast<LogicalCTERef>();
		auto cte_it = cte_map.find(ref.cte_index);
		if (cte_it != cte_map.end()) {
			auto &cte_tables = cte_it->second;

			// Path 1: CTE directly contains PU table → use PU PK columns
			if (cte_tables.count(pu_table_name) > 0 && CTERefExposesColumns(ref, pu_pks)) {
				return CTEHashMatch(&ref, pu_pks);
			}
			// Path 2: CTE contains FK-linked table → use FK columns
			// Sort FK path keys for deterministic behavior across platforms
			vector<string> sorted_fk_cte_keys;
			for (auto &fk_kv : check.fk_paths) {
				sorted_fk_cte_keys.push_back(fk_kv.first);
			}
			std::sort(sorted_fk_cte_keys.begin(), sorted_fk_cte_keys.end(), [&check](const string &a, const string &b) {
				auto a_it = check.fk_paths.find(a);
				auto b_it = check.fk_paths.find(b);
				size_t a_len = (a_it != check.fk_paths.end()) ? a_it->second.size() : 0;
				size_t b_len = (b_it != check.fk_paths.end()) ? b_it->second.size() : 0;
				if (a_len != b_len) {
					return a_len < b_len;
				}
				return a < b;
			});
			for (auto &fk_table : sorted_fk_cte_keys) {
				auto &fk_path = check.fk_paths.at(fk_table);
				if (fk_path.empty() || fk_path.back() != pu_table_name) {
					continue;
				}
				if (cte_tables.count(fk_table) == 0) {
					continue;
				}

				auto fk_meta_it = check.table_metadata.find(fk_table);
				if (fk_meta_it == check.table_metadata.end()) {
					continue;
				}

				// Find FK columns referencing the next table in the path toward the PU
				string next_table = fk_path.size() > 1 ? fk_path[1] : pu_table_name;
				for (auto &fk : fk_meta_it->second.fks) {
					if (fk.first == next_table && !fk.second.empty() && CTERefExposesColumns(ref, fk.second)) {
						return CTEHashMatch(&ref, fk.second);
					}
				}
			}
		}
	}
	for (auto &child : op->children) {
		bool child_in_delim = inside_delim_join || (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
		auto match = FindCTEHashSource(child.get(), pu_table_name, pu_pks, cte_map, check, child_in_delim);
		if (match) {
			return match;
		}
	}
	return CTEHashMatch();
}

void ModifyPlanWithPU(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                      const vector<string> &pu_table_names, const PACCompatibilityResult &check,
                      const CTETableMap &cte_map) {
	// Find ALL aggregate nodes in the plan first
	vector<LogicalAggregate *> all_aggregates;
	FindAllAggregates(plan, all_aggregates);

	if (all_aggregates.empty()) {
		throw InternalException("PAC Compiler: no aggregate nodes found in plan");
	}
	// Build a list of all tables that should trigger PAC transformation:
	// 1. Privacy unit tables themselves
	// 2. Tables that are FK-linked to privacy unit tables
	vector<string> relevant_tables;
	std::unordered_set<string> relevant_tables_set;

	// Add PU tables
	for (auto &pu : pu_table_names) {
		relevant_tables.push_back(pu);
		relevant_tables_set.insert(pu);
	}
	// Add FK-linked tables from the compatibility check results
	// Sort FK path keys for deterministic behavior across platforms
	vector<string> sorted_fk_path_keys;
	for (auto &kv : check.fk_paths) {
		sorted_fk_path_keys.push_back(kv.first);
	}
	std::sort(sorted_fk_path_keys.begin(), sorted_fk_path_keys.end());

	for (auto &fk_key : sorted_fk_path_keys) {
		// Add the FK table itself (the source table with the foreign key)
		if (relevant_tables_set.find(fk_key) == relevant_tables_set.end()) {
			relevant_tables.push_back(fk_key);
			relevant_tables_set.insert(fk_key);
		}
		// Add tables along the FK path
		auto &path = check.fk_paths.at(fk_key);
		for (auto &table : path) {
			if (relevant_tables_set.find(table) == relevant_tables_set.end()) {
				relevant_tables.push_back(table);
				relevant_tables_set.insert(table);
			}
		}
	}
	// Use the extended filter that handles the edge case where inner aggregate groups by PU key
	// (e.g., GROUP BY c_custkey where customer is the PU). In this case, we noise the outer
	// aggregate instead of the inner one.
	vector<LogicalAggregate *> target_aggregates =
	    FilterTargetAggregatesWithPUKeyCheck(all_aggregates, relevant_tables, check, pu_table_names, cte_map);

	if (target_aggregates.empty()) {
		throw InvalidInputException("PAC Compiler: no aggregate nodes with relevant tables found in plan");
	}

	// Cache for hash projections: get.table_index → hash column binding
	std::unordered_map<idx_t, ColumnBinding> hash_cache;

	// Cache for CTE hash projections: cte_ref.table_index → hash column binding
	// Prevents inserting duplicate projections above the same CTE_SCAN when
	// multiple aggregates try the CTE path for the same CTE_SCAN node.
	std::unordered_map<idx_t, ColumnBinding> cte_hash_cache;

	// For each target aggregate, build hash expressions and modify it
	for (auto *target_agg : target_aggregates) {
		// Build hash expressions for each privacy unit
		vector<unique_ptr<Expression>> hash_exprs;

		for (auto &pu_table_name : pu_table_names) {
			// Check if this aggregate has the PU table in its subtree
			// AND if the PU table's columns are actually accessible
			// (not blocked by MARK/SEMI/ANTI joins from IN/EXISTS subqueries)
			bool pu_in_subtree = HasTableInSubtree(target_agg, pu_table_name);
			bool pu_columns_accessible = false;
			LogicalGet *get_ptr = nullptr;

			if (pu_in_subtree) {
				get_ptr = FindTableScanInSubtree(target_agg, pu_table_name);
				if (get_ptr) {
					pu_columns_accessible = AreTableColumnsAccessible(target_agg, get_ptr->table_index);
				}
			}

			if (pu_in_subtree && pu_columns_accessible && get_ptr) {
				// Direct PU scan case: use PU's primary key
				// The PU table is in the subtree AND its columns are accessible
				auto &get = *get_ptr;

				// Get PAC_KEY columns (required for all PU tables)
				vector<string> pks;

				auto it = check.table_metadata.find(pu_table_name);
				if (it != check.table_metadata.end() && !it->second.pks.empty()) {
					pks = it->second.pks;
				} else {
					throw InternalException("PAC compiler: PU table '" + pu_table_name +
					                        "' has no PAC_KEY defined. Use ALTER PU TABLE " + pu_table_name +
					                        " ADD PAC_KEY (column_name) to define one.");
				}

				// Check if the PU table is directly reachable (not behind a nested aggregate).
				// PU-key passthrough: the table is behind an inner aggregate that groups
				// by PU key — insert hash deep and propagate through the inner's GROUP BY.
				if (!IsDirectlyReachable(target_agg, get.table_index)) {
					ColumnBinding pk_binding;
					auto *inner_agg = FindInnerAggregateWithPUKeyGroup(target_agg, check, pu_table_names, pk_binding);
					if (inner_agg) {
						auto *inner_get = FindTableScanInSubtree(inner_agg, pu_table_name);
						if (inner_get) {
							auto hash_expr = HandlePassthroughDeepHash(input, plan, target_agg, inner_agg, *inner_get,
							                                           pks, hash_cache);
							if (hash_expr) {
								hash_exprs.push_back(std::move(hash_expr));
							}
						}
					}
					continue;
				}

				// Direct path: insert hash projection above get and propagate
				auto hash_binding = GetOrInsertHashProjection(input, plan, get, pks, false, hash_cache);
				auto propagated = PropagateSingleBinding(*plan, hash_binding.table_index, hash_binding,
				                                         LogicalType::UBIGINT, target_agg);
				if (propagated.table_index == DConstants::INVALID_INDEX) {
					// Try DELIM_JOIN fallback before giving up
					auto delim_result = AddBindingToDelimJoin(plan, hash_binding.table_index, hash_binding,
					                                          LogicalType::UBIGINT, target_agg);
					if (delim_result.IsValid()) {
						propagated = delim_result.binding;
					}
				}
				if (propagated.table_index == DConstants::INVALID_INDEX) {
					continue;
				}
				auto hash_ref = make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, propagated);
				hash_exprs.push_back(std::move(hash_ref));
			} else {
				// FK-linked table case: find the deepest accessible table in the FK path
				// that has a direct FK to the PU (or closest to PU).
				// For deep chains (shipments -> order_items -> orders -> users),
				// prefer hashing orders.user_id (direct FK to PU) over shipments.item_id.
				bool found_fk_hash = false;

				for (auto &kv : check.fk_paths) {
					auto &path = kv.second;

					// Skip if this FK path doesn't lead to the current PU
					if (path.empty() || path.back() != pu_table_name) {
						continue;
					}
					// Check if any table in this path is in the aggregate's subtree
					if (!HasTableInSubtree(target_agg, kv.first)) {
						continue;
					}
					// Walk path from closest-to-PU toward the source, looking for
					// an accessible table with FK to the PU (or the next table toward PU).
					// path = [source, ..., penultimate, pu_table]
					// Try tables from penultimate back to source.
					for (int pi = static_cast<int>(path.size()) - 2; pi >= 0; pi--) {
						const string &candidate = path[pi];
						auto cand_meta_it = check.table_metadata.find(candidate);
						if (cand_meta_it == check.table_metadata.end()) {
							continue;
						}

						// Find FK from candidate to the next table in path toward PU
						const string &next_in_path = path[pi + 1];
						vector<string> fk_cols;
						for (auto &fk : cand_meta_it->second.fks) {
							if (fk.first == next_in_path) {
								fk_cols = fk.second;
								break;
							}
						}
						if (fk_cols.empty()) {
							continue;
						}
						// Check if this table is accessible in the aggregate's subtree
						auto *fk_get_ptr = FindAccessibleGetInSubtree(plan, target_agg, candidate);
						if (!fk_get_ptr) {
							continue;
						}
						// PU-key passthrough: if the FK table is behind an inner aggregate,
						// insert hash deep and propagate through the inner's GROUP BY.
						if (!IsDirectlyReachable(target_agg, fk_get_ptr->table_index)) {
							ColumnBinding pk_binding;
							auto *inner_agg =
							    FindInnerAggregateWithPUKeyGroup(target_agg, check, pu_table_names, pk_binding);
							if (inner_agg) {
								auto *inner_get = FindTableScanInSubtree(inner_agg, candidate);
								if (inner_get) {
									auto hash_expr = HandlePassthroughDeepHash(input, plan, target_agg, inner_agg,
									                                           *inner_get, fk_cols, hash_cache);
									if (hash_expr) {
										hash_exprs.push_back(std::move(hash_expr));
										found_fk_hash = true;
										break;
									}
								}
							}
							continue;
						}
						// Direct path: insert hash projection and propagate
						auto fk_hash_binding =
						    GetOrInsertHashProjection(input, plan, *fk_get_ptr, fk_cols, false, hash_cache);
						auto fk_propagated = PropagateSingleBinding(*plan, fk_hash_binding.table_index, fk_hash_binding,
						                                            LogicalType::UBIGINT, target_agg);

						// If propagation failed, try DELIM_JOIN fallback
						if (fk_propagated.table_index == DConstants::INVALID_INDEX) {
							auto delim_result = AddBindingToDelimJoin(
							    plan, fk_hash_binding.table_index, fk_hash_binding, LogicalType::UBIGINT, target_agg);
							if (delim_result.IsValid()) {
								fk_propagated = delim_result.binding;
							}
						}
						if (fk_propagated.table_index == DConstants::INVALID_INDEX) {
							continue;
						}
						auto fk_hash_ref = make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, fk_propagated);
						hash_exprs.push_back(std::move(fk_hash_ref));
						found_fk_hash = true;
						break; // Found a working table in this path
					}
					if (found_fk_hash) {
						break; // Only process one FK path per PU per aggregate
					}
				}
			}
		}

		// If no hash expressions were built via direct scan or FK paths,
		// try the CTE path: find CTE_SCAN nodes that transitively reference PU tables
		// (directly or through FK chains) and expose the relevant key columns
		if (hash_exprs.empty() && !cte_map.empty()) {
			for (auto &pu_table_name : pu_table_names) {
				vector<string> pu_pks;
				auto it = check.table_metadata.find(pu_table_name);
				if (it != check.table_metadata.end() && !it->second.pks.empty()) {
					pu_pks = it->second.pks;
				}
				if (pu_pks.empty()) {
					continue;
				}

				auto match = FindCTEHashSource(target_agg, pu_table_name, pu_pks, cte_map, check);
				if (!match) {
					continue;
				}
				ColumnBinding hash_binding;
				auto cte_cache_it = cte_hash_cache.find(match.cte_ref->table_index);
				if (cte_cache_it != cte_hash_cache.end()) {
					hash_binding = cte_cache_it->second;
				} else {
					hash_binding = InsertHashProjectionAboveCTERef(input, plan, *match.cte_ref, match.hash_columns);
					if (hash_binding.table_index == DConstants::INVALID_INDEX) {
						continue;
					}
					cte_hash_cache[match.cte_ref->table_index] = hash_binding;
				}

				auto propagated = PropagateSingleBinding(*plan, hash_binding.table_index, hash_binding,
				                                         LogicalType::UBIGINT, target_agg);

				// If propagation failed (e.g., blocked by DELIM_JOIN), try AddBindingToDelimJoin
				if (propagated.table_index == DConstants::INVALID_INDEX) {
					auto delim_result = AddBindingToDelimJoin(plan, hash_binding.table_index, hash_binding,
					                                          LogicalType::UBIGINT, target_agg);
					if (delim_result.IsValid()) {
						propagated = delim_result.binding;
					}
				}
				if (propagated.table_index == DConstants::INVALID_INDEX) {
					continue;
				}
				auto hash_ref = make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, propagated);
				hash_exprs.push_back(std::move(hash_ref));
			}
		}

		// Skip if no hash expressions were built for this aggregate
		if (hash_exprs.empty()) {
			continue;
		}

		// Compute correction factor for multi-PU: 2^(m-1) where m = number of PU hashes
		double correction = (hash_exprs.size() > 1) ? std::pow(2.0, static_cast<double>(hash_exprs.size() - 1)) : 1.0;

		// Combine all hash expressions with AND
		auto combined_hash_expr = BuildAndFromHashes(input, hash_exprs);

		// Modify this aggregate with PAC functions
		ModifyAggregatesWithPacFunctions(input, target_agg, combined_hash_expr, plan, correction);
	}
}

void CompilePacBitsliceQuery(const PACCompatibilityResult &check, OptimizerExtensionInput &input,
                             unique_ptr<LogicalOperator> &plan, const vector<string> &privacy_units,
                             const string &query, const string &query_hash) {
#if PAC_DEBUG
	PAC_DEBUG_PRINT("=== PAC BITSLICE COMPILATION ===");
	PAC_DEBUG_PRINT("Query hash: " + query_hash);
	PAC_DEBUG_PRINT("Query: " + query.substr(0, 100) + (query.length() > 100 ? "..." : ""));
	PAC_DEBUG_PRINT("Privacy units: " + std::to_string(privacy_units.size()));
	for (auto &pu : privacy_units) {
		PAC_DEBUG_PRINT("  " + pu);
	}
	PAC_DEBUG_PRINT("Scanned PU tables: " + std::to_string(check.scanned_pu_tables.size()));
	PAC_DEBUG_PRINT("Scanned non-PU tables: " + std::to_string(check.scanned_non_pu_tables.size()));
#endif
	// Resolve operator types on the raw plan so that .types vectors are populated.
	// In the pre-optimizer phase, ResolveOperatorTypes hasn't run yet.
	plan->ResolveOperatorTypes();

	// Generate filename with all PU names concatenated
	string path = GetPacCompiledPath(input.context, ".");
	if (!path.empty() && path.back() != '/') {
		path.push_back('/');
	}
	string pu_names_joined;
	for (size_t i = 0; i < privacy_units.size(); ++i) {
		if (i > 0) {
			pu_names_joined += "_";
		}
		pu_names_joined += privacy_units[i];
	}
	string filename = path + pu_names_joined + "_" + query_hash + "_bitslice.sql";

	// The bitslice compiler works in the following way:
	// a) the query scans PU table(s):
	// a.1) each PU table has 1 PK: we hash it
	// a.2) each PU table has multiple PKs: we XOR them and hash the result
	// a.3) each PU table must have a PAC_KEY (no rowid fallback)
	// a.4) we AND all the hashes together for multiple PUs
	// b) the query does not scan PU table(s):
	// b.1) we follow the FK path to find the PK(s) of each PU table
	// b.2) we join the chain of tables from the scanned table to each PU table (deduplicating)
	// b.3) we hash the PK(s) as in a) and AND them together

	// Build the CTE table map once — used for both routing and aggregate filtering
	auto cte_map = BuildAndResolveCTETableMap(plan.get());

	// Determine if a PU table is reachable from the plan (directly scanned, via CTE, or via FK+CTE)
	bool pu_present_in_tree = !check.scanned_pu_tables.empty();
	bool pu_via_cte = false;

	if (!pu_present_in_tree) {
		for (auto &cte_kv : cte_map) {
			auto &cte_tables = cte_kv.second;
			for (auto &pu : privacy_units) {
				if (cte_tables.count(pu) > 0) {
					pu_via_cte = true;
					break;
				}
			}
			if (!pu_via_cte) {
				for (auto &fk_kv : check.fk_paths) {
					if (cte_tables.count(fk_kv.first) > 0) {
						pu_via_cte = true;
						break;
					}
				}
			}
			if (pu_via_cte) {
				break;
			}
		}
		pu_present_in_tree = pu_via_cte;
	}
#if PAC_DEBUG
	PAC_DEBUG_PRINT("=== PLAN BEFORE PAC TRANSFORMATION ===");
	plan->Print();
#endif
	if (!pu_present_in_tree && !check.fk_paths.empty()) {
		// Phase 1: PU not reachable — add FK joins so plan has all tables needed
		string start_table;
		vector<string> target_pus;
		vector<string> gets_present, gets_missing;
		PopulateGetsFromFKPath(check, gets_present, gets_missing, start_table, target_pus);

		auto it = check.fk_paths.find(start_table);
		if (it == check.fk_paths.end() || it->second.empty()) {
			throw InvalidInputException("PAC compiler: expected fk_path for start table " + start_table);
		}

		// Deduplicate missing tables
		std::unordered_set<string> missing_set(gets_missing.begin(), gets_missing.end());
		vector<string> unique_gets_missing(missing_set.begin(), missing_set.end());
		std::sort(unique_gets_missing.begin(), unique_gets_missing.end());

		AddMissingFKJoins(check, input, plan, unique_gets_missing, gets_present, it->second, privacy_units, cte_map);
	}
	// Phase 2: always transform aggregates via unified path
	{
		auto &pu_names = (pu_present_in_tree && !pu_via_cte) ? check.scanned_pu_tables : privacy_units;
		ModifyPlanWithPU(input, plan, pu_names, check, cte_map);
	}

	// ============================================================================
	// CATEGORICAL QUERY HANDLING
	// ============================================================================
	// After the standard PAC transformation, check if this is a categorical query
	// (outer query compares against inner PAC aggregate without its own aggregate).
	// If so, rewrite to use _counters variants and pac_filter for probabilistic filtering.
	if (GetBooleanSetting(input.context, "pac_categorical", true)) {
		RewriteCategoricalQuery(input, plan);
	}
#if PAC_DEBUG
	PAC_DEBUG_PRINT("=== PAC-OPTIMIZED PLAN ===");
	plan->Print();
	PAC_DEBUG_PRINT("=== PAC COMPILATION END ===");
#endif
}
} // namespace duckdb
