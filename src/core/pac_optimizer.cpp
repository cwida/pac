//
// Created by ila on 12/12/25.
//

#include "core/pac_optimizer.hpp"
#include "aggregates/pac_aggregate.hpp"
#include "pac_debug.hpp"
#include <string>
#include <algorithm>

#include "utils/pac_helpers.hpp"
// Include PAC bitslice compiler
#include "compiler/pac_bitslice_compiler.hpp"
// Include PAC parser for metadata management
#include "parser/pac_parser.hpp"
// Include utility diff
#include "diff/pac_utility_diff.hpp"
// Include derived_pu rewriter (counter conversion + pac_finalize injection)
#include "query_processing/pac_derived_rewriter.hpp"
// Include deep copy for utility diff
#include "duckdb/planner/logical_operator_deep_copy.hpp"
#include "duckdb/optimizer/optimizer.hpp"
// Include DuckDB headers for DROP operation handling
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
// Include DuckDB headers for CTAS metadata propagation
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "metadata/pac_metadata_manager.hpp"
#include "metadata/pac_compatibility_check.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

#include <stack>

namespace duckdb {

// ============================================================================
// PACDropTableRule - Separate optimizer rule for DROP TABLE operations
// ============================================================================

void PACDropTableRule::PACDropTableRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan || plan->type != LogicalOperatorType::LOGICAL_DROP) {
		return;
	}
	// Cast to LogicalSimple to access the parse info (DROP operations use LogicalSimple)
	auto &simple = plan->Cast<LogicalSimple>();
	if (simple.info->info_type != ParseInfoType::DROP_INFO) {
		return;
	}
	// Cast to DropInfo to access drop details
	auto &drop_info = simple.info->Cast<DropInfo>();
	if (drop_info.type != CatalogType::TABLE_ENTRY) {
		return; // Only handle DROP TABLE operations
	}
	string table_name = drop_info.name;

	// Check if this table has PAC metadata
	auto &metadata_mgr = PACMetadataManager::Get();
	if (!metadata_mgr.HasMetadata(table_name)) {
		return; // No metadata for this table, nothing to clean up
	}
#if PAC_DEBUG
	std::cerr << "[PAC DEBUG] DROP TABLE detected for table with PAC metadata: " << table_name << "\n";
#endif
	// Check if any other tables have PAC LINKs pointing to this table
	auto all_tables = metadata_mgr.GetAllTableNames();
	vector<string> tables_with_links_to_dropped;

	for (const auto &other_table : all_tables) {
		if (StringUtil::Lower(other_table) == StringUtil::Lower(table_name)) {
			continue; // Skip the table being dropped
		}
		auto other_metadata = metadata_mgr.GetTableMetadata(other_table);
		if (!other_metadata) {
			continue;
		}
		// Check if this table has any links to the table being dropped
		for (const auto &link : other_metadata->links) {
			if (StringUtil::Lower(link.referenced_table) == StringUtil::Lower(table_name)) {
				tables_with_links_to_dropped.push_back(other_table);
				break;
			}
		}
	}
	// Remove links from other tables that reference the dropped table
	for (const auto &other_table : tables_with_links_to_dropped) {
		auto other_metadata = metadata_mgr.GetTableMetadata(other_table);
		if (!other_metadata) {
			continue;
		}

		// Make a copy and remove links
		PACTableMetadata updated_metadata = *other_metadata;
		updated_metadata.links.erase(std::remove_if(updated_metadata.links.begin(), updated_metadata.links.end(),
		                                            [&table_name](const PACLink &link) {
			                                            return StringUtil::Lower(link.referenced_table) ==
			                                                   StringUtil::Lower(table_name);
		                                            }),
		                             updated_metadata.links.end());

		// Update the metadata
		metadata_mgr.AddOrUpdateTable(other_table, updated_metadata);
#if PAC_DEBUG
		std::cerr << "[PAC DEBUG] Removed PAC LINKs from table '" << other_table << "' that referenced dropped table '"
		          << table_name << "'"
		          << "\n";
#endif
	}
	// Remove metadata for the dropped table
	metadata_mgr.RemoveTable(table_name);
#if PAC_DEBUG
	std::cerr << "[PAC DEBUG] Removed PAC metadata for dropped table: " << table_name << "\n";
#endif
	// Save updated metadata to file
	string metadata_path = PACMetadataManager::GetMetadataFilePath(input.context);
	if (!metadata_path.empty()) {
		metadata_mgr.SaveToFile(metadata_path);
#if PAC_DEBUG
		std::cerr << "[PAC DEBUG] Saved updated PAC metadata after DROP TABLE"
		          << "\n";
#endif
	}
}

// ============================================================================
// CTAS metadata propagation helper
// ============================================================================

// When a CTAS sources from a PU or derived_pu table, mark the new table as
// derived_pu and propagate PAC_KEY columns. derived_pu tables behave as:
//   - SELECT: untouched (no PAC check/noise)
//   - INSERT/UPDATE containing aggregation: PAC noise injected
//
// Handles column renames (e.g. SELECT value AS amount) by building a mapping
// from source column names to destination column names via the new table's ColumnList.
static void PropagateCTASMetadata(unique_ptr<LogicalOperator> &outer_plan, unique_ptr<LogicalOperator> &select_plan,
                                  OptimizerExtensionInput &input) {
	auto &create_table = outer_plan->Cast<LogicalCreateTable>();
	string new_table = create_table.info->Base().table;
	auto &mgr = PACMetadataManager::Get();
	if (mgr.HasMetadata(new_table)) {
		return;
	}

	// Walk the select plan to find the source PU/derived_pu GET and its metadata,
	// and check if the plan contains an aggregate (which means the output is already
	// aggregated — protected columns should not be propagated to the result table).
	std::stack<LogicalOperator *> stack;
	stack.push(select_plan.get());
	const PACTableMetadata *source_meta = nullptr;
	LogicalGet *source_get = nullptr;
	bool has_aggregate = false;

	while (!stack.empty()) {
		auto *node = stack.top();
		stack.pop();
		if (node->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			has_aggregate = true;
		}
		if (node->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = node->Cast<LogicalGet>();
			auto entry = get.GetTable();
			if (!entry) {
				continue;
			}
			auto *meta = mgr.GetTableMetadata(entry->name);
			if (meta && (meta->is_privacy_unit || meta->derived_pu)) {
				source_meta = meta;
				source_get = &get;
			}
		}
		for (auto &child : node->children) {
			stack.push(child.get());
		}
	}

	if (!source_meta) {
		return;
	}

	// Build OID → column name map from catalog (O(n) lookup instead of O(n*m))
	auto entry = source_get->GetTable();
	std::unordered_map<idx_t, string> oid_to_name;
	if (entry) {
		for (auto &col : entry->GetColumns().Logical()) {
			oid_to_name[col.Oid()] = col.Name();
		}
	}

	// Build source GET column index → source column name mapping
	std::unordered_map<idx_t, string> get_col_idx_to_name;
	auto &col_ids = source_get->GetColumnIds();
	for (idx_t i = 0; i < col_ids.size(); i++) {
		auto &col_idx = col_ids[i];
		if (col_idx.IsRowIdColumn()) {
			continue;
		}
		auto it = oid_to_name.find(col_idx.GetPrimaryIndex());
		if (it != oid_to_name.end()) {
			get_col_idx_to_name[i] = it->second;
		}
	}

	// Build source name → dest name mapping using the new table's column list.
	// The new table columns correspond positionally to the GET's column_ids
	// (for simple SELECT * / SELECT col AS alias queries).
	auto &new_columns = create_table.info->Base().columns;
	std::unordered_map<string, string> src_to_dest_name;

	idx_t dest_idx = 0;
	for (auto &new_col : new_columns.Logical()) {
		auto name_it = get_col_idx_to_name.find(dest_idx);
		if (name_it != get_col_idx_to_name.end()) {
			src_to_dest_name[name_it->second] = new_col.Name();
		}
		dest_idx++;
	}

	// Map source PAC_KEY and protected columns to destination names
	PACTableMetadata prop(new_table);
	prop.derived_pu = true;

	for (auto &pk : source_meta->primary_key_columns) {
		auto it = src_to_dest_name.find(pk);
		prop.primary_key_columns.push_back(it != src_to_dest_name.end() ? it->second : pk);
	}

	// Only propagate protected columns that appear directly in the output — not those
	// consumed by aggregate functions. For non-aggregate CTASes (schema copies), use
	// the positional rename mapping. For aggregate CTASes, only propagate if the source
	// protected column name appears directly in the new table's columns (i.e., it's a
	// GROUP BY column that passes through unchanged).
	if (!has_aggregate) {
		// No aggregate: full propagation with rename support
		for (auto &prot : source_meta->protected_columns) {
			auto it = src_to_dest_name.find(prot);
			prop.protected_columns.push_back(it != src_to_dest_name.end() ? it->second : prot);
		}
	} else {
		// Aggregate query: only propagate protected columns that appear directly
		// in the new table's column list (GROUP BY pass-through columns).
		// Note: the positional GET→dest mapping doesn't hold for aggregates, so we
		// fall back to a simple name check against the output column names.
		std::unordered_set<string> new_col_names;
		for (auto &new_col : new_columns.Logical()) {
			new_col_names.insert(new_col.Name());
		}
		for (auto &prot : source_meta->protected_columns) {
			if (new_col_names.count(prot) > 0) {
				prop.protected_columns.push_back(prot);
			}
		}
	}

	// Track counter columns: for non-aggregate CTAS from derived_pu, propagate the source's
	// counter_columns. For aggregate CTAS, counter_columns are populated after ConvertDerivedPuToCounters
	// sets the column types (see the DML target_table block below).
	if (!has_aggregate && source_meta->derived_pu) {
		for (auto &cc : source_meta->counter_columns) {
			auto it = src_to_dest_name.find(cc);
			prop.counter_columns.push_back(it != src_to_dest_name.end() ? it->second : cc);
		}
	}

	mgr.AddOrUpdateTable(new_table, prop);

	string path = PACMetadataManager::GetMetadataFilePath(input.context);
	if (!path.empty()) {
		mgr.SaveToFile(path);
	}
}

// ============================================================================
// PACDerivedTypePatcher — patches statement types after physical planning
// ============================================================================
// When the optimizer changes output types (e.g. LIST<FLOAT> → FLOAT via pac_finalize),
// the statement's result types (set at plan time) become stale. This state patches them
// in OnFinalizePrepare, which fires after the physical plan is generated.
struct PACDerivedTypePatcher : public ClientContextState {
	// Must return true so OnFinalizePrepare is called (DuckDB skips it otherwise).
	// This causes DuckDB to plan on a statement copy — minor overhead for all queries
	// when PAC is loaded. Only the actual type patching is gated on SELECT + type mismatch.
	bool CanRequestRebind() override {
		return true;
	}
	RebindQueryInfo OnFinalizePrepare(ClientContext &context, PreparedStatementData &prepared,
	                                  PreparedStatementMode mode) override {
		if (!prepared.physical_plan || prepared.statement_type != StatementType::SELECT_STATEMENT) {
			return RebindQueryInfo::DO_NOT_REBIND;
		}
		auto &root_types = prepared.physical_plan->Root().GetTypes();
		if (root_types.size() == prepared.types.size()) {
			for (idx_t i = 0; i < root_types.size(); i++) {
				if (root_types[i] != prepared.types[i]) {
					prepared.types[i] = root_types[i];
				}
			}
		}
		return RebindQueryInfo::DO_NOT_REBIND;
	}
};

// ============================================================================
// PACRewriteRule - Main PAC query rewriting optimizer rule
// ============================================================================

static bool PlanHasAggregate(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return true;
	}
	for (auto &child : op.children) {
		if (PlanHasAggregate(*child)) {
			return true;
		}
	}
	return false;
}

// PAC rewrites run in the pre-optimizer phase, BEFORE DuckDB's built-in optimizers.
// This way DuckDB's join ordering, filter pushdown, column lifetime, compressed
// materialization etc. all run on the PAC-transformed plan automatically.
void PACRewriteRule::PACPreOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	// Propagate CTAS metadata even when pac_rewrite is disabled, so that
	// tables created via CTAS (e.g. IVM delta tables) inherit PAC_KEY and
	// PROTECTED columns from their source tables.
	bool pac_ctas_enabled = GetBooleanSetting(input.context, "pac_ctas", true);
	if (pac_ctas_enabled && plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE && !plan->children.empty()) {
		PropagateCTASMetadata(plan, plan->children[0], input);
	}
	// When pac_rewrite is disabled (e.g. by IVM during internal delta plan derivation),
	// skip the rest of PAC optimization. This is separate from pac_check, which
	// only controls the protected column access restrictions.
	bool pac_rewrite_enabled = GetBooleanSetting(input.context, "pac_rewrite", true);
	if (!pac_rewrite_enabled) {
		return;
	}
	// Register type patcher to fix stale statement types after optimizer changes output types.
	// Read path (pac_finalize injection) is handled by PACDerivedReadRule (post-optimizer)
	// so it runs after DuckDB's built-in projection removal optimizer.
	input.context.registered_state->GetOrCreate<PACDerivedTypePatcher>("pac_derived_type_patcher");
	// If the optimizer extension provided a PACOptimizerInfo, and a replan is already in progress,
	// skip running the PAC checks to avoid re-entrant behavior.
	PACOptimizerInfo *pac_info = nullptr;
	if (input.info) {
		pac_info = dynamic_cast<PACOptimizerInfo *>(input.info.get());
		if (pac_info && pac_info->replan_in_progress.load(std::memory_order_acquire)) {
			return;
		}
	}
	// Run the PAC compatibility checks only if the plan is a projection, order by, or aggregate (i.e., a SELECT query)
	// For EXPLAIN/EXPLAIN_ANALYZE, look at the child operator to decide whether to rewrite
	LogicalOperator *check_plan = plan.get();
	PAC_DEBUG_PRINT("[PAC TRACE] plan->type = " + std::to_string((int)plan->type));
	if (plan->type == LogicalOperatorType::LOGICAL_EXPLAIN && !plan->children.empty()) {
		check_plan = plan->children[0].get();
	}
	// Drill through DML wrappers (INSERT...SELECT, CREATE TABLE AS SELECT) to reach the SELECT plan.
	// This allows PAC to fire on materialized view creation and IVM delta queries.
	if ((check_plan->type == LogicalOperatorType::LOGICAL_INSERT ||
	     check_plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) &&
	    !check_plan->children.empty()) {
		PAC_DEBUG_PRINT("[PAC TRACE] DML wrapper detected, drilling to child: " +
		                std::to_string((int)check_plan->children[0]->type));
		check_plan = check_plan->children[0].get();
	}
	// TODO: why this particular collection of operators? Maybe check against DDL or DML
	// For DISTINCT, look through to the child — SELECT DISTINCT has PROJECTION underneath,
	// but UNION's deduplication has LOGICAL_UNION underneath and should not enter PAC.
	if (check_plan->type == LogicalOperatorType::LOGICAL_DISTINCT && !check_plan->children.empty()) {
		check_plan = check_plan->children[0].get();
	}
	PAC_DEBUG_PRINT("[PAC TRACE] check_plan->type = " + std::to_string((int)check_plan->type));
	if (check_plan->type != LogicalOperatorType::LOGICAL_PROJECTION &&
	    check_plan->type != LogicalOperatorType::LOGICAL_ORDER_BY &&
	    check_plan->type != LogicalOperatorType::LOGICAL_TOP_N &&
	    check_plan->type != LogicalOperatorType::LOGICAL_LIMIT &&
	    check_plan->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY &&
	    check_plan->type != LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		PAC_DEBUG_PRINT("[PAC TRACE] check_plan type NOT in allowed list, returning early");
		return;
	}
	// For EXPLAIN queries, we need to operate on the child plan
	bool is_explain = (plan->type == LogicalOperatorType::LOGICAL_EXPLAIN && !plan->children.empty());
	unique_ptr<LogicalOperator> &outer_plan = is_explain ? plan->children[0] : plan;
	// For INSERT...SELECT and CREATE TABLE AS SELECT, operate on the SELECT child
	bool is_dml_wrapper = ((outer_plan->type == LogicalOperatorType::LOGICAL_INSERT ||
	                        outer_plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) &&
	                       !outer_plan->children.empty());
	// For WITH ... INSERT (CTE-wrapped DML), the INSERT is nested inside MATERIALIZED_CTEs.
	// Detect it by walking the consumer chain (child[1]).
	bool is_cte_dml = false;
	if (!is_dml_wrapper && outer_plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		LogicalOperator *inner = outer_plan.get();
		while (inner->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && inner->children.size() > 1) {
			inner = inner->children[1].get();
		}
		is_cte_dml = (inner->type == LogicalOperatorType::LOGICAL_INSERT ||
		              inner->type == LogicalOperatorType::LOGICAL_CREATE_TABLE);
	}
	// target_plan: for direct DML (INSERT/CTAS), use the SELECT child.
	// For CTE-wrapped DML, use the full CTE tree (PAC needs to rewrite aggregates inside CTEs).
	unique_ptr<LogicalOperator> &target_plan = is_dml_wrapper ? outer_plan->children[0] : outer_plan;

	// Delegate compatibility checks (including detecting PAC table presence and internal sample scans)
	PACCompatibilityResult check = PACRewriteQueryCheck(target_plan, input.context, pac_info);

	// For DML wrappers (INSERT/CTAS, including CTE-wrapped), check if any scanned tables are derived_pu.
	// derived_pu tables are transparent to SELECT but trigger PAC noise on DML aggregates.
	PAC_DEBUG_PRINT("[PAC TRACE] is_dml_wrapper=" + std::to_string(is_dml_wrapper) + " is_cte_dml=" +
	                std::to_string(is_cte_dml) + " fk_paths.empty=" + std::to_string(check.fk_paths.empty()) +
	                " scanned_pu_tables.empty=" + std::to_string(check.scanned_pu_tables.empty()));
	if ((is_dml_wrapper || is_cte_dml) && check.fk_paths.empty() && check.scanned_pu_tables.empty() &&
	    PlanHasAggregate(*target_plan)) {
		auto &mgr = PACMetadataManager::Get();
		std::unordered_map<string, idx_t> scan_counts;
		CountScans(*target_plan, scan_counts);
		PAC_DEBUG_PRINT("[PAC TRACE] CountScans found " + std::to_string(scan_counts.size()) + " tables:");
		for (auto &kv : scan_counts) {
			auto *meta = mgr.GetTableMetadata(kv.first);
			PAC_DEBUG_PRINT("[PAC TRACE]   " + kv.first + " count=" + std::to_string(kv.second) +
			                " has_meta=" + std::to_string(meta != nullptr) +
			                " derived_pu=" + std::to_string(meta ? meta->derived_pu : false));
			if (kv.second > 0 && meta && meta->derived_pu) {
				check.scanned_pu_tables.push_back(kv.first);
				check.eligible_for_rewrite = true;
				// Populate table_metadata so the compiler knows the PAC_KEY
				ColumnMetadata cm;
				cm.table_name = kv.first;
				cm.pks = meta->primary_key_columns;
				check.table_metadata[kv.first] = cm;
			}
		}
	}

	if (check.fk_paths.empty() && check.scanned_pu_tables.empty()) {
		return;
	}

	// Determine the set of discovered privacy units
	vector<string> discovered_pus;
	for (auto &kv : check.fk_paths) {
		if (!kv.second.empty()) {
			discovered_pus.push_back(kv.second.back());
		}
	}
	for (auto &t : check.scanned_pu_tables) {
		discovered_pus.push_back(t);
	}
	std::sort(discovered_pus.begin(), discovered_pus.end());
	discovered_pus.erase(std::unique(discovered_pus.begin(), discovered_pus.end()), discovered_pus.end());
	if (discovered_pus.empty()) {
		return;
	}

	// Compute normalized query hash once for file naming.
	// When another optimizer extension (e.g. OpenIVM's IVM rewrite) replans a query
	// inside its own hook, the new Optimizer runs on a Connection context whose
	// active_query is NULL. DuckDB's GetCurrentQuery() dereferences that unique_ptr
	// and throws InternalException. The query string is only used for the compiled-
	// file hash, so we fall back to a fixed string to let PAC compilation proceed
	// (needed so delta queries get pac_noised_sum / pac_hash rewrites).
	string current_query;
	try {
		current_query = input.context.GetCurrentQuery();
	} catch (InternalException &) {
		current_query = "replan";
	}
	string normalized = NormalizeQueryForHash(current_query);
	string query_hash = HashStringToHex(normalized);
	vector<string> privacy_units = std::move(discovered_pus);
#if PAC_DEBUG
	for (auto &pu : privacy_units) {
		auto it = check.table_metadata.find(pu);
		if (it != check.table_metadata.end() && !it->second.pks.empty()) {
			PAC_DEBUG_PRINT("Discovered primary key columns for privacy unit '" + pu + "':");
			for (const auto &col : it->second.pks) {
				PAC_DEBUG_PRINT(col);
			}
		}
	}
#endif
	// Check if utility diff is requested — deep-copy BEFORE any compilation
	unique_ptr<LogicalOperator> ref_plan;
	idx_t num_diffcols = 0;
	string diff_output_path;
	bool do_diff = false;
	{
		Value val;
		if (input.context.TryGetCurrentSetting("pac_diffcols", val) && !val.IsNull()) {
			string val_str = val.ToString();
			if (!val_str.empty()) {
				if (std::isdigit(val_str[0])) {
					auto colon = val_str.find(':');
					num_diffcols = static_cast<idx_t>(std::stoi(val_str.substr(0, colon)));
					if (colon != string::npos) {
						diff_output_path = val_str.substr(colon + 1);
					}
				} else {
					num_diffcols = DConstants::INVALID_INDEX;
					diff_output_path = (val_str[0] == ':') ? val_str.substr(1) : val_str;
				}
				do_diff = true;
				LogicalOperatorDeepCopy copier(input.optimizer.binder, nullptr);
				ref_plan = copier.DeepCopy(target_plan);
			}
		}
	}

	if (check.eligible_for_rewrite) {
		bool apply_noise = IsPacNoiseEnabled(input.context, true);
		if (apply_noise) {
#if PAC_DEBUG
			PAC_DEBUG_PRINT("Query requires PAC Compilation for privacy units:");
			for (const auto &pu_name : privacy_units) {
				PAC_DEBUG_PRINT("  " + pu_name);
			}
#endif
			// set replan flag for duration of compilation
			ReplanGuard scoped2(pac_info);
			CompilePacBitsliceQuery(check, input, target_plan, privacy_units, normalized, query_hash);

			// For DML targeting derived_pu tables: convert pac_noised_* → pac_* counter variants
			// so the table stores raw 64-element counter lists instead of noised scalars.
			// Check the DML TARGET table (not source tables) for derived_pu.
			// PropagateCTASMetadata already ran (line 276) and set derived_pu on the target.
			if (is_dml_wrapper) {
				string target_table;
				if (outer_plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
					auto &create = outer_plan->Cast<LogicalCreateTable>();
					target_table = create.info->Base().table;
				} else if (outer_plan->type == LogicalOperatorType::LOGICAL_INSERT) {
					auto &insert = outer_plan->Cast<LogicalInsert>();
					target_table = insert.table.name;
				}
				if (!target_table.empty()) {
					auto &mgr = PACMetadataManager::Get();
					auto *meta = mgr.GetTableMetadata(target_table);
					PAC_DEBUG_PRINT("[PAC TRACE] DML target='" + target_table +
					                "' has_meta=" + std::to_string(meta != nullptr) +
					                " derived_pu=" + std::to_string(meta ? meta->derived_pu : false));
					if (meta && meta->derived_pu) {
						PAC_DEBUG_PRINT("[PAC TRACE] Converting to counters for derived_pu table");
						ConvertDerivedPuToCounters(input, target_plan);
						// Resolve types so parent operators reflect the counter conversion
						target_plan->ResolveOperatorTypes();
						// Update CTAS column types and record counter columns in metadata
						if (outer_plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
							auto &create = outer_plan->Cast<LogicalCreateTable>();
							auto &columns = create.info->Base().columns;
							auto &plan_types = target_plan->types;
							auto counter_list_type = LogicalType::LIST(PacFloatLogicalType());
							auto *mut_meta = mgr.GetMutableTableMetadata(target_table);
							for (idx_t i = 0; i < plan_types.size() && i < columns.LogicalColumnCount(); i++) {
								auto &col = columns.GetColumnMutable(LogicalIndex(i));
								if (col.Type() != plan_types[i]) {
									col.SetType(plan_types[i]);
								}
								if (plan_types[i] == counter_list_type && mut_meta) {
									mut_meta->counter_columns.push_back(col.Name());
								}
							}
						}
					}
				}
			}
		}
	}

	if (do_diff) {
		ApplyUtilityDiff(input, target_plan, std::move(ref_plan), num_diffcols, diff_output_path);
	}
}

// ============================================================================
// PACDerivedReadRule — post-optimizer: finalize derived_pu counter columns
// ============================================================================
// Runs AFTER DuckDB's built-in optimizers (which may remove trivial projections).
// Wraps LIST<FLOAT> column refs from derived_pu tables with pac_finalize().
void PACDerivedReadRule::PACDerivedReadFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	bool pac_rewrite_enabled = GetBooleanSetting(input.context, "pac_rewrite", true);
	if (!pac_rewrite_enabled) {
		return;
	}
	// Skip pac_finalize injection for DML — these must operate on raw counter data.
	// CTAS is included: copying from a derived_pu table should preserve counter lists.
	if (plan->type == LogicalOperatorType::LOGICAL_INSERT || plan->type == LogicalOperatorType::LOGICAL_UPDATE ||
	    plan->type == LogicalOperatorType::LOGICAL_DELETE || plan->type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
		return;
	}
	// Also check for CTE-wrapped DML (WITH ... INSERT INTO ...)
	if (plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		LogicalOperator *inner = plan.get();
		while (inner->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && inner->children.size() > 1) {
			inner = inner->children[1].get();
		}
		if (inner->type == LogicalOperatorType::LOGICAL_INSERT || inner->type == LogicalOperatorType::LOGICAL_UPDATE ||
		    inner->type == LogicalOperatorType::LOGICAL_DELETE) {
			return;
		}
	}
	if (!HasDerivedPuCounterGets(plan.get())) {
		return;
	}
	PAC_DEBUG_PRINT("[PAC DERIVED READ] === PLAN BEFORE pac_finalize injection ===");
#if PAC_DEBUG
	plan->Print();
#endif
	InjectPacFinalizeForDerivedPu(input, plan);
	PAC_DEBUG_PRINT("[PAC DERIVED READ] === PLAN AFTER pac_finalize injection ===");
#if PAC_DEBUG
	plan->Print();
#endif
}

} // namespace duckdb
