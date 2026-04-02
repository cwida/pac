#define DUCKDB_EXTENSION_MAIN

#include "pac_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <fstream>
#include <unordered_set>

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/types.hpp"
#include "core/pac_optimizer.hpp"
#include "aggregates/pac_aggregate.hpp"
#include "aggregates/pac_count.hpp"
#include "aggregates/pac_sum.hpp"
#include "aggregates/pac_clip_sum.hpp"
#include "aggregates/pac_min_max.hpp"
#include "aggregates/pac_clip_min_max.hpp"
#include "categorical/pac_categorical.hpp"
#include "parser/pac_parser.hpp"
#include "diff/pac_utility_diff.hpp"
#include "query_processing/pac_topk_rewriter.hpp"
#include "query_processing/pac_avg_rewriter.hpp"
#include "pac_debug.hpp"

namespace duckdb {

// Pragma function to save PAC metadata to file
static void SavePACMetadataPragma(ClientContext &context, const FunctionParameters &parameters) {
	auto filepath = parameters.values[0].ToString();
	PACMetadataManager::Get().SaveToFile(filepath);
}

// Pragma function to load PAC metadata from file
static void LoadPACMetadataPragma(ClientContext &context, const FunctionParameters &parameters) {
	auto filepath = parameters.values[0].ToString();
	PACMetadataManager::Get().LoadFromFile(filepath);
}

// Pragma function to clear all PAC metadata (in-memory and file)
static void ClearPACMetadataPragma(ClientContext &context, const FunctionParameters &parameters) {
	// Clear in-memory metadata
	PACMetadataManager::Get().Clear();

	// Try to delete the metadata file if it exists
	try {
		string filepath = PACMetadataManager::GetMetadataFilePath(context);
		if (!filepath.empty()) {
			// Check if file exists
			std::ifstream file_check(filepath);
			if (file_check.good()) {
				file_check.close();
				// File exists, try to delete it
				if (std::remove(filepath.c_str()) != 0) {
					throw IOException("Failed to delete PAC metadata file: " + filepath);
				}
			}
			// If file doesn't exist, do nothing (no error)
		}
	} catch (const IOException &e) {
		// Re-throw IO exceptions (file deletion failures)
		throw;
	} catch (...) {
		// Ignore other exceptions (e.g., can't determine file path for in-memory DB)
		// This is fine - just clear in-memory metadata
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	// Try to automatically load PAC metadata from database directory if it exists
	auto &db = loader.GetDatabaseInstance();
	try {
		// Clear any existing metadata first (in case extension is reloaded)
		PACMetadataManager::Get().Clear();

		// Get all attached database paths and try to load metadata from the first one's directory
		auto paths = db.GetDatabaseManager().GetAttachedDatabasePaths();
		if (!paths.empty()) {
			const string &db_path = paths[0];

			// Extract database name from path (filename without extension)
			// Or use "memory" as default for in-memory databases
			string db_name = "memory";
			size_t last_slash = db_path.find_last_of("/\\");
			if (last_slash != string::npos && last_slash + 1 < db_path.length()) {
				string filename = db_path.substr(last_slash + 1);
				size_t dot_pos = filename.find_last_of('.');
				if (dot_pos != string::npos) {
					db_name = filename.substr(0, dot_pos);
				} else {
					db_name = filename;
				}
			} else if (last_slash == string::npos && !db_path.empty()) {
				// No slash, so path is just the filename
				size_t dot_pos = db_path.find_last_of('.');
				if (dot_pos != string::npos) {
					db_name = db_path.substr(0, dot_pos);
				} else {
					db_name = db_path;
				}
			}

			// Default schema is "main"
			string schema_name = DEFAULT_SCHEMA;

			// Build metadata path with db and schema names
			string metadata_path;
			string filename = "pac_metadata_" + db_name + "_" + schema_name + ".json";

			if (last_slash != string::npos) {
				metadata_path = db_path.substr(0, last_slash + 1) + filename;
			} else {
				metadata_path = filename;
			}

#if PAC_DEBUG
			std::cerr << "[PAC DEBUG] LoadInternal: Checking for metadata at: " << metadata_path << "\n";
#endif

			// Try to load the metadata file if it exists
			std::ifstream test_file(metadata_path);
			if (test_file.good()) {
				test_file.close();
				PACMetadataManager::Get().LoadFromFile(metadata_path);
#if PAC_DEBUG
				std::cerr << "[PAC DEBUG] LoadInternal: Successfully loaded metadata from " << metadata_path << "\n";
				std::cerr << "[PAC DEBUG] LoadInternal: Loaded " << PACMetadataManager::Get().GetAllTableNames().size()
				          << " tables"
				          << "\n";
#endif
			} else {
#if PAC_DEBUG
				std::cerr << "[PAC DEBUG] LoadInternal: Metadata file not found at " << metadata_path << "\n";
#endif
			}
		} else {
#if PAC_DEBUG
			std::cerr << "[PAC DEBUG] LoadInternal: No database paths available (in-memory DB?)"
			          << "\n";
#endif
		}
	} catch (const std::exception &e) {
#if PAC_DEBUG
		std::cerr << "[PAC DEBUG] LoadInternal: Failed to load metadata: " << e.what() << "\n";
#endif
	} catch (...) {
#if PAC_DEBUG
		std::cerr << "[PAC DEBUG] LoadInternal: Failed to load metadata (unknown exception)"
		          << "\n";
#endif
	}

	// Register PAC optimizer rule
	auto pac_rewrite_rule = PACRewriteRule();
	// attach PAC-specific optimizer info so the extension can coordinate replan state
	auto pac_info = make_shared_ptr<PACOptimizerInfo>();
	pac_rewrite_rule.optimizer_info = pac_info;
	OptimizerExtension::Register(db.config, std::move(pac_rewrite_rule));

	// Register PAC DROP TABLE cleanup rule (separate rule to handle DROP TABLE operations)
	auto pac_drop_table_rule = PACDropTableRule();
	OptimizerExtension::Register(db.config, std::move(pac_drop_table_rule));

	// Register PAC Top-K pushdown rule (post-optimizer: rewrites TopN over PAC aggregates)
	auto pac_topk_rule = PACTopKRule();
	pac_topk_rule.optimizer_info = pac_info;
	OptimizerExtension::Register(db.config, std::move(pac_topk_rule));

	// Register pac_avg rewrite rule (post-optimizer: decomposes pac_noised_avg/pac_avg into sum/count + division).
	// This post-optimizer handles user-written pac_avg() in SQL. Compiler-generated pac_noised_avg is already
	// decomposed during the pre-optimizer phase (in CompilePacBitsliceQuery) so this is a no-op for those.
	{
		OptimizerExtension pac_avg_rule;
		pac_avg_rule.optimize_function = RewritePacAvgToDiv;
		OptimizerExtension::Register(db.config, std::move(pac_avg_rule));
	}

	// Add option to enable/disable PAC noise application (this is useful for testing, since noise affects result
	// determinism)
	// ---- User-facing settings ----
	// Controls probabilistic vs deterministic mode for noise/NULL decisions
	db.config.AddExtensionOption(
	    "pac_mi",
	    "Mutual information bound controlling privacy-utility tradeoff (default: 1/128). "
	    "Lower values = more noise = more privacy. Set to 0 for deterministic (no noise) mode.",
	    LogicalType::DOUBLE, Value::DOUBLE(1.0 / 128));
	// Set deterministic RNG seed for PAC functions (useful for tests)
	db.config.AddExtensionOption("pac_seed", "RNG seed for reproducible noised results", LogicalType::BIGINT);
	// Enable/disable PAC noise application (useful for testing, since noise affects result determinism)
	db.config.AddExtensionOption("pac_noise", "Enable/disable PAC noise application (set to false for debugging)",
	                             LogicalType::BOOLEAN);
	// Correction factor: multiplies sum/avg/count results; reduces NULL probability for all aggregates
	db.config.AddExtensionOption("pac_correction", "Correction factor multiplied into aggregate results (default: 1.0)",
	                             LogicalType::DOUBLE, Value::DOUBLE(1.0));
	// Utility diff mode: number of key columns for matching + optional output path
	db.config.AddExtensionOption(
	    "pac_diffcols", "Measure utility: specify number of key columns and optional output path (e.g. '2:out.csv')",
	    LogicalType::VARCHAR);
	// Utility threshold: probabilistically NULL cells where |noised_value| / noise_std_dev < threshold.
	// Default z-score of 4 corresponds to ~20% expected relative error for values far from zero.
	// Uses a sigmoid P(keep) = 1/(1+exp(-3*(z - threshold))) for smooth probabilistic NULLing.
	// This is safe post-processing of the already-noised output (data processing inequality).
	db.config.AddExtensionOption("pac_utility_threshold",
	                             "Z-score threshold for utility NULLing: NULL cells with |value|/noise_std < threshold "
	                             "(default: 4, set to NULL to disable)",
	                             LogicalType::DOUBLE, Value::DOUBLE(4.0));

	// ---- Internal settings ----
	// Enforce protected column access restrictions (prevents direct projection of protected columns)
	db.config.AddExtensionOption("pac_check", "[INTERNAL] Enforce protected column access restrictions",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	// Enable PAC query plan rewriting (the core optimizer that injects noise)
	db.config.AddExtensionOption("pac_rewrite", "[INTERNAL] Enable PAC query plan rewriting", LogicalType::BOOLEAN,
	                             Value::BOOLEAN(true));
	// Number of per-sample subsets (m) used by PAC (default 128)
	db.config.AddExtensionOption("pac_m", "[INTERNAL] Number of per-sample subsets", LogicalType::INTEGER);
	// Toggle enforcement of per-sample array length == pac_m
	db.config.AddExtensionOption("enforce_m_values", "[INTERNAL] Enforce per-sample array length equals pac_m",
	                             LogicalType::BOOLEAN);
	// Path where compiled PAC artifacts (CTEs) are written
	db.config.AddExtensionOption("pac_compiled_path", "[INTERNAL] Path to write compiled PAC artifacts",
	                             LogicalType::VARCHAR);
	// Enable/disable join elimination (stop FK chain before reaching PU)
	db.config.AddExtensionOption("pac_join_elimination", "[INTERNAL] Eliminate final join to PU table",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	// When false, unsupported operators skip PAC compilation instead of throwing
	db.config.AddExtensionOption("pac_conservative_mode",
	                             "[INTERNAL] Throw errors for unsupported operators (when false, skip PAC compilation)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	// Enable categorical query rewrites (comparisons against PAC aggregates)
	db.config.AddExtensionOption("pac_categorical", "[INTERNAL] Enable categorical query rewrites",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	// Use pac_select for categorical filters below pac aggregates
	db.config.AddExtensionOption("pac_select", "[INTERNAL] Use pac_select for categorical filters below pac aggregates",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	// Top-k pushdown: when true, top-k is applied on true aggregates before noising
	db.config.AddExtensionOption("pac_pushdown_topk", "[INTERNAL] Apply top-k before noise instead of after",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	// Expansion factor for top-k superset approach: inner TopN selects ceil(c*K) candidates,
	// final TopN limits to K. With c=1 (default), no expansion. With c>1, more candidates
	// are considered before noising and re-ranking, improving utility.
	db.config.AddExtensionOption("pac_topk_expansion",
	                             "[INTERNAL] Expansion factor for top-k superset (1 = no expansion)",
	                             LogicalType::DOUBLE, Value::DOUBLE(1.0));
	// Control whether pac_hash() repairs hashes to exactly 32 bits set
	db.config.AddExtensionOption("pac_hash_repair", "[INTERNAL] pac_hash() repairs hash to exactly 32 bits set",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));
	// Enable/disable persistent secret p-tracking for correct query-level privacy composition.
	// When enabled (default), noise calibration tracks a Bayesian posterior over worlds across all cells
	// in a query, providing query-level MIA protection. When disabled, each cell uses uniform variance
	// independently (cell-level MIA only). Only active when pac_mi > 0.
	db.config.AddExtensionOption("pac_ptracking", "[INTERNAL] Enable persistent secret p-tracking for query-level MIA",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));

	db.config.AddExtensionOption("pac_clip_support",
	                             "Dynamic outlier clipping threshold for pac_clip_sum. "
	                             "Levels with fewer than this many estimated distinct contributors are zeroed out. "
	                             "NULL (default) disables pac_clip_sum; set to e.g. 64 to enable.",
	                             LogicalType::BIGINT, Value());

	db.config.AddExtensionOption("pac_clip_scale",
	                             "Scale unsupported outlier levels to nearest supported level instead of omitting. "
	                             "Default false (omit).",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Register pac_sum aggregate functions
	RegisterPacSumFunctions(loader);
	RegisterPacSumCountersFunctions(loader);
	RegisterPacClipSumFunctions(loader);
	RegisterPacNoisedClipSumFunctions(loader);
	RegisterPacNoisedClipSumCountFunctions(loader);
	RegisterPacCountFunctions(loader);
	RegisterPacCountCountersFunctions(loader);
	RegisterPacClipCountFunctions(loader);
	RegisterPacNoisedClipCountFunctions(loader);
	// Register pac_min/pac_max aggregate functions
	RegisterPacMinFunctions(loader);
	RegisterPacMaxFunctions(loader);
	// Register _counters variants for categorical queries
	RegisterPacMinCountersFunctions(loader);
	RegisterPacMaxCountersFunctions(loader);
	// Register clip synonyms for min/max
	RegisterPacClipMinFunctions(loader);
	RegisterPacClipMaxFunctions(loader);
	RegisterPacNoisedClipMinFunctions(loader);
	RegisterPacNoisedClipMaxFunctions(loader);

	// Register dummy pac_noised_avg / pac_avg (replaced by RewritePacAvgToDiv before execution)
	RegisterPacAvgFunctions(loader);

	// Register PAC categorical functions (pac_select, pac_filter, pac_filter_<cmp>, etc.)
	RegisterPacCategoricalFunctions(loader);

	// Register pac_mean scalar function (used by top-k pushdown for ordering)
	RegisterPacMeanFunction(loader);

	// Register pac_hash scalar function (UBIGINT -> UBIGINT with exactly 32 bits set)
	RegisterPacHashFunction(loader);

	// Register PAC parser extension
	ParserExtension::Register(db.config, PACParserExtension());

	// Register PAC metadata management pragmas
	auto save_pac_metadata_pragma =
	    PragmaFunction::PragmaCall("save_pac_metadata", SavePACMetadataPragma, {LogicalType::VARCHAR});
	loader.RegisterFunction(save_pac_metadata_pragma);

	auto load_pac_metadata_pragma =
	    PragmaFunction::PragmaCall("load_pac_metadata", LoadPACMetadataPragma, {LogicalType::VARCHAR});
	loader.RegisterFunction(load_pac_metadata_pragma);

	auto clear_pac_metadata_pragma = PragmaFunction::PragmaCall("clear_pac_metadata", ClearPACMetadataPragma, {});
	loader.RegisterFunction(clear_pac_metadata_pragma);
}

void PacExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
string PacExtension::Name() {
	return "pac";
}

string PacExtension::Version() const {
#ifdef EXT_VERSION_PAC
	return EXT_VERSION_PAC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(pac, loader) {
	duckdb::LoadInternal(loader);
}
}
