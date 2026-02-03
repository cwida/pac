#ifndef DUCKDB_OPENPAC_REWRITE_RULE_HPP
#define DUCKDB_OPENPAC_REWRITE_RULE_HPP

#include "duckdb.hpp"
#include <atomic>
#include <mutex>

namespace duckdb {

// PAC-specific optimizer info used to prevent re-entrant replanning from the extension
// and to manage optimizer disabling for PAC compilation
struct PACOptimizerInfo : public OptimizerExtensionInfo {
	std::atomic<bool> replan_in_progress {false};

	// Track which optimizers we disabled in pre-optimize so we can restore/run them in post-optimize
	bool disabled_column_lifetime {false};
	bool disabled_compressed_materialization {false};
	std::mutex optimizer_mutex; // Protect access to optimizer state

	PACOptimizerInfo() = default;
	~PACOptimizerInfo() override = default;
};

class PACRewriteRule : public OptimizerExtension {
public:
	PACRewriteRule() {
		// Use pre_optimize_function to disable COLUMN_LIFETIME and COMPRESSED_MATERIALIZATION
		// BEFORE built-in optimizers run. This gives us a clean plan for PAC transformation.
		pre_optimize_function = PACPreOptimizeFunction;

		// Use optimize_function to run AFTER all built-in optimizers.
		// If PAC compilation is needed, we transform the plan then run the disabled optimizers.
		// If PAC compilation is NOT needed, we just run the disabled optimizers to restore normal behavior.
		optimize_function = PACRewriteRuleFunction;
	}

	static unique_ptr<LogicalOperator> ModifyPlan(unique_ptr<LogicalOperator> &plan);

	// Pre-optimizer: disables COLUMN_LIFETIME and COMPRESSED_MATERIALIZATION
	static void PACPreOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

	// Post-optimizer: performs PAC rewriting if needed, then runs the disabled optimizers
	static void PACRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

	// Checks if the query plan is PAC compatible according to the rules.
	// Throws a ParserException with an explanatory message when the plan is not compatible.
};

// Separate optimizer rule to handle DROP TABLE operations and clean up PAC metadata
class PACDropTableRule : public OptimizerExtension {
public:
	PACDropTableRule() {
		optimize_function = PACDropTableRuleFunction;
	}

	static void PACDropTableRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

#endif // DUCKDB_OPENPAC_REWRITE_RULE_HPP
