#ifndef DUCKDB_OPENPAC_REWRITE_RULE_HPP
#define DUCKDB_OPENPAC_REWRITE_RULE_HPP

#include "duckdb.hpp"

class PACRewriteRule : public OptimizerExtension {
public:
	PACRewriteRule() {
		optimize_function = PACRewriteRuleFunction;
	}

	static unique_ptr<LogicalOperator> ModifyPlan(unique_ptr<LogicalOperator> &plan);

	static void PACRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

#endif // DUCKDB_OPENPAC_REWRITE_RULE_HPP