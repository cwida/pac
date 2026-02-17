//
// Created by ila on 1/16/26.
//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class LogicalGet;
class LogicalAggregate;
struct OptimizerExtensionInput;

/**
 * PropagateSingleBinding: Propagates a single column binding through intermediate operators
 * from a source operator to a target aggregate.
 *
 * This is the core propagation function. It traces the path from the target aggregate
 * down to the source operator (identified by source_table_index), and ensures the column
 * identified by source_binding passes through all intermediate operators (projections,
 * joins, filters) by adding it where needed.
 *
 * Returns the final ColumnBinding as seen from the aggregate's direct child.
 * Returns {INVALID_INDEX, INVALID_INDEX} if propagation fails (no path, blocked by semi/anti join).
 */
ColumnBinding PropagateSingleBinding(LogicalOperator &plan_root, idx_t source_table_index, ColumnBinding source_binding,
                                     const LogicalType &source_type, LogicalAggregate *target_agg);

/**
 * PropagatePKThroughProjections: Backward-compatible wrapper that propagates a complex hash
 * expression (possibly with multiple column bindings) through intermediate operators.
 *
 * Delegates to PropagateSingleBinding for each binding in the expression, then updates
 * the expression's column references with the new bindings.
 *
 * Returns nullptr if propagation fails for any binding.
 */
unique_ptr<Expression> PropagatePKThroughProjections(LogicalOperator &plan, LogicalGet &pu_get,
                                                     unique_ptr<Expression> hash_expr, LogicalAggregate *target_agg);

} // namespace duckdb
