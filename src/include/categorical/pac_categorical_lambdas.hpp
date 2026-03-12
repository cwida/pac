//
// PAC Categorical Lambda Construction
//
// Expression builder layer for categorical query rewriting.
// Builds list_transform / list_zip lambda expressions over PAC counter bindings,
// and algebraic simplification to pac_filter_<cmp> / pac_select_<cmp>.
//
// Created by ila on 3/13/26.
//

#ifndef PAC_CATEGORICAL_LAMBDAS_HPP
#define PAC_CATEGORICAL_LAMBDAS_HPP

#include "categorical/pac_categorical_rewriter.hpp"

namespace duckdb {

// Build list_transform (+ optional list_zip) over PAC counter bindings.
// For single binding: list_transform(counters, elem -> body(elem))
// For multiple bindings: list_transform(list_zip(c1, c2, ...), elem -> body(elem.a, elem.b, ...))
// Returns the list_transform expression, or nullptr on failure.
// The caller wraps with pac_noised or pac_filter as needed.
unique_ptr<Expression> BuildCategoricalLambdas(OptimizerExtensionInput &input,
                                               const vector<PacBindingInfo> &pac_bindings,
                                               Expression *expr_to_transform,
                                               const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                                               const LogicalType &result_element_type);

// Try algebraic simplification to pac_filter_<cmp> / pac_select_<cmp>.
// Given a comparison expression with one PAC binding, try to algebraically
// move arithmetic from the list side to the scalar side, then emit a single
// pac_filter_<cmp>(scalar, counters) call instead of list_transform + pac_filter.
// Returns nullptr if the expression is not a comparison or doesn't have exactly one PAC binding.
unique_ptr<Expression> TryRewriteFilterComparison(OptimizerExtensionInput &input,
                                                  const vector<PacBindingInfo> &pac_bindings, Expression *expr,
                                                  const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                                                  PacWrapKind wrap_kind = PacWrapKind::PAC_FILTER,
                                                  ColumnBinding pac_hash = ColumnBinding());

} // namespace duckdb

#endif // PAC_CATEGORICAL_LAMBDAS_HPP
