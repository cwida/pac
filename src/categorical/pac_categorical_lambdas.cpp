//
// PAC Categorical Lambda Construction - Implementation
//
// See pac_categorical_lambdas.hpp for design documentation.
//
// Created by ila on 3/13/26.
//
#include "categorical/pac_categorical_lambdas.hpp"
#include "aggregates/pac_aggregate.hpp"
#include "utils/privacy_helpers.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

// Clone an expression tree for use as a lambda body (unified single/multi binding version).
// PAC aggregate column refs are replaced with lambda element references.
// Other column refs are captured and become BoundReferenceExpression(1+i).
//
// pac_binding_map: maps binding hash -> struct field index (single: one entry mapping to 0)
// struct_type: nullptr = single binding (elem ref), non-null = multi binding (struct field extract)
static unique_ptr<Expression>
CloneForLambdaBody(Expression *expr, const unordered_map<uint64_t, idx_t> &pac_binding_map,
                   vector<unique_ptr<Expression>> &captures, unordered_map<uint64_t, idx_t> &capture_map,
                   const unordered_map<uint64_t, ColumnBinding> &counter_bindings, const LogicalType *struct_type) {
	// Helper to build the replacement expression for a matched PAC binding
	auto make_pac_replacement = [&](const unordered_map<uint64_t, idx_t>::const_iterator &it,
	                                const string &alias) -> unique_ptr<Expression> {
		if (struct_type) {
			idx_t field_idx = it->second;
			string field_name = GetStructFieldName(field_idx);
			auto elem_ref = make_uniq<BoundReferenceExpression>("elem", *struct_type, idx_t(0));
			auto child_types = StructType::GetChildTypes(*struct_type);
			LogicalType extract_return_type = PacFloatLogicalType();
			for (idx_t j = 0; j < child_types.size(); j++) {
				if (child_types[j].first == field_name) {
					extract_return_type = child_types[j].second;
					break;
				}
			}
			auto extract_func = StructExtractAtFun::GetFunction();
			auto bind_data = StructExtractAtFun::GetBindData(field_idx);
			vector<unique_ptr<Expression>> extract_children;
			extract_children.push_back(std::move(elem_ref));
			extract_children.push_back(
			    make_uniq<BoundConstantExpression>(Value::BIGINT(static_cast<int64_t>(field_idx + 1))));
			return make_uniq<BoundFunctionExpression>(extract_return_type, extract_func, std::move(extract_children),
			                                          std::move(bind_data));
		} else {
			return make_uniq<BoundReferenceExpression>(alias, PacFloatLogicalType(), idx_t(0));
		}
	};
	// rewrite all kinds of expressions:
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		uint64_t binding_hash = HashBinding(col_ref.binding);
		// Direct match
		auto it = pac_binding_map.find(binding_hash);
		if (it != pac_binding_map.end()) {
			return make_pac_replacement(it, "elem");
		}
		// Trace through counter_bindings to find the aggregate-level PAC binding
		auto cb_it = counter_bindings.find(HashBinding(col_ref.binding));
		if (cb_it != counter_bindings.end()) {
			auto traced_it = pac_binding_map.find(HashBinding(cb_it->second));
			if (traced_it != pac_binding_map.end()) {
				return make_pac_replacement(traced_it, col_ref.alias);
			}
		}
		// Other column ref — capture for use in lambda
		uint64_t hash = HashBinding(col_ref.binding);
		idx_t capture_idx;
		auto cap_it = capture_map.find(hash);
		if (cap_it != capture_map.end()) {
			capture_idx = cap_it->second;
		} else {
			capture_idx = captures.size();
			capture_map[hash] = capture_idx;
			captures.push_back(col_ref.Copy());
		}
		return make_uniq<BoundReferenceExpression>(col_ref.alias, col_ref.return_type, 1 + capture_idx);
	} else if (expr->type == ExpressionType::VALUE_CONSTANT) {
		return expr->Copy();
	} else if (expr->type == ExpressionType::OPERATOR_CAST) {
		auto &cast = expr->Cast<BoundCastExpression>();
		auto child_clone =
		    CloneForLambdaBody(cast.child.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type);
		if (child_clone->return_type == cast.return_type) {
			return child_clone; // If the child's type already matches the target type, skip the cast
		}
		// Otherwise, create a new cast with the correct function for the new child type
		return BoundCastExpression::AddDefaultCastToType(std::move(child_clone), cast.return_type);
	} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comp = expr->Cast<BoundComparisonExpression>();
		auto left_clone =
		    CloneForLambdaBody(comp.left.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type);
		auto right_clone =
		    CloneForLambdaBody(comp.right.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type);
		// Reconcile types if they differ (needed for multi where struct fields are DOUBLE
		// but original CASTs may introduce DECIMAL)
		if (left_clone->return_type != right_clone->return_type) {
			if (left_clone->return_type != PacFloatLogicalType()) {
				left_clone = BoundCastExpression::AddDefaultCastToType(std::move(left_clone), PacFloatLogicalType());
			}
			if (right_clone->return_type != PacFloatLogicalType()) {
				right_clone = BoundCastExpression::AddDefaultCastToType(std::move(right_clone), PacFloatLogicalType());
			}
		}
		return make_uniq<BoundComparisonExpression>(expr->type, std::move(left_clone), std::move(right_clone));
	} else if (expr->type == ExpressionType::BOUND_FUNCTION) {
		auto &func = expr->Cast<BoundFunctionExpression>();
		vector<unique_ptr<Expression>> new_children;
		bool any_cast_needed = false;
		for (idx_t i = 0; i < func.children.size(); i++) {
			auto child_clone = CloneForLambdaBody(func.children[i].get(), pac_binding_map, captures, capture_map,
			                                      counter_bindings, struct_type);
			// If a child's type changed (e.g., DECIMAL->DOUBLE from PAC counter conversion),
			// cast it to the type the bound function expects, so the function binding stays valid.
			if (i < func.function.arguments.size() && child_clone->return_type != func.function.arguments[i]) {
				child_clone =
				    BoundCastExpression::AddDefaultCastToType(std::move(child_clone), func.function.arguments[i]);
				any_cast_needed = true;
			}
			new_children.push_back(std::move(child_clone));
		}
		unique_ptr<Expression> result =
		    make_uniq<BoundFunctionExpression>(func.return_type, func.function, std::move(new_children),
		                                       func.bind_info ? func.bind_info->Copy() : nullptr);
		// Children were cast from PAC_FLOAT to the function's bound types (e.g., DECIMAL).
		// Cast the result back to PAC_FLOAT so the list_transform output stays LIST<PAC_FLOAT>.
		if (any_cast_needed && result->return_type != PacFloatLogicalType()) {
			result = BoundCastExpression::AddDefaultCastToType(std::move(result), PacFloatLogicalType());
		}
		return result;
	} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) { // NOT, arithmetic, COALESCE, IS NULL..
		auto &op = expr->Cast<BoundOperatorExpression>();
		vector<unique_ptr<Expression>> new_children;
		for (auto &child : op.children) {
			new_children.push_back(
			    CloneForLambdaBody(child.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type));
		}
		// For COALESCE with mismatched child types, cast all to the first child's type
		LogicalType result_type = op.return_type;
		if (expr->type == ExpressionType::OPERATOR_COALESCE && new_children.size() > 1) {
			LogicalType first_type = new_children[0]->return_type;
			bool types_mismatch = false;
			for (idx_t ci = 1; ci < new_children.size(); ci++) {
				if (new_children[ci]->return_type != first_type) {
					types_mismatch = true;
					break;
				}
			}
			if (types_mismatch) {
				for (auto &c : new_children) {
					if (c->return_type != first_type) {
						c = BoundCastExpression::AddDefaultCastToType(std::move(c), first_type);
					}
				}
				result_type = first_type;
			}
		}
		auto op_result = make_uniq<BoundOperatorExpression>(expr->type, result_type);
		for (auto &c : new_children) {
			op_result->children.push_back(std::move(c));
		}
		return op_result;
	} else if (expr->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) { // AND, OR
		auto &conj = expr->Cast<BoundConjunctionExpression>();
		auto result = make_uniq<BoundConjunctionExpression>(expr->type);
		for (auto &child : conj.children) {
			result->children.push_back(
			    CloneForLambdaBody(child.get(), pac_binding_map, captures, capture_map, counter_bindings, struct_type));
		}
		return result;
	} else if (expr->type == ExpressionType::CASE_EXPR) {
		auto &case_expr = expr->Cast<BoundCaseExpression>();
		if (IsScalarSubqueryWrapper(case_expr)) {
			return CloneForLambdaBody(case_expr.else_expr.get(), pac_binding_map, captures, capture_map,
			                          counter_bindings, struct_type);
		}
		// Regular CASE - recurse into all branches
		// Start with original return type, will update based on cloned branches
		auto result = make_uniq<BoundCaseExpression>(case_expr.return_type);
		for (auto &check : case_expr.case_checks) {
			BoundCaseCheck new_check;
			new_check.when_expr = CloneForLambdaBody(check.when_expr.get(), pac_binding_map, captures, capture_map,
			                                         counter_bindings, struct_type);
			new_check.then_expr = CloneForLambdaBody(check.then_expr.get(), pac_binding_map, captures, capture_map,
			                                         counter_bindings, struct_type);
			result->case_checks.push_back(std::move(new_check));
		}
		if (case_expr.else_expr) {
			result->else_expr = CloneForLambdaBody(case_expr.else_expr.get(), pac_binding_map, captures, capture_map,
			                                       counter_bindings, struct_type);
			// Update return type to match ELSE branch (the PAC element type)
			result->return_type = result->else_expr->return_type;
		}
		// Cast THEN branches to match the return type if needed
		for (auto &check : result->case_checks) {
			if (check.then_expr && check.then_expr->return_type != result->return_type) {
				check.then_expr =
				    BoundCastExpression::AddDefaultCastToType(std::move(check.then_expr), result->return_type);
			}
		}
		return result;
	}
	return expr->Copy();
}

// Build a BoundLambdaExpression from a lambda body and captures
static unique_ptr<Expression> BuildPacLambda(unique_ptr<Expression> lambda_body,
                                             vector<unique_ptr<Expression>> captures) {
	auto lambda =
	    make_uniq<BoundLambdaExpression>(ExpressionType::LAMBDA, LogicalType::LAMBDA, std::move(lambda_body), idx_t(1));
	lambda->captures = std::move(captures);
	return lambda;
}

// Build a list_transform function call with proper binding
// element_return_type: the type each element maps to (e.g., BOOLEAN for predicates, some other type for casts)
static unique_ptr<Expression> BuildListTransformCall(OptimizerExtensionInput &input,
                                                     unique_ptr<Expression> counters_list,
                                                     unique_ptr<Expression> lambda_expr,
                                                     const LogicalType &element_return_type = LogicalType::BOOLEAN) {
	// Get the lambda body for ListLambdaBindData
	auto &bound_lambda = lambda_expr->Cast<BoundLambdaExpression>();

	// Get list_transform function from catalog
	auto &catalog = Catalog::GetSystemCatalog(input.context);
	auto &func_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(input.context, DEFAULT_SCHEMA, "list_transform");

	// Get the first function overload (list_transform has only one signature pattern)
	auto &scalar_func = func_entry.functions.functions[0];

	// Create the ListLambdaBindData with the lambda body
	auto list_return_type = LogicalType::LIST(element_return_type);
	auto bind_data = make_uniq<ListLambdaBindData>(list_return_type, std::move(bound_lambda.lambda_expr), false, false);

	// Build children: [list, captures...] Note: The lambda itself is NOT a child after binding - only its captures are
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(counters_list));

	for (auto &capture : bound_lambda.captures) {
		children.push_back(std::move(capture)); // Add captures as children
	}
	// Create the bound function expression
	return make_uniq<BoundFunctionExpression>(list_return_type, scalar_func, std::move(children), std::move(bind_data));
}

// Build a list_zip function call combining multiple counter lists
// Returns LIST<STRUCT<a T1, b T2, ...>> where each field corresponds to one PAC binding
static unique_ptr<Expression> BuildListZipCall(OptimizerExtensionInput &input,
                                               vector<unique_ptr<Expression>> counter_lists,
                                               LogicalType &out_struct_type) {
	// Build the struct type for list_zip result -- list_zip returns LIST<STRUCT<a T1, b T2, ...>>
	child_list_t<LogicalType> struct_children;
	for (idx_t i = 0; i < counter_lists.size(); i++) {
		string field_name = GetStructFieldName(i);
		struct_children.push_back(
		    make_pair(field_name, PacFloatLogicalType())); // All counter lists use PAC_FLOAT element type
	}
	out_struct_type = LogicalType::STRUCT(struct_children);
	auto list_struct_type = LogicalType::LIST(out_struct_type);

	// Get list_zip function from catalog
	auto &catalog = Catalog::GetSystemCatalog(input.context);
	auto &func_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(input.context, DEFAULT_SCHEMA, "list_zip");

	// Find the appropriate overload (list_zip is variadic)
	vector<LogicalType> arg_types;
	for (auto &list : counter_lists) {
		arg_types.push_back(list->return_type);
	}
	ErrorData error;
	FunctionBinder function_binder(input.context);
	auto best_function = function_binder.BindFunction(func_entry.name, func_entry.functions, arg_types, error);
	if (best_function.IsValid()) {
		auto scalar_func = func_entry.functions.GetFunctionByOffset(best_function.GetIndex());
		return make_uniq<BoundFunctionExpression>(list_struct_type, scalar_func, std::move(counter_lists), nullptr);
	}
	return nullptr;
}

// Check whether an expression tree contains any null-handling operators
// (COALESCE, IS NULL, IS NOT NULL). When absent, pac_coalesce is unnecessary
// because NULL propagates identically through arithmetic.
static bool ExpressionContainsNullHandling(Expression *expr) {
	if (expr->type == ExpressionType::OPERATOR_COALESCE || expr->type == ExpressionType::OPERATOR_IS_NULL ||
	    expr->type == ExpressionType::OPERATOR_IS_NOT_NULL) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(*expr, [&](Expression &child) {
		if (!found) {
			found = ExpressionContainsNullHandling(&child);
		}
	});
	return found;
}

// Check which child of a binary function contains a counter binding
// Returns 0 if first child has counter, 1 if second child has counter, -1 if neither or both
static int FindPacChildIndex(Expression *expr, const unordered_map<uint64_t, ColumnBinding> &counter_bindings) {
	if (expr->type != ExpressionType::BOUND_FUNCTION) {
		return -1;
	}
	auto &func = expr->Cast<BoundFunctionExpression>();
	if (func.children.size() != 2) {
		return -1;
	}
	bool left_has = ReferencesCounterBinding(func.children[0].get(), counter_bindings);
	bool right_has = ReferencesCounterBinding(func.children[1].get(), counter_bindings);
	if (left_has && !right_has) {
		return 0;
	}
	if (!left_has && right_has) {
		return 1;
	}
	return -1; // both or neither
}

unique_ptr<Expression> BuildCategoricalLambdas(OptimizerExtensionInput &input,
                                               const vector<PacBindingInfo> &pac_bindings,
                                               Expression *expr_to_transform,
                                               const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                                               const LogicalType &result_element_type) {
	if (pac_bindings.size() == 1) { // --- SINGLE AGGREGATE ---
		auto &binding_info = pac_bindings[0];
		ColumnBinding pac_binding = binding_info.binding;
		auto counters_ref =
		    make_uniq<BoundColumnRefExpression>("pac_var", LogicalType::LIST(PacFloatLogicalType()), pac_binding);
		unique_ptr<Expression> input_list;
		if (ExpressionContainsNullHandling(expr_to_transform)) {
			input_list = input.optimizer.BindScalarFunction("pac_coalesce", std::move(counters_ref));
		} else {
			input_list = std::move(counters_ref);
		}
		// Outer lambda: clone the expression replacing PAC binding with lambda element
		unordered_map<uint64_t, idx_t> pac_binding_map;
		pac_binding_map[HashBinding(pac_binding)] = 0;
		vector<unique_ptr<Expression>> captures;
		unordered_map<uint64_t, idx_t> capture_map;
		auto lambda_body =
		    CloneForLambdaBody(expr_to_transform, pac_binding_map, captures, capture_map, counter_bindings, nullptr);
		// Ensure body returns result_element_type if needed
		if (result_element_type == PacFloatLogicalType() && lambda_body->return_type != PacFloatLogicalType()) {
			lambda_body = BoundCastExpression::AddDefaultCastToType(std::move(lambda_body), PacFloatLogicalType());
		}
		auto lambda = BuildPacLambda(std::move(lambda_body), std::move(captures));
		return BuildListTransformCall(input, std::move(input_list), std::move(lambda), result_element_type);
	} else { // --- MULTIPLE AGGREGATES ---
		vector<unique_ptr<Expression>> counter_lists;
		unordered_map<uint64_t, idx_t> binding_to_index;
		bool needs_coalesce = ExpressionContainsNullHandling(expr_to_transform);
		for (auto &bi : pac_bindings) {
			auto ref =
			    make_uniq<BoundColumnRefExpression>("pac_var", LogicalType::LIST(PacFloatLogicalType()), bi.binding);
			if (needs_coalesce) {
				counter_lists.push_back(input.optimizer.BindScalarFunction("pac_coalesce", std::move(ref)));
			} else {
				counter_lists.push_back(std::move(ref));
			}
			binding_to_index[HashBinding(bi.binding)] = bi.index;
		}
		LogicalType struct_type;
		auto zipped_list = BuildListZipCall(input, std::move(counter_lists), struct_type);
		if (zipped_list) {
			vector<unique_ptr<Expression>> captures;
			unordered_map<uint64_t, idx_t> map;
			auto lambda_body =
			    CloneForLambdaBody(expr_to_transform, binding_to_index, captures, map, counter_bindings, &struct_type);
			if (result_element_type == PacFloatLogicalType() && lambda_body->return_type != PacFloatLogicalType()) {
				lambda_body = BoundCastExpression::AddDefaultCastToType(std::move(lambda_body), PacFloatLogicalType());
			}
			auto lambda = BuildPacLambda(std::move(lambda_body), std::move(captures));
			return BuildListTransformCall(input, std::move(zipped_list), std::move(lambda), result_element_type);
		}
	}
	return nullptr;
}

unique_ptr<Expression> TryRewriteFilterComparison(OptimizerExtensionInput &input,
                                                  const vector<PacBindingInfo> &pac_bindings, Expression *expr,
                                                  const unordered_map<uint64_t, ColumnBinding> &counter_bindings,
                                                  PacWrapKind wrap_kind, ColumnBinding pac_hash) {
	if (pac_bindings.size() != 1 || expr->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
		return nullptr; // Only works with a comparison against a single PAC binding
	}
	auto &comp = expr->Cast<BoundComparisonExpression>();
	ExpressionType cmp_type = comp.type;

	// Determine which side has the PAC binding
	auto left_bindings = FindCounterBindings(comp.left.get(), counter_bindings);
	auto right_bindings = FindCounterBindings(comp.right.get(), counter_bindings);
	bool left_has_pac = !left_bindings.empty();
	bool right_has_pac = !right_bindings.empty();

	if (left_has_pac == right_has_pac) {
		return nullptr; // both sides or neither — can't optimize
	}
	// Normalize: scalar_side CMP list_side (PAC on right)
	unique_ptr<Expression> scalar_side;
	unique_ptr<Expression> list_side;
	if (left_has_pac) { // PAC on left: flip comparison
		scalar_side = comp.right->Copy();
		list_side = comp.left->Copy();
		cmp_type = FlipComparison(cmp_type);
	} else {
		scalar_side = comp.left->Copy();
		list_side = comp.right->Copy();
	}
	// Iteratively simplify: move arithmetic from list_side to scalar_side
	while (list_side->type == ExpressionType::BOUND_FUNCTION) {
		auto &func = list_side->Cast<BoundFunctionExpression>();
		if (func.children.size() != 2) {
			break;
		}
		int pac_child = FindPacChildIndex(list_side.get(), counter_bindings);
		if (pac_child < 0) {
			break;
		}
		bool needs_positive_check = false;
		const char *inverse_op = GetInverseArithmeticOp(func.function.name, pac_child, needs_positive_check);
		if (!inverse_op) {
			break;
		}
		auto &scalar_operand = func.children[1 - pac_child];
		if (needs_positive_check && !IsPositiveConstant(scalar_operand.get())) {
			break;
		}
		// When the inverse is division, multiply by the reciprocal instead (cheaper at runtime).
		// We know scalar_operand is a positive constant, so we can compute 1/value at plan time.
		double const_val;
		if (inverse_op[0] == '/' && TryGetConstantDouble(scalar_operand.get(), const_val)) {
			auto reciprocal = make_uniq<BoundConstantExpression>(Value::DOUBLE(1.0 / const_val));
			scalar_side = input.optimizer.BindScalarFunction("*", std::move(scalar_side), std::move(reciprocal));
		} else {
			scalar_side =
			    input.optimizer.BindScalarFunction(inverse_op, std::move(scalar_side), scalar_operand->Copy());
		}
		list_side = func.children[pac_child]->Copy();
	}
	// After simplification, list_side should be a bare column ref (possibly through casts)
	Expression *stripped = StripCasts(list_side.get());
	if (stripped->type != ExpressionType::BOUND_COLUMN_REF) {
		// Can't emit pac_filter_<cmp>, but return the simplified comparison
		// so the lambda path benefits from the algebraic rewriting
		return make_uniq<BoundComparisonExpression>(cmp_type, std::move(scalar_side), std::move(list_side));
	}
	// Verify the column ref is a counter binding
	auto &col_ref = stripped->Cast<BoundColumnRefExpression>();
	if (!counter_bindings.count(HashBinding(col_ref.binding))) {
		return make_uniq<BoundComparisonExpression>(cmp_type, std::move(scalar_side), std::move(list_side));
	}
	// Build pac_{filter,select}_<cmp>(scalar_side, counters) or pac_select_<cmp>(hash, scalar, counters)
	const char *func_name =
	    (wrap_kind == PacWrapKind::PAC_SELECT) ? GetPacSelectCmpName(cmp_type) : GetPacFilterCmpName(cmp_type);
	if (!func_name) {
		return make_uniq<BoundComparisonExpression>(cmp_type, std::move(scalar_side), std::move(list_side));
	}
	if (scalar_side->return_type != PacFloatLogicalType()) {
		scalar_side = BoundCastExpression::AddDefaultCastToType(std::move(scalar_side), PacFloatLogicalType());
	}
	auto counters_ref =
	    make_uniq<BoundColumnRefExpression>("pac_var", LogicalType::LIST(PacFloatLogicalType()), col_ref.binding);

	if (wrap_kind == PacWrapKind::PAC_SELECT) {
		// pac_select_<cmp>(hash, scalar, counters) — 3 args, need manual binding
		auto hash_ref = make_uniq<BoundColumnRefExpression>("pac_pu", LogicalType::UBIGINT, pac_hash);
		vector<unique_ptr<Expression>> args;
		args.push_back(std::move(hash_ref));
		args.push_back(std::move(scalar_side));
		args.push_back(std::move(counters_ref));
		vector<LogicalType> arg_types;
		for (auto &a : args) {
			arg_types.push_back(a->return_type);
		}
		auto &catalog = Catalog::GetSystemCatalog(input.context);
		auto &entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(input.context, DEFAULT_SCHEMA, func_name);
		ErrorData error;
		FunctionBinder func_binder(input.context);
		auto best = func_binder.BindFunction(func_name, entry.functions, arg_types, error);
		if (best.IsValid()) {
			auto func = entry.functions.GetFunctionByOffset(best.GetIndex());
			auto bind_info = func.bind ? func.bind(input.context, func, args) : nullptr;
			return make_uniq<BoundFunctionExpression>(func.return_type, func, std::move(args), std::move(bind_info));
		}
		return nullptr;
	}
	return input.optimizer.BindScalarFunction(func_name, std::move(scalar_side), std::move(counters_ref));
}

} // namespace duckdb
