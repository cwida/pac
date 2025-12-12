#define DUCKDB_EXTENSION_MAIN

#include "pac_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void PacScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Pac " + name.GetString() + " 🐥");
	});
}

inline void PacOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Pac " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {

	auto &db_config = duckdb::DBConfig::GetConfig(loader);

	// Register a scalar function
	auto pac_scalar_function = ScalarFunction("pac", {LogicalType::VARCHAR}, LogicalType::VARCHAR, PacScalarFun);
	loader.RegisterFunction(pac_scalar_function);

	auto pac_rewrite_rule = duckdb::PACRewriteRule();
	db_config.optimizer_extensions.push_back(pac_rewrite_rule);

	// Register another scalar function
	auto pac_openssl_version_scalar_function = ScalarFunction("pac_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, PacOpenSSLVersionScalarFun);
	loader.RegisterFunction(pac_openssl_version_scalar_function);
}

void PacExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string PacExtension::Name() {
	return "pac";
}

std::string PacExtension::Version() const {
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
