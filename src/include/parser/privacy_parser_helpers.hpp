//
// PAC Parser Helpers
//
// This file provides the implementation of PAC-specific SQL parsing helpers.
// The actual function declarations are in privacy_parser.hpp as static methods
// of the PrivacyParserExtension class.
//
// Functions implemented in privacy_parser_helpers.cpp:
// - PrivacyParserExtension::ExtractTableName
// - PrivacyParserExtension::ExtractPACPrimaryKey
// - PrivacyParserExtension::ExtractPACLink
// - PrivacyParserExtension::ExtractProtectedColumns
// - PrivacyParserExtension::StripPACClauses
// - PrivacyParserExtension::ParseCreatePACTable
// - PrivacyParserExtension::ParseAlterTableAddPAC
// - PrivacyParserExtension::ParseAlterTableDropPAC
//
// Created by refactoring privacy_parser.cpp on 1/22/26.
//

#ifndef PAC_PARSER_HELPERS_HPP
#define PAC_PARSER_HELPERS_HPP

#include "privacy_parser.hpp"

namespace duckdb {

// All parser helper functions are declared as static methods of PrivacyParserExtension
// in privacy_parser.hpp. This header exists for organizational purposes and to allow
// privacy_parser_helpers.cpp to be compiled as a separate translation unit.

} // namespace duckdb

#endif // PAC_PARSER_HELPERS_HPP
