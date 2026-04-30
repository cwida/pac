//
// PAC Metadata Serialization
//
// This file provides the implementation of JSON serialization and deserialization
// for PAC metadata. The actual function declarations are in privacy_metadata_manager.hpp
// as methods of the PrivacyMetadataManager class.
//
// Functions implemented in privacy_metadata_serialization.cpp:
// - PrivacyMetadataManager::SerializeToJSON
// - PrivacyMetadataManager::SerializeAllToJSON
// - PrivacyMetadataManager::DeserializeFromJSON
// - PrivacyMetadataManager::DeserializeAllFromJSON
// - PrivacyMetadataManager::SaveToFile
// - PrivacyMetadataManager::LoadFromFile
//
// Created by refactoring privacy_parser.cpp on 1/22/26.
//

#ifndef PAC_METADATA_SERIALIZATION_HPP
#define PAC_METADATA_SERIALIZATION_HPP

#include "privacy_metadata_manager.hpp"

namespace duckdb {

// All serialization functions are declared as methods of PrivacyMetadataManager
// in privacy_metadata_manager.hpp. This header exists for organizational purposes
// and to allow privacy_metadata_serialization.cpp to be compiled as a separate
// translation unit.

} // namespace duckdb

#endif // PAC_METADATA_SERIALIZATION_HPP
