/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/SchemaManager.h"
#include "meta/FileBasedSchemaManager.h"
#include "meta/ServerBasedSchemaManager.h"
#include "meta/AdHocSchemaManager.h"

DECLARE_string(schema_file);
DECLARE_string(meta_server);

namespace nebula {
namespace meta {

folly::RWSpinLock SchemaManager::tagLock_;
std::unordered_map<
    std::pair<GraphSpaceID, TagID>,
    std::map<int32_t, std::shared_ptr<const SchemaProviderIf>>> SchemaManager::tagSchemas_;

folly::RWSpinLock SchemaManager::edgeLock_;
std::unordered_map<
    std::pair<GraphSpaceID, EdgeType>,
    std::map<int32_t, std::shared_ptr<const SchemaProviderIf>>> SchemaManager::edgeSchemas_;

std::once_flag schemaInitFlag;


// static
void SchemaManager::init() {
    std::call_once(schemaInitFlag, []() {
        if (!FLAGS_schema_file.empty()) {
            return FileBasedSchemaManager::init();
        } else if (!FLAGS_meta_server.empty()) {
            LOG(FATAL) << "ServerBasedSchemaManager has not been implemented";
        } else {
            // Memory based SchemaManager
            return AdHocSchemaManager::init();
        }
    });
}


// static
std::shared_ptr<const SchemaProviderIf> SchemaManager::getTagSchema(
        const folly::StringPiece spaceName,
        const folly::StringPiece tagName,
        int32_t ver) {
    init();
    return getTagSchema(toGraphSpaceID(spaceName), toTagID(tagName), ver);
}


// static
std::shared_ptr<const SchemaProviderIf> SchemaManager::getTagSchema(
        GraphSpaceID space, TagID tag, int32_t ver) {
    init();

    folly::RWSpinLock::ReadHolder rh(tagLock_);

    auto it = tagSchemas_.find(std::make_pair(space, tag));
    if (it == tagSchemas_.end()) {
        // Not found
        return std::shared_ptr<const SchemaProviderIf>();
    } else {
        // Now check the version
        if (ver < 0) {
            // Get the latest version
            if (it->second.empty()) {
                // No schema
                return std::shared_ptr<const SchemaProviderIf>();
            } else {
                return it->second.rbegin()->second;
            }
        } else {
            // Looking for the specified version
            auto verIt = it->second.find(ver);
            if (verIt == it->second.end()) {
                // Not found
                return std::shared_ptr<const SchemaProviderIf>();
            } else {
                return verIt->second;
            }
        }
    }
}


// static
int32_t SchemaManager::getNewestTagSchemaVer(const folly::StringPiece spaceName,
                                             const folly::StringPiece tagName) {
    init();
    return getNewestTagSchemaVer(toGraphSpaceID(spaceName), toTagID(tagName));
}


// static
int32_t SchemaManager::getNewestTagSchemaVer(GraphSpaceID space, TagID tag) {
    init();

    folly::RWSpinLock::ReadHolder rh(tagLock_);

    auto it = tagSchemas_.find(std::make_pair(space, tag));
    if (it == tagSchemas_.end() || it->second.empty()) {
        // Not found
        return -1;
    } else {
        // Now get the latest version
        return it->second.rbegin()->first;
    }
}


// static
std::shared_ptr<const SchemaProviderIf> SchemaManager::getEdgeSchema(
        const folly::StringPiece spaceName,
        const folly::StringPiece typeName,
        int32_t ver) {
    init();
    return getEdgeSchema(toGraphSpaceID(spaceName), toEdgeType(typeName), ver);
}


// static
std::shared_ptr<const SchemaProviderIf> SchemaManager::getEdgeSchema(
        GraphSpaceID space, EdgeType edge, int32_t ver) {
    init();

    folly::RWSpinLock::ReadHolder rh(edgeLock_);

    auto it = edgeSchemas_.find(std::make_pair(space, edge));
    if (it == edgeSchemas_.end()) {
        // Not found
        return std::shared_ptr<const SchemaProviderIf>();
    } else {
        // Now check the version
        if (ver < 0) {
            // Get the latest version
            if (it->second.empty()) {
                // No schema
                return std::shared_ptr<const SchemaProviderIf>();
            } else {
                return it->second.rbegin()->second;
            }
        } else {
            // Looking for the specified version
            auto verIt = it->second.find(ver);
            if (verIt == it->second.end()) {
                // Not found
                return std::shared_ptr<const SchemaProviderIf>();
            } else {
                return verIt->second;
            }
        }
    }
}


// static
int32_t SchemaManager::getNewestEdgeSchemaVer(const folly::StringPiece spaceName,
                                              const folly::StringPiece typeName) {
    init();
    return getNewestEdgeSchemaVer(toGraphSpaceID(spaceName), toEdgeType(typeName));
}


// static
int32_t SchemaManager::getNewestEdgeSchemaVer(GraphSpaceID space, EdgeType edge) {
    init();

    folly::RWSpinLock::ReadHolder rh(edgeLock_);

    auto it = edgeSchemas_.find(std::make_pair(space, edge));
    if (it == edgeSchemas_.end() || it->second.empty()) {
        // Not found
        return -1;
    } else {
        // Now get the latest version
        return it->second.rbegin()->first;
    }
}


// static
GraphSpaceID SchemaManager::toGraphSpaceID(const folly::StringPiece spaceName) {
    if (!FLAGS_schema_file.empty()) {
        return FileBasedSchemaManager::toGraphSpaceID(spaceName);
    } else if (!FLAGS_meta_server.empty()) {
        return ServerBasedSchemaManager::toGraphSpaceID(spaceName);
    } else {
        // Memory based
        return AdHocSchemaManager::toGraphSpaceID(spaceName);
    }
}


// static
TagID SchemaManager::toTagID(const folly::StringPiece tagName) {
    if (!FLAGS_schema_file.empty()) {
        return FileBasedSchemaManager::toTagID(tagName);
    } else if (!FLAGS_meta_server.empty()) {
        return ServerBasedSchemaManager::toTagID(tagName);
    } else {
        // Memory based
        return AdHocSchemaManager::toTagID(tagName);
    }
}


// static
EdgeType SchemaManager::toEdgeType(const folly::StringPiece typeName) {
    if (!FLAGS_schema_file.empty()) {
        return FileBasedSchemaManager::toEdgeType(typeName);
    } else if (!FLAGS_meta_server.empty()) {
        return ServerBasedSchemaManager::toEdgeType(typeName);
    } else {
        // Memory based
        return AdHocSchemaManager::toEdgeType(typeName);
    }
}


int32_t SchemaManager::getVersion() const noexcept {
    return ver_;
}


size_t SchemaManager::getNumFields() const noexcept {
    return fields_.size();
}


int64_t SchemaManager::getFieldIndex(const folly::StringPiece name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (it == fieldNameIndex_.end()) {
        // Not found
        return -1;
    } else {
        return it->second;
    }
}


const char* SchemaManager::getFieldName(int64_t index) const {
    CHECK_GE(index, 0) << "Invalid index " << index;
    CHECK_LT(index, fields_.size()) << "Index is out of range";

    return fields_[index]->getName();
}


const cpp2::ValueType& SchemaManager::getFieldType(int64_t index) const {
    CHECK_GE(index, 0) << "Invalid index " << index;
    CHECK_LT(index, fields_.size()) << "Index is out of range";

    return fields_[index]->getType();
}


const cpp2::ValueType& SchemaManager::getFieldType(const folly::StringPiece name)
        const {
    auto it = fieldNameIndex_.find(name.toString());
    CHECK(it != fieldNameIndex_.end())
        << "Unknown field \"" << name.toString() << "\"";
    return fields_[it->second]->getType();
}


std::shared_ptr<const SchemaProviderIf::Field> SchemaManager::field(
        int64_t index) const {
    if (index < 0) {
        VLOG(2) << "Invalid index " << index;
        return nullptr;
    }
    if (index >= static_cast<int64_t>(fields_.size())) {
        VLOG(2) << "Index " << index << " is out of range";
        return nullptr;
    }

    return fields_[index];
}


std::shared_ptr<const SchemaProviderIf::Field> SchemaManager::field(
        const folly::StringPiece name) const {
    auto it = fieldNameIndex_.find(name.toString());
    if (it == fieldNameIndex_.end()) {
        VLOG(2) << "Unknown field \"" << name.toString() << "\"";
        return nullptr;
    }

    return fields_[it->second];
}

}  // namespace meta
}  // namespace nebula

