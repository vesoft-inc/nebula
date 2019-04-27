/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/SchemaManager.h"
#include "meta/FileBasedSchemaManager.h"

DECLARE_string(schema_file);
DECLARE_string(meta_server);

namespace nebula {
namespace meta {

std::unique_ptr<SchemaManager> SchemaManager::create() {
    if (!FLAGS_schema_file.empty()) {
        std::unique_ptr<SchemaManager> sm(new FileBasedSchemaManager());
        return sm;
    } else {
        std::unique_ptr<SchemaManager> sm(new AdHocSchemaManager());
        return sm;
    }
}

void AdHocSchemaManager::addTagSchema(GraphSpaceID space,
                                      TagID tag,
                                      std::shared_ptr<SchemaProviderIf> schema
                                      ) {
    folly::RWSpinLock::WriteHolder wh(tagLock_);
    // Only version 0
    tagSchemas_[std::make_pair(space, tag)][0] = schema;
}

void AdHocSchemaManager::addEdgeSchema(GraphSpaceID space,
                                       EdgeType edge,
                                       std::shared_ptr<SchemaProviderIf> schema) {
    folly::RWSpinLock::WriteHolder wh(edgeLock_);
    // Only version 0
    edgeSchemas_[std::make_pair(space, edge)][0] = schema;
}

std::shared_ptr<const SchemaProviderIf> AdHocSchemaManager::getTagSchema(
        folly::StringPiece spaceName,
        folly::StringPiece tagName,
        int32_t ver) {
    return getTagSchema(toGraphSpaceID(spaceName), toTagID(tagName), ver);
}

std::shared_ptr<const SchemaProviderIf> AdHocSchemaManager::getTagSchema(
        GraphSpaceID space, TagID tag, int32_t ver) {
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

bool AdHocSchemaManager::removeTagSchema(folly::StringPiece spaceName,
                                         folly::StringPiece tagName,
                                         int32_t version) {
    return removeTagSchema(toGraphSpaceID(spaceName), toTagID(tagName), version);
}

bool AdHocSchemaManager::removeTagSchema(GraphSpaceID space, TagID tag, int32_t version) {
    folly::RWSpinLock::ReadHolder rh(tagLock_);
    auto key = std::make_pair(space, tag);
    auto it = tagSchemas_.find(key);
    if (it == tagSchemas_.end()) {
        return false;
    } else {
        if (version < 0) {
            if (it->second.empty()) {
                return false;
            } else {
                auto latestVersionKey = it->second.rbegin()->first;
                tagSchemas_[key].erase(latestVersionKey);
                return true;
            }
        } else {
            auto verIt = it->second.find(version);
            if (verIt == it->second.end()) {
                return false;
            } else {
                auto versionKey = verIt->first;
                tagSchemas_[key].erase(versionKey);
                return true;
            }
        }
    }
}

int32_t AdHocSchemaManager::getNewestTagSchemaVer(folly::StringPiece spaceName,
                                                  folly::StringPiece tagName) {
    return getNewestTagSchemaVer(toGraphSpaceID(spaceName), toTagID(tagName));
}

int32_t AdHocSchemaManager::getNewestTagSchemaVer(GraphSpaceID space, TagID tag) {
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

std::shared_ptr<const SchemaProviderIf> AdHocSchemaManager::getEdgeSchema(
        folly::StringPiece spaceName,
        folly::StringPiece typeName,
        int32_t ver) {
    return getEdgeSchema(toGraphSpaceID(spaceName), toEdgeType(typeName), ver);
}

std::shared_ptr<const SchemaProviderIf> AdHocSchemaManager::getEdgeSchema(
        GraphSpaceID space, EdgeType edge, int32_t ver) {
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

bool AdHocSchemaManager::removeEdgeSchema(folly::StringPiece spaceName,
                                          folly::StringPiece edgeName,
                                          int32_t version) {
    return removeEdgeSchema(toGraphSpaceID(spaceName), toEdgeType(edgeName), version);
}

bool AdHocSchemaManager::removeEdgeSchema(GraphSpaceID space, EdgeType edge, int32_t version) {
    folly::RWSpinLock::ReadHolder rh(edgeLock_);
    auto key = std::make_pair(space, edge);
    auto it = edgeSchemas_.find(key);
    if (it == edgeSchemas_.end()) {
        return false;
    } else {
        if (version < 0) {
            if (it->second.empty()) {
                return false;
            } else {
                auto latestVersionKey = it->second.rbegin()->first;
                edgeSchemas_[key].erase(latestVersionKey);
                return true;
            }
        } else {
            auto verIt = it->second.find(version);
            if (verIt == it->second.end()) {
                return false;
            } else {
                auto versionKey = verIt->first;
                edgeSchemas_[key].erase(versionKey);
                return true;
            }
        }
    }
}

int32_t AdHocSchemaManager::getNewestEdgeSchemaVer(folly::StringPiece spaceName,
                                                   folly::StringPiece typeName) {
    return getNewestEdgeSchemaVer(toGraphSpaceID(spaceName), toEdgeType(typeName));
}

int32_t AdHocSchemaManager::getNewestEdgeSchemaVer(GraphSpaceID space, EdgeType edge) {
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

GraphSpaceID AdHocSchemaManager::toGraphSpaceID(folly::StringPiece spaceName) {
    return folly::hash::fnv32_buf(spaceName.start(), spaceName.size());
}

TagID AdHocSchemaManager::toTagID(folly::StringPiece tagName) {
    return folly::hash::fnv32_buf(tagName.start(), tagName.size());
}

EdgeType AdHocSchemaManager::toEdgeType(folly::StringPiece typeName) {
    return folly::hash::fnv32_buf(typeName.start(), typeName.size());
}

}  // namespace meta
}  // namespace nebula

