/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "AdHocSchemaManager.h"

namespace nebula {
namespace storage {

void AdHocSchemaManager::addTagSchema(GraphSpaceID space,
                                      TagID tag,
                                      std::shared_ptr<nebula::meta::SchemaProviderIf> schema) {
    {
        folly::RWSpinLock::WriteHolder wh(tagLock_);
        // Only version 0
        tagSchemas_[std::make_pair(space, tag)][0] = schema;
        auto key = folly::stringPrintf("%d_%d", space, tag);
        tagNameToId_[key] = tag;
    }
    {
        folly::RWSpinLock::WriteHolder wh(spaceLock_);
        spaces_.emplace(space);
    }
}

void AdHocSchemaManager::addEdgeSchema(GraphSpaceID space,
                                       EdgeType edge,
                                       std::shared_ptr<nebula::meta::SchemaProviderIf> schema) {
    {
        folly::RWSpinLock::WriteHolder wh(edgeLock_);
        // Only version 0
        edgeSchemas_[std::make_pair(space, edge)][0] = schema;
    }
    {
        folly::RWSpinLock::WriteHolder wh(spaceLock_);
        spaces_.emplace(space);
    }
}

void AdHocSchemaManager::removeTagSchema(GraphSpaceID space, TagID tag) {
    folly::RWSpinLock::WriteHolder wh(tagLock_);
    tagSchemas_.erase(std::make_pair(space, tag));
}

std::shared_ptr<const nebula::meta::SchemaProviderIf>
AdHocSchemaManager::getTagSchema(GraphSpaceID space,
                                 TagID tag,
                                 SchemaVer ver) {
    folly::RWSpinLock::ReadHolder rh(tagLock_);
    auto it = tagSchemas_.find(std::make_pair(space, tag));
    if (it == tagSchemas_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::SchemaProviderIf>();
    } else {
        // Now check the version
        if (ver < 0) {
            // Get the latest version
            if (it->second.empty()) {
                // No schema
                return std::shared_ptr<const nebula::meta::SchemaProviderIf>();
            } else {
                return it->second.rbegin()->second;
            }
        } else {
            // Looking for the specified version
            auto verIt = it->second.find(ver);
            if (verIt == it->second.end()) {
                // Not found
                return std::shared_ptr<const nebula::meta::SchemaProviderIf>();
            } else {
                return verIt->second;
            }
        }
    }
}

StatusOr<SchemaVer> AdHocSchemaManager::getNewestTagSchemaVer(GraphSpaceID space, TagID tag) {
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

std::shared_ptr<const nebula::meta::SchemaProviderIf>
AdHocSchemaManager::getEdgeSchema(GraphSpaceID space,
                                  EdgeType edge,
                                  SchemaVer ver) {
    folly::RWSpinLock::ReadHolder rh(edgeLock_);
    auto it = edgeSchemas_.find(std::make_pair(space, edge));
    if (it == edgeSchemas_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::SchemaProviderIf>();
    } else {
        // Now check the version
        if (ver < 0) {
            // Get the latest version
            if (it->second.empty()) {
                // No schema
                return std::shared_ptr<const nebula::meta::SchemaProviderIf>();
            } else {
                return it->second.rbegin()->second;
            }
        } else {
            // Looking for the specified version
            auto verIt = it->second.find(ver);
            if (verIt == it->second.end()) {
                // Not found
                return std::shared_ptr<const nebula::meta::SchemaProviderIf>();
            } else {
                return verIt->second;
            }
        }
    }
}

StatusOr<SchemaVer> AdHocSchemaManager::getNewestEdgeSchemaVer(GraphSpaceID space, EdgeType edge) {
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

StatusOr<GraphSpaceID> AdHocSchemaManager::toGraphSpaceID(folly::StringPiece spaceName) {
    try {
        return folly::to<GraphSpaceID>(spaceName);
    } catch (const std::exception& e) {
        return Status::SpaceNotFound();
    }
}

StatusOr<TagID> AdHocSchemaManager::toTagID(GraphSpaceID space, folly::StringPiece tagName) {
    UNUSED(space);
    try {
        return folly::to<TagID>(tagName);
    } catch (const std::exception& e) {
        LOG(FATAL) << e.what();
    }
    return -1;
}

StatusOr<EdgeType> AdHocSchemaManager::toEdgeType(GraphSpaceID space, folly::StringPiece typeName) {
    UNUSED(space);
    try {
        return folly::to<EdgeType>(typeName);
    } catch (const std::exception& e) {
        LOG(FATAL) << e.what();
    }
    return -1;
}

StatusOr<std::string> AdHocSchemaManager::toEdgeName(GraphSpaceID space, EdgeType edgeType) {
    UNUSED(space);
    try {
        return folly::to<std::string>(edgeType);
    } catch (const std::exception& e) {
        LOG(FATAL) << e.what();
    }
    return "";
}
}  // namespace storage
}  // namespace nebula
