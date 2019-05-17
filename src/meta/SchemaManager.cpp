/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/SchemaManager.h"
#include "meta/FileBasedSchemaManager.h"
#include "meta/ServerBasedSchemaManager.h"

DECLARE_string(schema_file);
DECLARE_string(meta_server_addrs);

namespace nebula {
namespace meta {

std::unique_ptr<SchemaManager> SchemaManager::create() {
    if (!FLAGS_schema_file.empty()) {
        std::unique_ptr<SchemaManager> sm(new FileBasedSchemaManager());
        return sm;
    } else if (!FLAGS_meta_server_addrs.empty()) {
        std::unique_ptr<SchemaManager> sm(new ServerBasedSchemaManager());
        return sm;
    } else {
        std::unique_ptr<SchemaManager> sm(new AdHocSchemaManager());
        return sm;
    }
}

void AdHocSchemaManager::addTagSchema(GraphSpaceID space,
                                      TagID tag,
                                      std::shared_ptr<SchemaProviderIf> schema) {
    {
        folly::RWSpinLock::WriteHolder wh(tagLock_);
        // Only version 0
        tagSchemas_[std::make_pair(space, tag)][0] = schema;
    }
    {
        folly::RWSpinLock::WriteHolder wh(spaceLock_);
        spaces_.emplace(space);
    }
}

void AdHocSchemaManager::addEdgeSchema(GraphSpaceID space,
                                       EdgeType edge,
                                       std::shared_ptr<SchemaProviderIf> schema) {
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

std::shared_ptr<const SchemaProviderIf>
AdHocSchemaManager::getTagSchema(folly::StringPiece spaceName,
                                 folly::StringPiece tagName,
                                 SchemaVer ver) {
    auto space = toGraphSpaceID(spaceName);
    return getTagSchema(space, toTagID(space, tagName), ver);
}

std::shared_ptr<const SchemaProviderIf> AdHocSchemaManager::getTagSchema(GraphSpaceID space,
                                                                         TagID tag,
                                                                         SchemaVer ver) {
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

SchemaVer AdHocSchemaManager::getNewestTagSchemaVer(folly::StringPiece spaceName,
                                                    folly::StringPiece tagName) {
    auto space = toGraphSpaceID(spaceName);
    return getNewestTagSchemaVer(space, toTagID(space, tagName));
}

SchemaVer AdHocSchemaManager::getNewestTagSchemaVer(GraphSpaceID space, TagID tag) {
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

std::shared_ptr<const SchemaProviderIf>
AdHocSchemaManager::getEdgeSchema(folly::StringPiece spaceName,
                                  folly::StringPiece typeName,
                                  SchemaVer ver) {
    auto space = toGraphSpaceID(spaceName);
    return getEdgeSchema(space, toEdgeType(space, typeName), ver);
}

std::shared_ptr<const SchemaProviderIf>
AdHocSchemaManager::getEdgeSchema(GraphSpaceID space,
                                  EdgeType edge,
                                  SchemaVer ver) {
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

SchemaVer AdHocSchemaManager::getNewestEdgeSchemaVer(folly::StringPiece spaceName,
                                                     folly::StringPiece typeName) {
    auto space = toGraphSpaceID(spaceName);
    return getNewestEdgeSchemaVer(space, toEdgeType(space, typeName));
}

SchemaVer AdHocSchemaManager::getNewestEdgeSchemaVer(GraphSpaceID space, EdgeType edge) {
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

TagID AdHocSchemaManager::toTagID(GraphSpaceID space, folly::StringPiece tagName) {
    UNUSED(space);
    return folly::hash::fnv32_buf(tagName.start(), tagName.size());
}

EdgeType AdHocSchemaManager::toEdgeType(GraphSpaceID space, folly::StringPiece typeName) {
    UNUSED(space);
    return folly::hash::fnv32_buf(typeName.start(), typeName.size());
}

Status AdHocSchemaManager::checkSpaceExist(folly::StringPiece spaceName) {
    folly::RWSpinLock::ReadHolder rh(spaceLock_);
    auto it = spaces_.find(toGraphSpaceID(spaceName));
    if (it == spaces_.end()) {
        return Status::Error("Space not exist");
    }
    return Status::OK();
}

}  // namespace meta
}  // namespace nebula

