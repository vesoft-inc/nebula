/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/AdHocSchemaManager.h"

namespace nebula {
namespace mock {

void AdHocSchemaManager::addTagSchema(GraphSpaceID space,
                                      TagID tag,
                                      std::shared_ptr<nebula::meta::NebulaSchemaProvider> schema) {
    {
        folly::RWSpinLock::WriteHolder wh(tagLock_);
        // schema must be added from version 0
        tagSchemasInVector_[space][tag].emplace_back(schema);
        tagSchemasInMap_[space][std::make_pair(tag, schema->getVersion())] = schema;
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
                                       std::shared_ptr<nebula::meta::NebulaSchemaProvider> schema) {
    {
        folly::RWSpinLock::WriteHolder wh(edgeLock_);
        // schema must be added from version 0
        edgeSchemasInVector_[space][edge].emplace_back(schema);
        edgeSchemasInMap_[space][std::make_pair(edge, schema->getVersion())] = schema;
    }
    {
        folly::RWSpinLock::WriteHolder wh(spaceLock_);
        spaces_.emplace(space);
    }
}

void AdHocSchemaManager::removeTagSchema(GraphSpaceID space, TagID tag) {
    folly::RWSpinLock::WriteHolder wh(tagLock_);
    auto spaceIt = tagSchemasInVector_.find(space);
    if (spaceIt != tagSchemasInVector_.end()) {
        spaceIt->second.erase(tag);
    }
}

std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
AdHocSchemaManager::getTagSchema(GraphSpaceID space,
                                 TagID tag,
                                 SchemaVer ver) {
    folly::RWSpinLock::ReadHolder rh(tagLock_);
    auto spaceIt = tagSchemasInVector_.find(space);
    if (spaceIt == tagSchemasInVector_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    auto tagIt = spaceIt->second.find(tag);
    if (tagIt == spaceIt->second.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    // Now check the version
    if (ver < 0) {
        // Get the latest version
        if (tagIt->second.empty()) {
            // No schema
            return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
        } else {
            // the newest is the last one
            return tagIt->second.back();
        }
    } else {
        // Looking for the specified version
        if (static_cast<size_t>(ver) >= tagIt->second.size()) {
            if (ver == 0 && tagIt->first == 0) {
                return tagIt->second[ver];
            }
            // Not found
            return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
        } else {
            return tagIt->second[ver];
        }
    }
}

std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
AdHocSchemaManager::getTagSchemaFromMap(GraphSpaceID space,
                                        TagID tag,
                                        SchemaVer ver) {
    folly::RWSpinLock::ReadHolder rh(tagLock_);
    auto spaceIt = tagSchemasInMap_.find(space);
    if (spaceIt == tagSchemasInMap_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    } else {
        auto tagIt = spaceIt->second.find(std::make_pair(tag, ver));
        if (tagIt == spaceIt->second.end()) {
            return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
        } else {
            return tagIt->second;
        }
    }
}

StatusOr<SchemaVer> AdHocSchemaManager::getLatestTagSchemaVersion(GraphSpaceID space, TagID tag) {
    folly::RWSpinLock::ReadHolder rh(tagLock_);
    auto spaceIt = tagSchemasInVector_.find(space);
    if (spaceIt == tagSchemasInVector_.end()) {
        // Not found
        return -1;
    }
    auto tagIt = spaceIt->second.find(tag);
    if (tagIt == spaceIt->second.end()) {
        // Not found
        return -1;
    }
    // Now get the latest version
    return tagIt->second.front()->getVersion();
}

std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
AdHocSchemaManager::getEdgeSchema(GraphSpaceID space,
                                  EdgeType edge,
                                  SchemaVer ver) {
    folly::RWSpinLock::ReadHolder rh(edgeLock_);
    auto spaceIt = edgeSchemasInVector_.find(space);
    if (spaceIt == edgeSchemasInVector_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    auto edgeIt = spaceIt->second.find(edge);
    if (edgeIt == spaceIt->second.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    // Now check the version
    if (ver < 0) {
        // Get the latest version
        if (edgeIt->second.empty()) {
            // No schema
            return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
        } else {
            // the newest is the last one
            return edgeIt->second.back();
        }
    } else {
        // Looking for the specified version
        if (static_cast<size_t>(ver) >= edgeIt->second.size()) {
            if (ver == 0 && edgeIt->first == 0) {
                return edgeIt->second[ver];
            }
            // Not found
            return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
        } else {
            return edgeIt->second[ver];
        }
    }
}

std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
AdHocSchemaManager::getEdgeSchemaFromMap(GraphSpaceID space,
                                         EdgeType edge,
                                         SchemaVer ver) {
    folly::RWSpinLock::ReadHolder rh(edgeLock_);
    auto spaceIt = edgeSchemasInMap_.find(space);
    if (spaceIt == edgeSchemasInMap_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    } else {
        auto tagIt = spaceIt->second.find(std::make_pair(edge, ver));
        if (tagIt == spaceIt->second.end()) {
            return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
        } else {
            return tagIt->second;
        }
    }
}

StatusOr<SchemaVer> AdHocSchemaManager::getLatestEdgeSchemaVersion(GraphSpaceID space,
                                                                   EdgeType edge) {
    folly::RWSpinLock::ReadHolder rh(edgeLock_);
    auto spaceIt = edgeSchemasInVector_.find(space);
    if (spaceIt == edgeSchemasInVector_.end()) {
        // Not found
        return -1;
    }
    auto edgeIt = spaceIt->second.find(edge);
    if (edgeIt == spaceIt->second.end()) {
        // Not found
        return -1;
    }
    // Now get the latest version
    return edgeIt->second.front()->getVersion();
}

StatusOr<GraphSpaceID> AdHocSchemaManager::toGraphSpaceID(folly::StringPiece spaceName) {
    try {
        return folly::to<GraphSpaceID>(spaceName);
    } catch (const std::exception& e) {
        return Status::SpaceNotFound();
    }
}

StatusOr<std::string> AdHocSchemaManager::toGraphSpaceName(GraphSpaceID) {
    return "default_space";
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

StatusOr<std::string> AdHocSchemaManager::toTagName(GraphSpaceID space, TagID tagId) {
    UNUSED(space);
    try {
        return folly::to<std::string>(tagId);
    } catch (const std::exception& e) {
        LOG(FATAL) << e.what();
    }
    return "";
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

StatusOr<int32_t> AdHocSchemaManager::getSpaceVidLen(GraphSpaceID space) {
    UNUSED(space);
    // Here default 32
    return 32;
}

StatusOr<meta::cpp2::PropertyType> AdHocSchemaManager::getSpaceVidType(GraphSpaceID) {
    return meta::cpp2::PropertyType::FIXED_STRING;
}

StatusOr<TagSchemas> AdHocSchemaManager::getAllVerTagSchema(GraphSpaceID space) {
    folly::RWSpinLock::ReadHolder rh(tagLock_);
    auto iter = tagSchemasInVector_.find(space);
    if (iter == tagSchemasInVector_.end()) {
        return Status::Error("Space not found");
    }
    return iter->second;
}

StatusOr<TagSchema> AdHocSchemaManager::getAllLatestVerTagSchema(GraphSpaceID space) {
    folly::RWSpinLock::ReadHolder rh(tagLock_);
    auto iter = tagSchemasInVector_.find(space);
    if (iter == tagSchemasInVector_.end()) {
        return Status::Error("Space not found");
    }
    TagSchema tagsSchema;
    tagsSchema.reserve(iter->second.size());
    // fetch all tagIds
    for (const auto& tagSchema : iter->second) {
        tagsSchema.emplace(tagSchema.first, tagSchema.second.back());
    }
    return tagsSchema;
}

StatusOr<EdgeSchemas> AdHocSchemaManager::getAllVerEdgeSchema(GraphSpaceID space) {
    folly::RWSpinLock::ReadHolder rh(edgeLock_);
    auto iter = edgeSchemasInVector_.find(space);
    if (iter == edgeSchemasInVector_.end()) {
        return Status::Error("Space not found");
    }
    return iter->second;
}

StatusOr<EdgeSchema> AdHocSchemaManager::getAllLatestVerEdgeSchema(GraphSpaceID space) {
    folly::RWSpinLock::ReadHolder rh(edgeLock_);
    auto iter = edgeSchemasInVector_.find(space);
    if (iter == edgeSchemasInVector_.end()) {
        return Status::Error("Space not found");
    }
    EdgeSchema edgesSchema;
    edgesSchema.reserve(iter->second.size());
    // fetch all edgetypes
    for (const auto& edgeSchema : iter->second) {
        edgesSchema.emplace(edgeSchema.first, edgeSchema.second.back());
    }
    return edgesSchema;
}


StatusOr<std::vector<nebula::meta::cpp2::FTClient>> AdHocSchemaManager::getFTClients() {
    return ftClients_;
}

void AdHocSchemaManager::addFTClient(const nebula::meta::cpp2::FTClient& client) {
    ftClients_.emplace_back(client);
}
}  // namespace mock
}  // namespace nebula
