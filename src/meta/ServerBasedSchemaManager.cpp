/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "meta/ServerBasedSchemaManager.h"

namespace nebula {
namespace meta {

ServerBasedSchemaManager::~ServerBasedSchemaManager() {
    if (nullptr != metaClient_) {
        metaClient_ = nullptr;
    }
}

void ServerBasedSchemaManager::init(MetaClient *client) {
    CHECK_NOTNULL(client);
    metaClient_ = client;
}

std::shared_ptr<const SchemaProviderIf>
ServerBasedSchemaManager::getTagSchema(GraphSpaceID space, TagID tag, SchemaVer ver) {
    VLOG(3) << "Get Tag Schema Space " << space << ", TagID " << tag << ", Version " << ver;
    CHECK(metaClient_);
    // ver less 0, get the newest ver
    if (ver < 0) {
        auto ret = getLatestTagSchemaVersion(space, tag);
        if (!ret.ok()) {
            return std::shared_ptr<const SchemaProviderIf>();
        }
        ver = ret.value();
    }
    auto ret = metaClient_->getTagSchemaFromCache(space, tag, ver);
    if (ret.ok()) {
        return ret.value();
    }

    return std::shared_ptr<const SchemaProviderIf>();
}

// Returns a negative number when the schema does not exist
StatusOr<SchemaVer> ServerBasedSchemaManager::getLatestTagSchemaVersion(GraphSpaceID space,
                                                                        TagID tag) {
    CHECK(metaClient_);
    return  metaClient_->getLatestTagVersionFromCache(space, tag);
}

std::shared_ptr<const SchemaProviderIf>
ServerBasedSchemaManager::getEdgeSchema(GraphSpaceID space, EdgeType edge, SchemaVer ver) {
    VLOG(3) << "Get Edge Schema Space " << space << ", EdgeType " << edge << ", Version " << ver;
    CHECK(metaClient_);
    // ver less 0, get the newest ver
    if (ver < 0) {
        auto ret = getLatestEdgeSchemaVersion(space, edge);
        if (!ret.ok()) {
            return std::shared_ptr<const SchemaProviderIf>();
        }
        ver = ret.value();
    }

    auto ret = metaClient_->getEdgeSchemaFromCache(space, edge, ver);
    if (ret.ok()) {
        return ret.value();
    }

    return std::shared_ptr<const SchemaProviderIf>();
}

// Returns a negative number when the schema does not exist
StatusOr<SchemaVer> ServerBasedSchemaManager::getLatestEdgeSchemaVersion(GraphSpaceID space,
                                                                         EdgeType edge) {
    CHECK(metaClient_);
    return  metaClient_->getLatestEdgeVersionFromCache(space, edge);
}

StatusOr<GraphSpaceID> ServerBasedSchemaManager::toGraphSpaceID(folly::StringPiece spaceName) {
    CHECK(metaClient_);
    return  metaClient_->getSpaceIdByNameFromCache(spaceName.str());
}

StatusOr<TagID> ServerBasedSchemaManager::toTagID(GraphSpaceID space,
                                                  folly::StringPiece tagName) {
    CHECK(metaClient_);
    return metaClient_->getTagIDByNameFromCache(space, tagName.str());
}

StatusOr<std::string> ServerBasedSchemaManager::toTagName(GraphSpaceID space, TagID tagId) {
    CHECK(metaClient_);
    return metaClient_->getTagNameByIdFromCache(space, tagId);
}

StatusOr<EdgeType> ServerBasedSchemaManager::toEdgeType(GraphSpaceID space,
                                                        folly::StringPiece typeName) {
    CHECK(metaClient_);
    return metaClient_->getEdgeTypeByNameFromCache(space, typeName.str());
}

StatusOr<std::string> ServerBasedSchemaManager::toEdgeName(GraphSpaceID space, EdgeType edgeType) {
    CHECK(metaClient_);
    return metaClient_->getEdgeNameByTypeFromCache(space, edgeType);
}

StatusOr<std::vector<std::string>> ServerBasedSchemaManager::getAllEdge(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->getAllEdgeFromCache(space);
}

StatusOr<std::vector<std::string>> ServerBasedSchemaManager::getAllTag(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->getAllTagFromCache(space);
}

}  // namespace meta
}  // namespace nebula
