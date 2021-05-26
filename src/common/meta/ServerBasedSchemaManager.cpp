/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/meta/ServerBasedSchemaManager.h"

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

StatusOr<int32_t> ServerBasedSchemaManager::getSpaceVidLen(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->getSpaceVidLen(space);
}

StatusOr<cpp2::PropertyType> ServerBasedSchemaManager::getSpaceVidType(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->getSpaceVidType(space);
}

StatusOr<int32_t> ServerBasedSchemaManager::getPartsNum(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->partsNum(space);
}

std::shared_ptr<const NebulaSchemaProvider>
ServerBasedSchemaManager::getTagSchema(GraphSpaceID space, TagID tag, SchemaVer ver) {
    VLOG(3) << "Get Tag Schema Space " << space << ", TagID " << tag << ", Version " << ver;
    CHECK(metaClient_);
    // ver less 0, get the newest ver
    if (ver < 0) {
        auto ret = getLatestTagSchemaVersion(space, tag);
        if (!ret.ok()) {
            return std::shared_ptr<const NebulaSchemaProvider>();
        }
        ver = ret.value();
    }
    auto ret = metaClient_->getTagSchemaFromCache(space, tag, ver);
    if (ret.ok()) {
        return ret.value();
    }

    return std::shared_ptr<const NebulaSchemaProvider>();
}

// Returns a negative number when the schema does not exist
StatusOr<SchemaVer> ServerBasedSchemaManager::getLatestTagSchemaVersion(GraphSpaceID space,
                                                                        TagID tag) {
    CHECK(metaClient_);
    return  metaClient_->getLatestTagVersionFromCache(space, tag);
}

std::shared_ptr<const NebulaSchemaProvider>
ServerBasedSchemaManager::getEdgeSchema(GraphSpaceID space, EdgeType edge, SchemaVer ver) {
    VLOG(3) << "Get Edge Schema Space " << space << ", EdgeType " << edge << ", Version " << ver;
    CHECK(metaClient_);
    // ver less 0, get the newest ver
    if (ver < 0) {
        auto ret = getLatestEdgeSchemaVersion(space, edge);
        if (!ret.ok()) {
            return std::shared_ptr<const NebulaSchemaProvider>();
        }
        ver = ret.value();
    }

    auto ret = metaClient_->getEdgeSchemaFromCache(space, edge, ver);
    if (ret.ok()) {
        return ret.value();
    }

    return std::shared_ptr<const NebulaSchemaProvider>();
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

StatusOr<std::string> ServerBasedSchemaManager::toGraphSpaceName(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->getSpaceNameByIdFromCache(space);
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

StatusOr<TagSchemas> ServerBasedSchemaManager::getAllVerTagSchema(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->getAllVerTagSchema(space);
}

StatusOr<TagSchema> ServerBasedSchemaManager::getAllLatestVerTagSchema(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->getAllLatestVerTagSchema(space);
}

StatusOr<EdgeSchemas> ServerBasedSchemaManager::getAllVerEdgeSchema(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->getAllVerEdgeSchema(space);
}

StatusOr<EdgeSchema> ServerBasedSchemaManager::getAllLatestVerEdgeSchema(GraphSpaceID space) {
    CHECK(metaClient_);
    return metaClient_->getAllLatestVerEdgeSchemaFromCache(space);
}

StatusOr<std::vector<nebula::meta::cpp2::FTClient>> ServerBasedSchemaManager::getFTClients() {
    auto ret = metaClient_->getFTClientsFromCache();
    if (!ret.ok()) {
        return ret.status();
    }
    if (ret.value().empty()) {
        return Status::Error("fulltext client list is empty");
    }
    return std::move(ret).value();
}

StatusOr<std::pair<std::string, nebula::meta::cpp2::FTIndex>>
ServerBasedSchemaManager::getFTIndex(GraphSpaceID spaceId, int32_t schemaId) {
    auto ret = metaClient_->getFTIndexBySpaceSchemaFromCache(spaceId, schemaId);
    if (!ret.ok()) {
        return ret.status();
    }
    return std::move(ret).value();
}

std::unique_ptr<ServerBasedSchemaManager> ServerBasedSchemaManager::create(MetaClient *client) {
    auto mgr = std::make_unique<ServerBasedSchemaManager>();
    mgr->init(client);
    return mgr;
}


}  // namespace meta
}  // namespace nebula
