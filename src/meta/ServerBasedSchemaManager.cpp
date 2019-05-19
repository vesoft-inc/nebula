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
    metaClient_ = client;
    CHECK_NOTNULL(metaClient_);
}

std::shared_ptr<const SchemaProviderIf>
ServerBasedSchemaManager::getTagSchema(GraphSpaceID space, TagID tag, SchemaVer ver) {
    VLOG(3) << "Get Tag Schema Space " << space << ", TagID " << tag << ", Version " << ver;
    CHECK(metaClient_);
    // ver less 0, get the newest ver
    if (ver < 0) {
        ver = getNewestTagSchemaVer(space, tag);
    }
    auto ret = metaClient_->getTagSchemaFromCache(space, tag, ver);
    if (ret.ok()) {
        return ret.value();
    }

    return std::shared_ptr<const SchemaProviderIf>();
}

std::shared_ptr<const SchemaProviderIf>
ServerBasedSchemaManager::getTagSchema(folly::StringPiece spaceName,
                                       folly::StringPiece tagName,
                                       SchemaVer ver) {
    auto spaceStatus = toGraphSpaceID(spaceName);
    if (!spaceStatus.ok()) {
        return std::shared_ptr<const SchemaProviderIf>();
    }
    auto spaceId = spaceStatus.value();
    return getTagSchema(spaceId, toTagID(spaceId, tagName), ver);
}

// Returns a negative number when the schema does not exist
SchemaVer ServerBasedSchemaManager::getNewestTagSchemaVer(GraphSpaceID space, TagID tag) {
    CHECK(metaClient_);
    return  metaClient_->getNewestTagVerFromCache(space, tag);
}

SchemaVer ServerBasedSchemaManager::getNewestTagSchemaVer(folly::StringPiece spaceName,
                                                          folly::StringPiece tagName) {
    auto spaceStatus = toGraphSpaceID(spaceName);
    if (!spaceStatus.ok()) {
        return -1;
    }
    auto spaceId = spaceStatus.value();
    return getNewestTagSchemaVer(spaceId, toTagID(spaceId, tagName));
}

std::shared_ptr<const SchemaProviderIf>
ServerBasedSchemaManager::getEdgeSchema(GraphSpaceID space, EdgeType edge, SchemaVer ver) {
    VLOG(3) << "Get Edge Schema Space " << space << ", EdgeType " << edge << ", Version " << ver;
    CHECK(metaClient_);
    // ver less 0, get the newest ver
    if (ver < 0) {
        ver = getNewestEdgeSchemaVer(space, edge);
    }

    auto ret = metaClient_->getEdgeSchemaFromCache(space, edge, ver);
    if (ret.ok()) {
        return ret.value();
    }

    return std::shared_ptr<const SchemaProviderIf>();
}

std::shared_ptr<const SchemaProviderIf>
ServerBasedSchemaManager::getEdgeSchema(folly::StringPiece spaceName,
                                        folly::StringPiece typeName,
                                        SchemaVer ver) {
    auto spaceStatus = toGraphSpaceID(spaceName);
    if (!spaceStatus.ok()) {
        return std::shared_ptr<const SchemaProviderIf>();
    }
    auto spaceId = spaceStatus.value();
    return getEdgeSchema(spaceId, toEdgeType(spaceId, typeName), ver);
}

// Returns a negative number when the schema does not exist
SchemaVer ServerBasedSchemaManager::getNewestEdgeSchemaVer(GraphSpaceID space,
                                                           EdgeType edge) {
    CHECK(metaClient_);
    return  metaClient_->getNewestEdgeVerFromCache(space, edge);
}

SchemaVer ServerBasedSchemaManager::getNewestEdgeSchemaVer(folly::StringPiece spaceName,
                                                           folly::StringPiece typeName) {
    auto spaceStatus = toGraphSpaceID(spaceName);
    if (!spaceStatus.ok()) {
        return -1;
    }
    auto spaceId = spaceStatus.value();
    return getNewestEdgeSchemaVer(spaceId, toEdgeType(spaceId, typeName));
}

StatusOr<GraphSpaceID> ServerBasedSchemaManager::toGraphSpaceID(folly::StringPiece spaceName) {
    CHECK(metaClient_);
    return  metaClient_->getSpaceIdByNameFromCache(spaceName.str());
}

TagID ServerBasedSchemaManager::toTagID(GraphSpaceID space,
                                        folly::StringPiece tagName) {
    CHECK(metaClient_);
    auto ret = metaClient_->getTagIDByNameFromCache(space, tagName.str());
    if (ret.ok()) {
        return ret.value();
    }
    return -1;
}

EdgeType ServerBasedSchemaManager::toEdgeType(GraphSpaceID space,
                                              folly::StringPiece typeName) {
    CHECK(metaClient_);
    auto ret = metaClient_->getEdgeTypeByNameFromCache(space, typeName.str());
    if (ret.ok()) {
        return ret.value();
    }
    return -1;
}

}  // namespace meta
}  // namespace nebula

