/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    auto space = toGraphSpaceID(spaceName);
    return getTagSchema(space, toTagID(space, tagName), ver);
}

// Returns a negative number when the schema does not exist
SchemaVer ServerBasedSchemaManager::getNewestTagSchemaVer(GraphSpaceID space, TagID tag) {
    CHECK(metaClient_);
    return  metaClient_->getNewestTagVerFromCache(space, tag);
}

SchemaVer ServerBasedSchemaManager::getNewestTagSchemaVer(folly::StringPiece spaceName,
                                                          folly::StringPiece tagName) {
    auto space = toGraphSpaceID(spaceName);
    return getNewestTagSchemaVer(space, toTagID(space, tagName));
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

std::shared_ptr<const SchemaProviderIf> ServerBasedSchemaManager::getEdgeSchema(
        folly::StringPiece spaceName,
        folly::StringPiece typeName,
        SchemaVer ver) {
    auto space = toGraphSpaceID(spaceName);
    return getEdgeSchema(space, toEdgeType(space, typeName), ver);
}

// Returns a negative number when the schema does not exist
SchemaVer ServerBasedSchemaManager::getNewestEdgeSchemaVer(GraphSpaceID space,
                                                           EdgeType edge) {
    CHECK(metaClient_);
    return  metaClient_->getNewestEdgeVerFromCache(space, edge);
}

SchemaVer ServerBasedSchemaManager::getNewestEdgeSchemaVer(folly::StringPiece spaceName,
                                                           folly::StringPiece typeName) {
    auto space = toGraphSpaceID(spaceName);
    return getNewestEdgeSchemaVer(space, toEdgeType(space, typeName));
}

GraphSpaceID ServerBasedSchemaManager::toGraphSpaceID(folly::StringPiece spaceName) {
    CHECK(metaClient_);
    auto ret = metaClient_->getSpaceIdByNameFromCache(spaceName.str());
    if (ret.ok()) {
        return ret.value();
    }
    return -1;
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

Status ServerBasedSchemaManager::checkSpaceExist(folly::StringPiece spaceName) {
    CHECK(metaClient_);
    // Check from the cache, if space not exists, schemas also not exist
    auto ret = metaClient_->getSpaceIdByNameFromCache(spaceName.str());
    if (!ret.ok()) {
        return Status::Error("Space not exist");
    }
    return Status::OK();
}

}  // namespace meta
}  // namespace nebula

