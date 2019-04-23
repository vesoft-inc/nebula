/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/ServerBasedSchemaManager.h"

namespace nebula {
namespace meta {

std::shared_ptr<const SchemaProviderIf>
ServerBasedSchemaManager::getTagSchema(GraphSpaceID space, TagID tag, int32_t ver) {
    CHECK(metaClient_);
    auto ret = metaClient_->getTagSchemeFromCache(space, tag, ver);
    if (ret.ok()) {
        return ret.value();
    }

    return std::shared_ptr<const SchemaProviderIf>();
}

std::shared_ptr<const SchemaProviderIf>
ServerBasedSchemaManager::getTagSchema(folly::StringPiece spaceName,
                                       folly::StringPiece tagName,
                                       int32_t ver) {
    auto space = toGraphSpaceID(spaceName);
    return getTagSchema(space, toTagID(space, tagName), ver);
}

// Returns a negative number when the schema does not exist
int32_t ServerBasedSchemaManager::getNewestTagSchemaVer(GraphSpaceID space, TagID tag) {
    CHECK(metaClient_);
    auto ret = metaClient_->getTagSchemeFromCache(space, tag);
    if (ret.ok()) {
        return ret.value()->getVersion();
    }

    return -1;
}

int32_t ServerBasedSchemaManager::getNewestTagSchemaVer(folly::StringPiece spaceName,
                                                        folly::StringPiece tagName) {
    auto space = toGraphSpaceID(spaceName);
    return getNewestTagSchemaVer(space, toTagID(space, tagName));
}

std::shared_ptr<const SchemaProviderIf>
ServerBasedSchemaManager::getEdgeSchema(GraphSpaceID space, EdgeType edge, int32_t ver) {
    CHECK(metaClient_);
    auto ret = metaClient_->getEdgeSchemeFromCache(space, edge, ver);
    if (ret.ok()) {
        return ret.value();
    }

    return std::shared_ptr<const SchemaProviderIf>();
}

std::shared_ptr<const SchemaProviderIf> ServerBasedSchemaManager::getEdgeSchema(
        folly::StringPiece spaceName,
        folly::StringPiece typeName,
        int32_t ver) {
    auto space = toGraphSpaceID(spaceName);
    return getEdgeSchema(space, toEdgeType(space, typeName), ver);
}

// Returns a negative number when the schema does not exist
int32_t ServerBasedSchemaManager::getNewestEdgeSchemaVer(GraphSpaceID space,
                                                         EdgeType edge) {
    CHECK(metaClient_);
    auto ret = metaClient_->getEdgeSchemeFromCache(space, edge);
    if (ret.ok()) {
        return ret.value()->getVersion();
    }

    return -1;
}

int32_t ServerBasedSchemaManager::getNewestEdgeSchemaVer(folly::StringPiece spaceName,
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

}  // namespace meta
}  // namespace nebula

