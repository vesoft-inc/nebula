/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/ServerBasedSchemaManager.h"

DEFINE_string(meta_server, "",
              "The address (in the form of \"ip:port\") of the meta server");


namespace nebula {
namespace meta {

std::unique_ptr<MetaClient> ServerBasedSchemaManager::client_;

void ServerBasedSchemaManager::init() {
    client_ = std::make_unique<MetaClient>();
    client_->init();
}

// static
GraphSpaceID ServerBasedSchemaManager::toGraphSpaceID(const folly::StringPiece spaceName) {
    auto spaceStr = spaceName.str();
    auto ret = client_->getSpaceIdByNameFromCache(spaceStr);
    CHECK(ret.ok());
    return ret.value();
}


// static
TagID ServerBasedSchemaManager::toTagID(const folly::StringPiece tagName) {
    return folly::hash::fnv32_buf(tagName.start(), tagName.size());
}


// static
EdgeType ServerBasedSchemaManager::toEdgeType(const folly::StringPiece typeName) {
    return folly::hash::fnv32_buf(typeName.start(), typeName.size());
}

}  // namespace meta
}  // namespace nebula

