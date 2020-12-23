/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/meta/ServerBasedIndexManager.h"

namespace nebula {
namespace meta {

ServerBasedIndexManager::~ServerBasedIndexManager() {
    if (nullptr != metaClient_) {
        metaClient_ = nullptr;
    }
}

std::unique_ptr<ServerBasedIndexManager> ServerBasedIndexManager::create(MetaClient *client) {
    auto mgr = std::make_unique<ServerBasedIndexManager>();
    mgr->init(client);
    return mgr;
}

void ServerBasedIndexManager::init(MetaClient *client) {
    CHECK_NOTNULL(client);
    metaClient_ = client;
}

StatusOr<std::shared_ptr<IndexItem>>
ServerBasedIndexManager::getTagIndex(GraphSpaceID space, IndexID index) {
    return metaClient_->getTagIndexFromCache(space, index);
}

StatusOr<std::shared_ptr<IndexItem>>
ServerBasedIndexManager::getEdgeIndex(GraphSpaceID space, IndexID index) {
    return metaClient_->getEdgeIndexFromCache(space, index);
}

StatusOr<std::vector<std::shared_ptr<IndexItem>>>
ServerBasedIndexManager::getTagIndexes(GraphSpaceID space) {
    return metaClient_->getTagIndexesFromCache(space);
}

StatusOr<std::vector<std::shared_ptr<IndexItem>>>
ServerBasedIndexManager::getEdgeIndexes(GraphSpaceID space) {
    return metaClient_->getEdgeIndexesFromCache(space);
}

StatusOr<IndexID>
ServerBasedIndexManager::toTagIndexID(GraphSpaceID space, std::string tagName) {
    auto status = metaClient_->getTagIndexByNameFromCache(space, tagName);
    if (!status.ok()) {
        return status.status();
    }
    return status.value()->get_index_id();
}

StatusOr<IndexID>
ServerBasedIndexManager::toEdgeIndexID(GraphSpaceID space, std::string edgeName) {
    auto status = metaClient_->getEdgeIndexByNameFromCache(space, edgeName);
    if (!status.ok()) {
        return status.status();
    }
    return status.value()->get_index_id();
}

Status ServerBasedIndexManager::checkTagIndexed(GraphSpaceID space, IndexID index) {
    return metaClient_->checkTagIndexed(space, index);
}

Status ServerBasedIndexManager::checkEdgeIndexed(GraphSpaceID space, IndexID index) {
    return metaClient_->checkEdgeIndexed(space, index);
}

}  // namespace meta
}  // namespace nebula
