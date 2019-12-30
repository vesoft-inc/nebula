/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/IndexManager.h"

namespace nebula {
namespace meta {

std::unique_ptr<IndexManager> IndexManager::create() {
    return std::make_unique<IndexManager>();
}

void IndexManager::init(MetaClient *client) {
    CHECK_NOTNULL(client);
    metaClient_ = client;
}

StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
IndexManager::getTagIndex(GraphSpaceID space, IndexID index) {
    return metaClient_->getTagIndexFromCache(space, index);
}

StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
IndexManager::getEdgeIndex(GraphSpaceID space, IndexID index) {
    return metaClient_->getEdgeIndexFromCache(space, index);
}

StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
IndexManager::getTagIndexes(GraphSpaceID space) {
    return metaClient_->getTagIndexesFromCache(space);
}

StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
IndexManager::getEdgeIndexes(GraphSpaceID space) {
    return metaClient_->getEdgeIndexesFromCache(space);
}

StatusOr<IndexID>
IndexManager::toTagIndexID(GraphSpaceID space, std::string tagName) {
    auto status = metaClient_->getTagIndexByNameFromCache(space, tagName);
    if (!status.ok()) {
        return status.status();
    }
    return status.value()->get_index_id();
}

StatusOr<IndexID>
IndexManager::toEdgeIndexID(GraphSpaceID space, std::string edgeName) {
    auto status = metaClient_->getEdgeIndexByNameFromCache(space, edgeName);
    if (!status.ok()) {
        return status.status();
    }
    return status.value()->get_index_id();
}

Status IndexManager::checkTagIndexed(GraphSpaceID space, TagID tagID) {
    return metaClient_->checkTagIndexed(space, tagID);
}

Status IndexManager::checkEdgeIndexed(GraphSpaceID space, EdgeType edgeType) {
    return metaClient_->checkEdgeIndexed(space, edgeType);
}

}   // namespace meta
}   // namespace nebula

