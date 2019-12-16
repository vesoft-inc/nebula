/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "AdHocIndexManager.h"

namespace nebula {
namespace storage {

void AdHocIndexManager::addTagIndex(GraphSpaceID space,
                                    IndexID indexID,
                                    TagID tagID,
                                    meta::cpp2::IndexFields fields) {
    folly::RWSpinLock::WriteHolder wh(tagIndexLock_);
    auto iter = tagIndexes_.find(space);
    if (iter == tagIndexes_.end()) {
        auto tuple = std::make_tuple(indexID, tagID, fields);
        std::shared_ptr<TagIndexTuple> tuplePtr = std::make_shared<TagIndexTuple>(tuple);
        std::vector<std::shared_ptr<TagIndexTuple>> tuples{tuplePtr};
        tagIndexes_.emplace(space, tuples);
    }
}

void AdHocIndexManager::addEdgeIndex(GraphSpaceID space,
                                     IndexID indexID,
                                     EdgeType edgeType,
                                     meta::cpp2::IndexFields fields) {
    folly::RWSpinLock::WriteHolder wh(edgeIndexLock_);
    auto iter = edgeIndexes_.find(space);
    if (iter == edgeIndexes_.end()) {
        auto tuple = std::make_tuple(indexID, edgeType, fields);
        std::shared_ptr<EdgeIndexTuple> tuplePtr = std::make_shared<EdgeIndexTuple>(tuple);
        std::vector<std::shared_ptr<EdgeIndexTuple>> tuples{tuplePtr};
        edgeIndexes_.emplace(space, tuples);
    }
}

StatusOr<std::shared_ptr<std::pair<TagID, meta::cpp2::IndexFields>>>
AdHocIndexManager::getTagIndex(GraphSpaceID space, IndexID index) {
    folly::RWSpinLock::ReadHolder rh(tagIndexLock_);
    auto iter = tagIndexes_.find(space);
    if (iter == tagIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    auto tuples = iter->second;
    for (auto &tuple : tuples) {
        if (std::get<1>(*tuple) == index) {
            auto tagID = std::get<0>(*tuple);
            auto fields = std::get<2>(*tuple);
            auto pair = std::make_pair(tagID, fields);
            return std::make_shared<std::pair<TagID, meta::cpp2::IndexFields>>(pair);
        }
    }
    return Status::TagIndexNotFound();
}

StatusOr<std::shared_ptr<std::pair<EdgeType, meta::cpp2::IndexFields>>>
AdHocIndexManager::getEdgeIndex(GraphSpaceID space, IndexID index) {
    folly::RWSpinLock::ReadHolder rh(edgeIndexLock_);
    auto iter = edgeIndexes_.find(space);
    if (iter == edgeIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    auto tuples = iter->second;
    for (auto &tuple : tuples) {
        if (std::get<1>(*tuple) == index) {
            auto edgeType = std::get<0>(*tuple);
            auto fields = std::get<2>(*tuple);
            auto pair = std::make_pair(edgeType, fields);
            return std::make_shared<std::pair<EdgeType, meta::cpp2::IndexFields>>(pair);
        }
    }
    return Status::EdgeIndexNotFound();
}

StatusOr<std::vector<std::shared_ptr<TagIndexTuple>>>
AdHocIndexManager::getTagIndexes(GraphSpaceID space) {
    folly::RWSpinLock::ReadHolder rh(tagIndexLock_);
    auto iter = tagIndexes_.find(space);
    if (iter == tagIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    return iter->second;
}

StatusOr<std::vector<std::shared_ptr<EdgeIndexTuple>>>
AdHocIndexManager::getEdgeIndexes(GraphSpaceID space) {
    folly::RWSpinLock::ReadHolder rh(edgeIndexLock_);
    auto iter = edgeIndexes_.find(space);
    if (iter == edgeIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    return iter->second;
}

Status AdHocIndexManager::checkTagIndexed(GraphSpaceID space, TagID tagID) {
    folly::RWSpinLock::ReadHolder rh(tagIndexLock_);
    auto iter = tagIndexes_.find(space);
    if (iter == tagIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    auto tuples = iter->second;
    for (auto &tuple : tuples) {
        if (std::get<0>(*tuple) == tagID) {
            return Status::OK();
        }
    }
    return Status::TagNotFound();
}

Status AdHocIndexManager::checkEdgeIndexed(GraphSpaceID space, EdgeType edgeType) {
    folly::RWSpinLock::ReadHolder rh(edgeIndexLock_);
    auto iter = edgeIndexes_.find(space);
    if (iter == edgeIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    auto tuples = iter->second;
    for (auto &tuple : tuples) {
        if (std::get<0>(*tuple) == edgeType) {
            return Status::OK();
        }
    }
    return Status::EdgeNotFound();
}

}  // namespace storage
}  // namespace nebula

