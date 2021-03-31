/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/AdHocIndexManager.h"

namespace nebula {
namespace mock {

void AdHocIndexManager::addTagIndex(GraphSpaceID space,
                                    TagID tagID,
                                    IndexID indexID,
                                    std::vector<nebula::meta::cpp2::ColumnDef>&& fields) {
    folly::RWSpinLock::WriteHolder wh(tagIndexLock_);
    IndexItem item;
    item.set_index_id(indexID);
    item.set_index_name(folly::stringPrintf("index_%d", indexID));
    nebula::meta::cpp2::SchemaID schemaID;
    schemaID.set_tag_id(tagID);
    item.set_schema_id(schemaID);
    item.set_schema_name(folly::stringPrintf("tag_%d", tagID));
    item.set_fields(std::move(fields));
    std::shared_ptr<IndexItem> itemPtr = std::make_shared<IndexItem>(item);

    auto iter = tagIndexes_.find(space);
    if (iter == tagIndexes_.end()) {
        std::vector<std::shared_ptr<IndexItem>> items{itemPtr};
        tagIndexes_.emplace(space, std::move(items));
    } else {
        iter->second.emplace_back(std::move(itemPtr));
    }
}

void AdHocIndexManager::removeTagIndex(GraphSpaceID space,
                                       IndexID indexID) {
    folly::RWSpinLock::WriteHolder wh(tagIndexLock_);
    auto iter = tagIndexes_.find(space);
    if (iter != tagIndexes_.end()) {
        std::vector<std::shared_ptr<IndexItem>>::iterator iItemIter = iter->second.begin();
        for (; iItemIter != iter->second.end(); iItemIter++) {
            if (*(*iItemIter)->index_id_ref() == indexID) {
                iter->second.erase(iItemIter);
                return;
            }
        }
    }
}

void AdHocIndexManager::addEdgeIndex(GraphSpaceID space,
                                     EdgeType edgeType,
                                     IndexID indexID,
                                     std::vector<nebula::meta::cpp2::ColumnDef>&& fields) {
    folly::RWSpinLock::WriteHolder wh(edgeIndexLock_);
    IndexItem item;
    item.set_index_id(indexID);
    item.set_index_name(folly::stringPrintf("index_%d", indexID));
    nebula::meta::cpp2::SchemaID schemaID;
    schemaID.set_edge_type(edgeType);
    item.set_schema_id(schemaID);
    item.set_schema_name(folly::stringPrintf("edge_%d", edgeType));
    item.set_fields(std::move(fields));
    std::shared_ptr<IndexItem> itemPtr = std::make_shared<IndexItem>(item);

    auto iter = edgeIndexes_.find(space);
    if (iter == edgeIndexes_.end()) {
        std::vector<std::shared_ptr<IndexItem>> items{itemPtr};
        edgeIndexes_.emplace(space, items);
    } else {
        iter->second.emplace_back(std::move(itemPtr));
    }
}

StatusOr<std::shared_ptr<IndexItem>>
AdHocIndexManager::getTagIndex(GraphSpaceID space, IndexID index) {
    folly::RWSpinLock::ReadHolder rh(tagIndexLock_);
    auto iter = tagIndexes_.find(space);
    if (iter == tagIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    auto items = iter->second;
    for (auto &item : items) {
        if (item->get_index_id() == index) {
            return item;
        }
    }
    return Status::IndexNotFound();
}

StatusOr<std::shared_ptr<IndexItem>>
AdHocIndexManager::getEdgeIndex(GraphSpaceID space, IndexID index) {
    folly::RWSpinLock::ReadHolder rh(edgeIndexLock_);
    auto iter = edgeIndexes_.find(space);
    if (iter == edgeIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    auto items = iter->second;
    for (auto &item : items) {
        if (item->get_index_id() == index) {
            return item;
        }
    }
    return Status::IndexNotFound();
}

StatusOr<std::vector<std::shared_ptr<IndexItem>>>
AdHocIndexManager::getTagIndexes(GraphSpaceID space) {
    folly::RWSpinLock::ReadHolder rh(tagIndexLock_);
    auto iter = tagIndexes_.find(space);
    if (iter == tagIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    return iter->second;
}

StatusOr<std::vector<std::shared_ptr<IndexItem>>>
AdHocIndexManager::getEdgeIndexes(GraphSpaceID space) {
    folly::RWSpinLock::ReadHolder rh(edgeIndexLock_);
    auto iter = edgeIndexes_.find(space);
    if (iter == edgeIndexes_.end()) {
        return Status::SpaceNotFound();
    }
    return iter->second;
}

StatusOr<IndexID>
AdHocIndexManager::toTagIndexID(GraphSpaceID space, std::string indexName) {
    folly::RWSpinLock::ReadHolder rh(tagIndexLock_);
    auto iter = tagIndexes_.find(space);
    if (iter == tagIndexes_.end()) {
        return Status::SpaceNotFound();
    }

    auto items = iter->second;
    for (auto &item : items) {
        if (item->get_index_name() == indexName) {
            return item->get_index_id();
        }
    }
    return Status::TagNotFound();
}

StatusOr<IndexID>
AdHocIndexManager::toEdgeIndexID(GraphSpaceID space, std::string indexName) {
    folly::RWSpinLock::ReadHolder rh(edgeIndexLock_);
    auto iter = edgeIndexes_.find(space);
    if (iter == edgeIndexes_.end()) {
        return Status::SpaceNotFound();
    }

    auto items = iter->second;
    for (auto &item : items) {
        if (item->get_index_name() == indexName) {
            return item->get_index_id();
        }
    }
    return Status::EdgeNotFound();
}

Status AdHocIndexManager::checkTagIndexed(GraphSpaceID space, TagID tagID) {
    folly::RWSpinLock::ReadHolder rh(tagIndexLock_);
    auto iter = tagIndexes_.find(space);
    if (iter == tagIndexes_.end()) {
        return Status::SpaceNotFound();
    }

    auto items = iter->second;
    for (auto &item : items) {
        if (item->get_schema_id().get_tag_id() == tagID) {
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

    auto items = iter->second;
    for (auto &item : items) {
        if (item->get_schema_id().get_edge_type() == edgeType) {
            return Status::OK();
        }
    }
    return Status::EdgeNotFound();
}

}  // namespace mock
}  // namespace nebula

