/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "mock/AdHocIndexManager.h"

namespace nebula {
namespace mock {

void AdHocIndexManager::addEmptyIndex(GraphSpaceID space) {
  folly::RWSpinLock::WriteHolder wh(tagIndexLock_);
  std::vector<std::shared_ptr<IndexItem>> tagItems{};
  std::vector<std::shared_ptr<IndexItem>> edgeItems{};
  tagIndexes_.emplace(space, std::move(tagItems));
  edgeIndexes_.emplace(space, std::move(edgeItems));
}

void AdHocIndexManager::addTagIndex(GraphSpaceID space,
                                    TagID tagID,
                                    IndexID indexID,
                                    std::vector<nebula::meta::cpp2::ColumnDef> &&fields) {
  folly::RWSpinLock::WriteHolder wh(tagIndexLock_);
  IndexItem item;
  item.index_id_ref() = indexID;
  item.index_name_ref() = folly::stringPrintf("index_%d", indexID);
  nebula::cpp2::SchemaID schemaID;
  schemaID.tag_id_ref() = tagID;
  item.schema_id_ref() = schemaID;
  item.schema_name_ref() = folly::stringPrintf("tag_%d", tagID);
  item.fields_ref() = std::move(fields);
  std::shared_ptr<IndexItem> itemPtr = std::make_shared<IndexItem>(item);

  auto iter = tagIndexes_.find(space);
  if (iter == tagIndexes_.end()) {
    std::vector<std::shared_ptr<IndexItem>> items{itemPtr};
    tagIndexes_.emplace(space, std::move(items));
  } else {
    iter->second.emplace_back(std::move(itemPtr));
  }
}

void AdHocIndexManager::addTagAnnIndex(GraphSpaceID space,
                                       const std::vector<TagID> &tagIDs,
                                       IndexID indexID,
                                       nebula::meta::cpp2::ColumnDef &&field) {
  folly::RWSpinLock::WriteHolder wh(tagIndexLock_);
  AnnIndexItem item;
  item.index_id_ref() = indexID;
  item.index_name_ref() = folly::stringPrintf("index_%d", indexID);
  item.prop_name_ref() = "vec";
  std::vector<nebula::cpp2::SchemaID> schemaIDs;
  for (const auto &tagID : tagIDs) {
    nebula::cpp2::SchemaID schemaID;
    schemaID.tag_id_ref() = tagID;
    schemaIDs.emplace_back(schemaID);
  }
  item.schema_ids_ref() = std::move(schemaIDs);
  std::vector<std::string> tagNames;
  for (const auto &tagID : tagIDs) {
    tagNames.emplace_back(folly::stringPrintf("tag_%d", tagID));
  }
  item.schema_names_ref() = std::move(tagNames);
  std::vector<nebula::meta::cpp2::ColumnDef> fields;
  fields.emplace_back(std::move(field));
  item.fields_ref() = std::move(fields);
  item.ann_params_ref() =
      std::vector<std::string>{"IVF", "3", "l2", "3", "3"};  // [type, dim, METRIC, nlist, trainsz]
  std::shared_ptr<AnnIndexItem> itemPtr = std::make_shared<AnnIndexItem>(item);

  // Convert schema_ids to strings for logging
  std::vector<std::string> schemaIdStrs;
  for (const auto &id : item.get_schema_ids()) {
    if (id.get_tag_id() != 0) {
      schemaIdStrs.emplace_back(folly::stringPrintf("tag_%d", id.get_tag_id()));
    } else if (id.get_edge_type() != 0) {
      schemaIdStrs.emplace_back(folly::stringPrintf("edge_%d", id.get_edge_type()));
    }
  }

  LOG(ERROR) << "AnnIndexItem: "
             << "index_id=" << indexID << ", index_name=" << item.get_index_name()
             << ", schema_ids=" << folly::join(",", schemaIdStrs)
             << ", schema_names=" << folly::join(",", *item.schema_names_ref())
             << ", ann_params=" << folly::join(",", *item.ann_params_ref());
  auto iter = tagAnnIndexes_.find(space);
  if (iter == tagAnnIndexes_.end()) {
    std::vector<std::shared_ptr<AnnIndexItem>> items{itemPtr};
    tagAnnIndexes_.emplace(space, std::move(items));
  } else {
    iter->second.emplace_back(std::move(itemPtr));
  }
}

void AdHocIndexManager::removeTagIndex(GraphSpaceID space, IndexID indexID) {
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
                                     std::vector<nebula::meta::cpp2::ColumnDef> &&fields) {
  folly::RWSpinLock::WriteHolder wh(edgeIndexLock_);
  IndexItem item;
  item.index_id_ref() = indexID;
  item.index_name_ref() = folly::stringPrintf("index_%d", indexID);
  nebula::cpp2::SchemaID schemaID;
  schemaID.edge_type_ref() = edgeType;
  item.schema_id_ref() = schemaID;
  item.schema_name_ref() = folly::stringPrintf("edge_%d", edgeType);
  item.fields_ref() = std::move(fields);
  std::shared_ptr<IndexItem> itemPtr = std::make_shared<IndexItem>(item);

  auto iter = edgeIndexes_.find(space);
  if (iter == edgeIndexes_.end()) {
    std::vector<std::shared_ptr<IndexItem>> items{itemPtr};
    edgeIndexes_.emplace(space, items);
  } else {
    iter->second.emplace_back(std::move(itemPtr));
  }
}

void AdHocIndexManager::addEdgeAnnIndex(GraphSpaceID space,
                                        const std::vector<EdgeType> &edgeTypes,
                                        IndexID indexID,
                                        nebula::meta::cpp2::ColumnDef &&field) {
  folly::RWSpinLock::WriteHolder wh(tagIndexLock_);
  AnnIndexItem item;
  item.index_id_ref() = indexID;
  item.index_name_ref() = folly::stringPrintf("index_%d", indexID);
  item.prop_name_ref() = "vec";
  std::vector<nebula::cpp2::SchemaID> schemaIDs;
  for (const auto &edgeType : edgeTypes) {
    nebula::cpp2::SchemaID schemaID;
    schemaID.edge_type_ref() = edgeType;
    schemaIDs.emplace_back(schemaID);
  }
  item.schema_ids_ref() = std::move(schemaIDs);
  std::vector<std::string> edgeNames;
  for (const auto &edgeType : edgeTypes) {
    edgeNames.emplace_back(folly::stringPrintf("edge_%d", edgeType));
  }
  item.schema_names_ref() = std::move(edgeNames);
  std::vector<nebula::meta::cpp2::ColumnDef> fields;
  fields.emplace_back(std::move(field));
  item.fields_ref() = std::move(fields);
  item.ann_params_ref() = std::vector<std::string>{"IVF", "3", "l2", "3", "3"};
  std::shared_ptr<AnnIndexItem> itemPtr = std::make_shared<AnnIndexItem>(item);

  // Convert schema_ids to strings for logging
  LOG(ERROR) << "AnnIndexItem: "
             << "index_id=" << indexID << ", index_name=" << item.get_index_name()
             << ", schema_names=" << folly::join(",", *item.schema_names_ref())
             << ", ann_params=" << folly::join(",", *item.ann_params_ref());
  auto iter = edgeAnnIndexes_.find(space);
  if (iter == edgeAnnIndexes_.end()) {
    std::vector<std::shared_ptr<AnnIndexItem>> items{itemPtr};
    edgeAnnIndexes_.emplace(space, std::move(items));
  } else {
    iter->second.emplace_back(std::move(itemPtr));
  }
}

StatusOr<std::shared_ptr<IndexItem>> AdHocIndexManager::getTagIndex(GraphSpaceID space,
                                                                    IndexID index) {
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

StatusOr<std::shared_ptr<IndexItem>> AdHocIndexManager::getEdgeIndex(GraphSpaceID space,
                                                                     IndexID index) {
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

StatusOr<std::vector<std::shared_ptr<IndexItem>>> AdHocIndexManager::getTagIndexes(
    GraphSpaceID space) {
  folly::RWSpinLock::ReadHolder rh(tagIndexLock_);
  auto iter = tagIndexes_.find(space);
  if (iter == tagIndexes_.end()) {
    return Status::SpaceNotFound();
  }
  return iter->second;
}

StatusOr<std::vector<std::shared_ptr<IndexItem>>> AdHocIndexManager::getEdgeIndexes(
    GraphSpaceID space) {
  folly::RWSpinLock::ReadHolder rh(edgeIndexLock_);
  auto iter = edgeIndexes_.find(space);
  if (iter == edgeIndexes_.end()) {
    return Status::SpaceNotFound();
  }
  return iter->second;
}

StatusOr<IndexID> AdHocIndexManager::toTagIndexID(GraphSpaceID space, std::string indexName) {
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

StatusOr<IndexID> AdHocIndexManager::toEdgeIndexID(GraphSpaceID space, std::string indexName) {
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

StatusOr<std::shared_ptr<AnnIndexItem>> AdHocIndexManager::getTagAnnIndex(GraphSpaceID space,
                                                                          IndexID index) {
  folly::RWSpinLock::ReadHolder rh(tagIndexLock_);
  auto iter = tagAnnIndexes_.find(space);
  if (iter == tagAnnIndexes_.end()) {
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

StatusOr<std::shared_ptr<AnnIndexItem>> AdHocIndexManager::getEdgeAnnIndex(GraphSpaceID space,
                                                                           IndexID index) {
  // For now, return IndexNotFound as edge ann indexes are not implemented yet
  folly::RWSpinLock::ReadHolder rh(edgeIndexLock_);
  auto iter = edgeAnnIndexes_.find(space);
  if (iter == edgeAnnIndexes_.end()) {
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

}  // namespace mock
}  // namespace nebula
