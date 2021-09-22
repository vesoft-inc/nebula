/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/RebuildTagIndexTask.h"

#include "codec/RowReaderWrapper.h"
#include "common/utils/IndexKeyUtils.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

const int32_t kReserveNum = 1024 * 4;

StatusOr<IndexItems> RebuildTagIndexTask::getIndexes(GraphSpaceID space) {
  return env_->indexMan_->getTagIndexes(space);
}

StatusOr<std::shared_ptr<meta::cpp2::IndexItem>> RebuildTagIndexTask::getIndex(GraphSpaceID space,
                                                                               IndexID index) {
  return env_->indexMan_->getTagIndex(space, index);
}

nebula::cpp2::ErrorCode RebuildTagIndexTask::buildIndexGlobal(GraphSpaceID space,
                                                              PartitionID part,
                                                              const IndexItems& items,
                                                              kvstore::RateLimiter* rateLimiter) {
  if (canceled_) {
    LOG(ERROR) << "Rebuild Tag Index is Canceled";
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  auto vidSizeRet = env_->schemaMan_->getSpaceVidLen(space);
  if (!vidSizeRet.ok()) {
    LOG(ERROR) << "Get VID Size Failed";
    return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
  }

  std::unordered_set<TagID> tagIds;
  for (const auto& item : items) {
    tagIds.emplace(item->get_schema_id().get_tag_id());
  }

  auto schemasRet = env_->schemaMan_->getAllLatestVerTagSchema(space);
  if (!schemasRet.ok()) {
    LOG(ERROR) << "Get space tag schema failed";
    return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
  }
  auto schemas = schemasRet.value();

  auto vidSize = vidSizeRet.value();
  std::unique_ptr<kvstore::KVIterator> iter;
  auto prefix = NebulaKeyUtils::vertexPrefix(part);
  auto ret = env_->kvstore_->prefix(space, part, prefix, &iter);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Processing Part " << part << " Failed";
    return ret;
  }

  std::vector<kvstore::KV> data;
  data.reserve(kReserveNum);
  RowReaderWrapper reader;
  size_t batchSize = 0;
  while (iter && iter->valid()) {
    if (canceled_) {
      LOG(ERROR) << "Rebuild Tag Index is Canceled";
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    if (batchSize >= FLAGS_rebuild_index_batch_size) {
      auto result = writeData(space, part, std::move(data), batchSize, rateLimiter);
      if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Write Part " << part << " Index Failed";
        return result;
      }
      data.clear();
      batchSize = 0;
    }

    auto key = iter->key();
    auto val = iter->val();

    auto tagID = NebulaKeyUtils::getTagId(vidSize, key);

    // Check whether this record contains the index of indexId
    if (tagIds.find(tagID) == tagIds.end()) {
      VLOG(3) << "This record is not built index.";
      iter->next();
      continue;
    }

    auto vertex = NebulaKeyUtils::getVertexId(vidSize, key);
    VLOG(3) << "Tag ID " << tagID << " Vertex ID " << vertex;

    reader = RowReaderWrapper::getTagPropReader(env_->schemaMan_, space, tagID, val);
    if (reader == nullptr) {
      iter->next();
      continue;
    }

    auto schemaIter = schemas.find(tagID);
    if (schemaIter == schemas.end()) {
      LOG(WARNING) << "Space " << space << ", tag " << tagID << " invalid";
      iter->next();
      continue;
    }
    auto* schema = schemaIter->second.get();

    auto ttlProp = CommonUtils::ttlProps(schema);
    if (ttlProp.first && CommonUtils::checkDataExpiredForTTL(
                             schema, reader.get(), ttlProp.second.second, ttlProp.second.first)) {
      VLOG(3) << "ttl expired : "
              << "Tag ID " << tagID << " Vertex ID " << vertex;
      iter->next();
      continue;
    }

    std::string indexVal = "";
    if (ttlProp.first) {
      auto ttlValRet = CommonUtils::ttlValue(schema, reader.get());
      if (ttlValRet.ok()) {
        indexVal = IndexKeyUtils::indexVal(std::move(ttlValRet).value());
      }
    }

    for (const auto& item : items) {
      if (item->get_schema_id().get_tag_id() == tagID) {
        auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(), item->get_fields());
        if (!valuesRet.ok()) {
          LOG(WARNING) << "Collect index value failed";
          continue;
        }
        auto indexKey = IndexKeyUtils::vertexIndexKey(
            vidSize, part, item->get_index_id(), vertex.toString(), std::move(valuesRet).value());
        batchSize += indexKey.size() + indexVal.size();
        data.emplace_back(std::move(indexKey), indexVal);
      }
    }
    iter->next();
  }

  auto result = writeData(space, part, std::move(data), batchSize, rateLimiter);
  if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Write Part " << part << " Index Failed";
    return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
