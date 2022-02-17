/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/RebuildTagIndexTask.h"

#include <folly/Likely.h>  // for UNLIKELY
#include <folly/Range.h>   // for operator<<, Range
#include <stddef.h>        // for size_t
#include <stdint.h>        // for int32_t

#include <string>         // for string, basic_string
#include <type_traits>    // for remove_reference<>::type
#include <unordered_map>  // for operator==, _Node_iter...
#include <unordered_set>  // for unordered_set, unorder...
#include <vector>         // for vector

#include "codec/RowReaderWrapper.h"            // for RowReaderWrapper
#include "common/base/Logging.h"               // for LogMessage, LOG, _LOG_...
#include "common/meta/IndexManager.h"          // for IndexManager
#include "common/meta/NebulaSchemaProvider.h"  // for NebulaSchemaProvider
#include "common/meta/SchemaManager.h"         // for SchemaManager
#include "common/utils/IndexKeyUtils.h"        // for IndexKeyUtils
#include "common/utils/NebulaKeyUtils.h"       // for NebulaKeyUtils
#include "interface/gen-cpp2/meta_types.h"     // for IndexItem
#include "kvstore/Common.h"                    // for KV
#include "kvstore/KVIterator.h"                // for KVIterator
#include "kvstore/KVStore.h"                   // for KVStore
#include "storage/CommonUtils.h"               // for StorageEnv, CommonUtils
#include "storage/StorageFlags.h"              // for FLAGS_rebuild_index_ba...

namespace nebula {
namespace kvstore {
class RateLimiter;

class RateLimiter;
}  // namespace kvstore

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
  if (UNLIKELY(canceled_)) {
    LOG(ERROR) << "Rebuild Tag Index is Canceled";
    return nebula::cpp2::ErrorCode::E_USER_CANCEL;
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
  auto prefix = NebulaKeyUtils::tagPrefix(part);
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
    if (UNLIKELY(canceled_)) {
      LOG(ERROR) << "Rebuild Tag Index is Canceled";
      return nebula::cpp2::ErrorCode::E_USER_CANCEL;
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
        auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(), item.get(), schema);
        if (!valuesRet.ok()) {
          LOG(WARNING) << "Collect index value failed";
          continue;
        }
        auto indexKeys = IndexKeyUtils::vertexIndexKeys(
            vidSize, part, item->get_index_id(), vertex.toString(), std::move(valuesRet).value());
        for (auto& indexKey : indexKeys) {
          batchSize += indexKey.size() + indexVal.size();
          data.emplace_back(std::move(indexKey), indexVal);
        }
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
