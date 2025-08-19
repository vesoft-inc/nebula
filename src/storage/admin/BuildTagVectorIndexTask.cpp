/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/BuildTagVectorIndexTask.h"

#include <folly/String.h>

#include "codec/RowReaderWrapper.h"
#include "common/base/Status.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "common/vectorIndex/VectorIndexUtils.h"
#include "interface/gen-cpp2/meta_types.h"
#include "storage/StorageFlags.h"
#include "storage/VectorIndexManager.h"

namespace nebula {
namespace storage {

const int32_t kReserveNum = 1024 * 4;

StatusOr<std::shared_ptr<AnnIndexItem>> BuildTagVectorIndexTask::getIndex(GraphSpaceID space,
                                                                          IndexID index) {
  // 使用可配置的重试参数
  const int maxRetries = FLAGS_vector_index_cache_retry_times;
  const int retryIntervalMs = FLAGS_vector_index_cache_retry_interval_ms;

  for (int retry = 0; retry < maxRetries; ++retry) {
    auto indexRet = env_->indexMan_->getTagAnnIndex(space, index);
    if (indexRet.ok()) {
      return indexRet.value();
    }

    // 如果是IndexNotFound且不是最后一次重试，则刷新缓存并重试
    if (indexRet.status().code() == Status::Code::kIndexNotFound && retry < maxRetries - 1) {
      LOG(ERROR) << "Index " << index << " not found in cache, refreshing meta cache. Retry "
                 << (retry + 1) << "/" << maxRetries;

      // 强制刷新meta client缓存
      if (auto* metaClient = env_->metaClient_) {
        auto refreshStatus = metaClient->refreshCache();
        if (!refreshStatus.ok()) {
          LOG(WARNING) << "Failed to refresh meta cache: " << refreshStatus;
        }
      }

      // 等待一段时间再重试
      std::this_thread::sleep_for(std::chrono::milliseconds(retryIntervalMs));
      continue;
    }

    // 最后一次重试：尝试直接从Meta服务获取所有Ann索引，然后查找目标索引
    if (indexRet.status().code() == Status::Code::kIndexNotFound && retry == maxRetries - 1) {
      LOG(ERROR) << "Last retry: attempting to fetch index directly from meta service";

      if (auto* metaClient = env_->metaClient_) {
        auto allIndexesRet = metaClient->listTagAnnIndexes(space).get();
        if (allIndexesRet.ok()) {
          for (const auto& item : allIndexesRet.value()) {
            if (item.get_index_id() == index) {
              LOG(INFO) << "Found index " << index << " directly from meta service";
              auto sharedItem = std::make_shared<meta::cpp2::AnnIndexItem>(item);
              return sharedItem;
            }
          }
        } else {
          LOG(WARNING) << "Failed to fetch indexes from meta service: " << allIndexesRet.status();
        }
      }
    }

    // 其他错误直接返回
    return Status::Error("Get Tag Ann Index Failed: %s", indexRet.status().toString().c_str());
  }

  return Status::Error("Get Tag Ann Index Failed after %d retries: IndexNotFound", maxRetries);
}

nebula::cpp2::ErrorCode BuildTagVectorIndexTask::buildIndexGlobal(
    GraphSpaceID space,
    PartitionID part,
    const std::shared_ptr<AnnIndexItem>& item,
    kvstore::RateLimiter* rateLimiter) {
  if (UNLIKELY(canceled_)) {
    LOG(INFO) << "Build Tag Ann Index is Canceled";
    return nebula::cpp2::ErrorCode::E_USER_CANCEL;
  }

  auto vidSizeRet = env_->schemaMan_->getSpaceVidLen(space);
  if (!vidSizeRet.ok()) {
    LOG(INFO) << "Get VID Size Failed";
    return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
  }

  auto vidTypeRet = env_->schemaMan_->getSpaceVidType(space);
  if (!vidTypeRet.ok()) {
    LOG(INFO) << "Get VID Type Failed";
    return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
  }

  std::unordered_set<TagID> tagIds;
  for (const auto& schema : item->get_schema_ids()) {
    tagIds.emplace(schema.get_tag_id());
  }

  auto schemasRet = env_->schemaMan_->getAllLatestVerTagSchema(space);
  if (!schemasRet.ok()) {
    LOG(INFO) << "Get space tag schema failed";
    return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
  }
  auto schemas = schemasRet.value();

  auto vidSize = vidSizeRet.value();
  std::unique_ptr<kvstore::KVIterator> iter;
  auto prefix = NebulaKeyUtils::vectorTagPrefix(part);
  auto ret = env_->kvstore_->prefix(space, part, prefix, &iter);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Processing Part " << part << " Failed";
    return ret;
  }

  std::vector<float> data;
  std::vector<VectorID> vectorIds;
  std::vector<kvstore::KV> vidData;

  data.reserve(kReserveNum);
  vectorIds.reserve(kReserveNum);
  vidData.reserve(kReserveNum);
  RowReaderWrapper reader;
  size_t batchSize = 0;
  IndexID indexId = item->get_index_id();
  auto propName = item->get_prop_name();
  auto schemaIds = item->get_schema_ids();
  auto dim = (*item->get_ann_params())[1];

  while (iter && iter->valid()) {
    if (UNLIKELY(canceled_)) {
      LOG(INFO) << "Build Tag Ann Index is Canceled";
      return nebula::cpp2::ErrorCode::E_USER_CANCEL;
    }

    // if (batchSize >= FLAGS_rebuild_index_batch_size) {
    //   LOG(ERROR) << "Write id vid data";
    //   auto result = writeData(space, part, std::move(vidData), batchSize, rateLimiter);
    //   if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    //     LOG(ERROR) << "Write Part " << part << " Index Failed";
    //     return result;
    //   }
    //   VecData vecData;
    //   vecData.fdata = data.data();
    //   vecData.ids = vectorIds.data();
    //   vecData.cnt = static_cast<int32_t>(vectorIds.size());
    //   vecData.dim = folly::to<size_t>(dim);
    //   LOG(ERROR) << "Vec Data: "
    //              << "size=" << vecData.cnt << ", dim=" << vecData.dim;
    //   result = buildAnnIndex(space, part, item, vecData);
    //   if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    //     LOG(ERROR) << "Build Ann Index Part " << part << " Index Failed";
    //     return result;
    //   }
    //   data.clear();
    //   vectorIds.clear();
    //   vidData.clear();
    //   batchSize = 0;
    // }

    auto key = iter->key();
    auto val = iter->val();

    auto tagID = NebulaKeyUtils::getVectorTagId(vidSize, key);

    // Check whether this record contains the index of indexId
    if (tagIds.find(tagID) == tagIds.end()) {
      VLOG(1) << "This record is not built index.";
      iter->next();
      continue;
    }

    auto vertex = NebulaKeyUtils::getVectorVertexId(vidSize, key);
    VLOG(1) << "Tag ID " << tagID << " Vertex ID " << vertex;
    VectorID vectorId = folly::hash::fnv64_buf(vertex.data(), vertex.size());
    auto schemaIter = schemas.find(tagID);
    if (schemaIter == schemas.end()) {
      LOG(WARNING) << "Space " << space << ", tag " << tagID << " invalid";
      iter->next();
      continue;
    }
    auto* schema = schemaIter->second.get();

    auto vecIndex = schema->getVectorFieldIndex(propName);

    reader = RowReaderWrapper::getTagPropReader(
        env_->schemaMan_, space, tagID, static_cast<int32_t>(vecIndex), val);

    for (auto& schemaId : schemaIds) {
      if (schemaId.get_tag_id() == tagID) {
        auto value = reader.getVectorValueByName(propName);
        if (value.isNull()) {
          continue;
        }
        if (!value.isVector()) {
          return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
        }
        auto vecObj = value.moveVector();
        batchSize += vecObj.dim() * sizeof(float) + sizeof(vectorId);
        const auto& vec = vecObj.data();
        data.insert(
            data.end(), std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end()));
        vectorIds.emplace_back(vectorId);
      }
    }

    auto idVidKey = NebulaKeyUtils::idVidTagKey(vidSize, part, indexId, vertex.toString());

    auto vidIdKey = NebulaKeyUtils::vidIdTagKey(part, indexId, vectorId);

    vidData.emplace_back(std::move(idVidKey), std::to_string(vectorId));
    batchSize += vidData.back().first.size() + sizeof(vectorId);
    vidData.emplace_back(std::move(vidIdKey), vertex.toString());
    batchSize += vidData.back().first.size() + sizeof(vertex);
    iter->next();
  }

  // write vid data to kvstore
  ret = writeData(space, part, std::move(vidData), batchSize, rateLimiter);
  // buildAnnIndex
  VecData vecData;
  vecData.fdata = data.data();
  vecData.ids = vectorIds.data();
  vecData.cnt = static_cast<int32_t>(vectorIds.size());
  vecData.dim = folly::to<size_t>(dim);
  return buildAnnIndex(space, part, item, vecData);
}

nebula::cpp2::ErrorCode BuildTagVectorIndexTask::buildAnnIndex(
    GraphSpaceID space,
    PartitionID part,
    const std::shared_ptr<AnnIndexItem>& item,
    const VecData& data) {
  if (data.cnt == 0) {
    LOG(ERROR) << "Part: " << part << ", No vectors to add to the index, skipping.";
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  // Build ANN index
  auto& vecIdxMgr = VectorIndexManager::getInstance();
  Status ret = Status::OK();
  IndexID indexId = item->get_index_id();

  if (!vecIdxMgr.getIndex(space, part, indexId).ok()) {
    ret = vecIdxMgr.createOrUpdateIndex(space, part, indexId, item);
    if (!ret.ok()) {
      LOG(ERROR) << "Failed to create or update ANN index: " << ret;
      return nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
  }
  // Add vectors to the index
  ret = vecIdxMgr.addVectors(space, part, indexId, data);
  if (!ret.ok()) {
    LOG(ERROR) << "Part: " << part << ", Failed to add vectors to ANN index: " << ret;
    return nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
