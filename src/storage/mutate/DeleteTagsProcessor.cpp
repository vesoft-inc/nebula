/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/mutate/DeleteTagsProcessor.h"

#include "common/stats/StatsManager.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "common/utils/OperationKeyUtils.h"
#include "storage/StorageFlags.h"
#include "storage/stats/StorageStats.h"

namespace nebula {
namespace storage {

ProcessorCounters kDelTagsCounters;

void DeleteTagsProcessor::process(const cpp2::DeleteTagsRequest& req) {
  spaceId_ = req.get_space_id();
  const auto& parts = req.get_parts();

  CHECK_NOTNULL(env_->schemaMan_);
  auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
  if (!ret.ok()) {
    LOG(ERROR) << ret.status();
    for (auto& part : parts) {
      pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
    }
    onFinished();
    return;
  }
  spaceVidLen_ = ret.value();
  callingNum_ = parts.size();

  CHECK_NOTNULL(env_->indexMan_);
  auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
  if (!iRet.ok()) {
    LOG(ERROR) << iRet.status();
    for (auto& part : parts) {
      pushResultCode(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, part.first);
    }
    onFinished();
    return;
  }
  indexes_ = std::move(iRet).value();

  CHECK_NOTNULL(env_->kvstore_);
  if (indexes_.empty()) {
    std::vector<std::string> keys;
    keys.reserve(32);
    for (const auto& part : parts) {
      auto partId = part.first;
      const auto& delTags = part.second;
      keys.clear();
      for (const auto& entry : delTags) {
        const auto& vId = entry.get_id().getStr();
        for (const auto& tagId : entry.get_tags()) {
          auto key = NebulaKeyUtils::tagKey(spaceVidLen_, partId, vId, tagId);
          keys.emplace_back(std::move(key));
        }
      }
      doRemove(spaceId_, partId, std::move(keys));
      stats::StatsManager::addValue(kNumTagsDeleted, keys.size());
    }
  } else {
    for (const auto& part : parts) {
      IndexCountWrapper wrapper(env_);
      auto partId = part.first;
      std::vector<VMLI> lockedKeys;
      auto batch = deleteTags(partId, part.second, lockedKeys);
      if (!nebula::ok(batch)) {
        env_->verticesML_->unlockBatch(lockedKeys);
        handleAsync(spaceId_, partId, nebula::error(batch));
        continue;
      }
      // keys has been locked in deleteTags
      nebula::MemoryLockGuard<VMLI> lg(
          env_->verticesML_.get(), std::move(lockedKeys), false, false);
      env_->kvstore_->asyncAppendBatch(spaceId_,
                                       partId,
                                       std::move(nebula::value(batch)),
                                       [l = std::move(lg), icw = std::move(wrapper), partId, this](
                                           nebula::cpp2::ErrorCode code) {
                                         UNUSED(l);
                                         UNUSED(icw);
                                         handleAsync(spaceId_, partId, code);
                                       });
    }
  }
}

ErrorOr<nebula::cpp2::ErrorCode, std::string> DeleteTagsProcessor::deleteTags(
    PartitionID partId, const std::vector<cpp2::DelTags>& delTags, std::vector<VMLI>& lockedKeys) {
  std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
  for (const auto& entry : delTags) {
    const auto& vId = entry.get_id().getStr();
    for (const auto& tagId : entry.get_tags()) {
      auto key = NebulaKeyUtils::tagKey(spaceVidLen_, partId, vId, tagId);
      auto tup = std::make_tuple(spaceId_, partId, tagId, vId);
      // ignore if there are duplicate delete
      if (std::find(lockedKeys.begin(), lockedKeys.end(), tup) != lockedKeys.end()) {
        continue;
      }
      if (!env_->verticesML_->try_lock(tup)) {
        LOG(WARNING) << folly::sformat("The vertex locked : vId {}, tagId {}", vId, tagId);
        return nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
      }

      std::string val;
      auto code = env_->kvstore_->get(spaceId_, partId, key, &val);
      lockedKeys.emplace_back(std::move(tup));
      if (code == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        continue;
      } else if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return code;
      }

      auto reader = RowReaderWrapper::getTagPropReader(env_->schemaMan_, spaceId_, tagId, val);
      if (reader == nullptr) {
        return nebula::cpp2::ErrorCode::E_INVALID_DATA;
      }
      for (auto& index : indexes_) {
        if (index->get_schema_id().get_tag_id() == tagId) {
          auto indexId = index->get_index_id();

          auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(), index.get());
          if (!valuesRet.ok()) {
            return nebula::cpp2::ErrorCode::E_INVALID_DATA;
          }
          auto indexKeys = IndexKeyUtils::vertexIndexKeys(
              spaceVidLen_, partId, indexId, vId, std::move(valuesRet).value());

          // Check the index is building for the specified partition or not
          auto indexState = env_->getIndexState(spaceId_, partId);
          if (env_->checkRebuilding(indexState)) {
            auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
            for (auto& indexKey : indexKeys) {
              batchHolder->put(std::string(deleteOpKey), std::move(indexKey));
            }
          } else if (env_->checkIndexLocked(indexState)) {
            return nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
          } else {
            for (auto& indexKey : indexKeys) {
              batchHolder->remove(std::move(indexKey));
            }
          }
        }
      }
      batchHolder->remove(std::move(key));
      stats::StatsManager::addValue(kNumTagsDeleted);
    }
  }
  return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula
