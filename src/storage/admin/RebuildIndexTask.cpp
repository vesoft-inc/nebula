/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/RebuildIndexTask.h"

#include "common/utils/OperationKeyUtils.h"
#include "kvstore/Common.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

const int32_t kReserveNum = 1024 * 4;

RebuildIndexTask::RebuildIndexTask(StorageEnv* env, TaskContext&& ctx)
    : AdminTask(env, std::move(ctx)) {
  // Rebuild index rate is limited to FLAGS_rebuild_index_part_rate_limit * SubTaskConcurrency. As
  // for default configuration in a 3 replica cluster, send rate is 512Kb for a partition. From a
  // global perspective, the leaders are distributed evenly, so both send and recv traffic will be
  // 1Mb (512 * 2 peers). Muliplied by the subtasks concurrency, the total send/recv traffic will be
  // 10Mb, which is non-trival.
  LOG(INFO) << "Rebuild index task is rate limited to " << FLAGS_rebuild_index_part_rate_limit
            << " for each subtask by default";
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> RebuildIndexTask::genSubTasks() {
  CHECK_NOTNULL(env_->kvstore_);
  space_ = *ctx_.parameters_.space_id_ref();
  auto parts = *ctx_.parameters_.parts_ref();

  IndexItems items;
  if (!ctx_.parameters_.task_specfic_paras_ref().has_value() ||
      (*ctx_.parameters_.task_specfic_paras_ref()).empty()) {
    auto itemsRet = getIndexes(space_);
    if (!itemsRet.ok()) {
      LOG(ERROR) << "Indexes not found";
      return nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }

    items = std::move(itemsRet).value();
  } else {
    for (const auto& index : *ctx_.parameters_.task_specfic_paras_ref()) {
      auto indexID = folly::to<IndexID>(index);
      auto indexRet = getIndex(space_, indexID);
      if (!indexRet.ok()) {
        LOG(ERROR) << "Index not found: " << indexID;
        return nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
      }
      items.emplace_back(indexRet.value());
    }
  }

  if (items.empty()) {
    LOG(ERROR) << "Index is empty";
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  std::vector<AdminSubTask> tasks;
  for (auto it = env_->rebuildIndexGuard_->cbegin(); it != env_->rebuildIndexGuard_->cend(); ++it) {
    if (std::get<0>(it->first) == space_ && it->second != IndexState::FINISHED) {
      LOG(ERROR) << "This space is building index";
      return nebula::cpp2::ErrorCode::E_REBUILD_INDEX_FAILED;
    }
  }

  for (const auto& part : parts) {
    env_->rebuildIndexGuard_->insert_or_assign(std::make_tuple(space_, part), IndexState::STARTING);
    std::function<nebula::cpp2::ErrorCode()> task =
        std::bind(&RebuildIndexTask::invoke, this, space_, part, items);
    tasks.emplace_back(std::move(task));
  }
  return tasks;
}

nebula::cpp2::ErrorCode RebuildIndexTask::invoke(GraphSpaceID space,
                                                 PartitionID part,
                                                 const IndexItems& items) {
  auto rateLimiter = std::make_unique<kvstore::RateLimiter>();
  // TaskMananger will make sure that there won't be cocurrent invoke of a given part
  auto result = removeLegacyLogs(space, part);
  if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Remove legacy logs at part: " << part << " failed";
    return nebula::cpp2::ErrorCode::E_REBUILD_INDEX_FAILED;
  } else {
    VLOG(1) << "Remove legacy logs at part: " << part << " successful";
  }

  // todo(doodle): this place has potential bug is that we'd better lock the
  // part at first, then switch to BUILDING, otherwise some data won't build
  // index in worst case.
  env_->rebuildIndexGuard_->assign(std::make_tuple(space, part), IndexState::BUILDING);

  LOG(INFO) << "Start building index";
  result = buildIndexGlobal(space, part, items, rateLimiter.get());
  if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Building index failed";
    return nebula::cpp2::ErrorCode::E_REBUILD_INDEX_FAILED;
  } else {
    LOG(INFO) << folly::sformat("Building index successful, space={}, part={}", space, part);
  }

  LOG(INFO) << folly::sformat("Processing operation logs, space={}, part={}", space, part);
  result = buildIndexOnOperations(space, part, rateLimiter.get());
  if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << folly::sformat(
        "Building index with operation logs failed, space={}, part={}", space, part);
    return nebula::cpp2::ErrorCode::E_INVALID_OPERATION;
  }

  env_->rebuildIndexGuard_->assign(std::make_tuple(space, part), IndexState::FINISHED);
  LOG(INFO) << folly::sformat("RebuildIndexTask Finished, space={}, part={}", space, part);
  return result;
}

nebula::cpp2::ErrorCode RebuildIndexTask::buildIndexOnOperations(
    GraphSpaceID space, PartitionID part, kvstore::RateLimiter* rateLimiter) {
  if (canceled_) {
    LOG(INFO) << folly::sformat("Rebuild index canceled, space={}, part={}", space, part);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  while (true) {
    std::unique_ptr<kvstore::KVIterator> operationIter;
    auto operationPrefix = OperationKeyUtils::operationPrefix(part);
    auto operationRet = env_->kvstore_->prefix(space, part, operationPrefix, &operationIter);
    if (operationRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Processing Part " << part << " Failed";
      return operationRet;
    }

    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    batchHolder->reserve(kReserveNum);
    while (operationIter->valid()) {
      auto opKey = operationIter->key();
      auto opVal = operationIter->val();
      // replay operation record
      if (OperationKeyUtils::isModifyOperation(opKey)) {
        VLOG(3) << "Processing Modify Operation " << opKey;
        auto key = OperationKeyUtils::getOperationKey(opKey);
        batchHolder->put(std::move(key), opVal.str());
      } else if (OperationKeyUtils::isDeleteOperation(opKey)) {
        VLOG(3) << "Processing Delete Operation " << opVal;
        batchHolder->remove(opVal.str());
      } else {
        LOG(ERROR) << "Unknow Operation Type";
        return nebula::cpp2::ErrorCode::E_INVALID_OPERATION;
      }

      batchHolder->remove(opKey.str());
      if (batchHolder->size() > FLAGS_rebuild_index_batch_size) {
        auto ret = writeOperation(space, part, batchHolder.get(), rateLimiter);
        if (nebula::cpp2::ErrorCode::SUCCEEDED != ret) {
          LOG(ERROR) << "Write Operation Failed";
          return ret;
        }
      }
      operationIter->next();
    }

    auto ret = writeOperation(space, part, batchHolder.get(), rateLimiter);
    if (nebula::cpp2::ErrorCode::SUCCEEDED != ret) {
      LOG(ERROR) << "Write Operation Failed";
      return ret;
    }

    // When the processed operation size is less than the batch size,
    // we will mark the lock's building in StorageEnv and refuse writing for
    // a short piece of time.
    if (batchHolder->size() <= FLAGS_rebuild_index_batch_size) {
      // lock the part
      auto key = std::make_tuple(space, part);
      auto stateIter = env_->rebuildIndexGuard_->find(key);
      // If the state is LOCKED, we should wait the on flying request process
      // finished.
      if (stateIter != env_->rebuildIndexGuard_->cend() &&
          stateIter->second == IndexState::BUILDING) {
        env_->rebuildIndexGuard_->assign(std::move(key), IndexState::LOCKED);

        // Waiting all of the on flying requests have finished.
        int32_t currentRequestNum;
        do {
          currentRequestNum = env_->onFlyingRequest_.load();
          VLOG(3) << "On Flying Request: " << currentRequestNum;
          usleep(100);
        } while (currentRequestNum != 0);
      } else {
        break;
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode RebuildIndexTask::removeLegacyLogs(GraphSpaceID space, PartitionID part) {
  auto operationPrefix = OperationKeyUtils::operationPrefix(part);
  folly::Baton<true, std::atomic> baton;
  auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
  env_->kvstore_->asyncRemoveRange(space,
                                   part,
                                   NebulaKeyUtils::firstKey(operationPrefix, sizeof(int64_t)),
                                   NebulaKeyUtils::lastKey(operationPrefix, sizeof(int64_t)),
                                   [&result, &baton](nebula::cpp2::ErrorCode code) {
                                     if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                                       LOG(ERROR) << "Modify the index failed";
                                       result = code;
                                     }
                                     baton.post();
                                   });
  baton.wait();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode RebuildIndexTask::writeData(GraphSpaceID space,
                                                    PartitionID part,
                                                    std::vector<kvstore::KV> data,
                                                    size_t batchSize,
                                                    kvstore::RateLimiter* rateLimiter) {
  folly::Baton<true, std::atomic> baton;
  auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
  rateLimiter->consume(static_cast<double>(batchSize),                             // toConsume
                       static_cast<double>(FLAGS_rebuild_index_part_rate_limit),   // rate
                       static_cast<double>(FLAGS_rebuild_index_part_rate_limit));  // burstSize
  env_->kvstore_->asyncMultiPut(
      space, part, std::move(data), [&result, &baton](nebula::cpp2::ErrorCode code) {
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
          result = code;
        }
        baton.post();
      });
  baton.wait();
  return result;
}

nebula::cpp2::ErrorCode RebuildIndexTask::writeOperation(GraphSpaceID space,
                                                         PartitionID part,
                                                         kvstore::BatchHolder* batchHolder,
                                                         kvstore::RateLimiter* rateLimiter) {
  folly::Baton<true, std::atomic> baton;
  auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
  auto encoded = encodeBatchValue(batchHolder->getBatch());
  rateLimiter->consume(static_cast<double>(batchHolder->size()),                   // toConsume
                       static_cast<double>(FLAGS_rebuild_index_part_rate_limit),   // rate
                       static_cast<double>(FLAGS_rebuild_index_part_rate_limit));  // burstSize
  env_->kvstore_->asyncAppendBatch(
      space, part, std::move(encoded), [&result, &baton](nebula::cpp2::ErrorCode code) {
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
          result = code;
        }
        baton.post();
      });
  baton.wait();
  return result;
}

}  // namespace storage
}  // namespace nebula
