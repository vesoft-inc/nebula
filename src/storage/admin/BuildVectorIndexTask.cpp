/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/BuildVectorIndexTask.h"

#include "common/base/Base.h"
#include "common/utils/OperationKeyUtils.h"
#include "kvstore/Common.h"
#include "storage/StorageFlags.h"
#include "storage/VectorIndexManager.h"

namespace nebula {
namespace storage {

bool BuildVectorIndexTask::check() {
  return env_->kvstore_ != nullptr;
}

void BuildVectorIndexTask::finish(nebula::cpp2::ErrorCode rc) {
  LOG(INFO) << "Finish rebuild index task in space " << *ctx_.parameters_.space_id_ref();
  if (changedSpaceGuard_) {
    auto space = *ctx_.parameters_.space_id_ref();
    for (auto it = env_->rebuildIndexGuard_->begin(); it != env_->rebuildIndexGuard_->end(); ++it) {
      if (std::get<0>(it->first) == space) {
        env_->rebuildIndexGuard_->insert_or_assign(it->first, IndexState::FINISHED);
      }
    }
  }
  AdminTask::finish(rc);
}

BuildVectorIndexTask::BuildVectorIndexTask(StorageEnv* env, TaskContext&& ctx)
    : AdminTask(env, std::move(ctx)) {
  // Build Vector Index rate is limited to FLAGS_rebuild_index_part_rate_limit * SubTaskConcurrency.
  // As for default configuration in a 3 replica cluster, send rate is 512Kb for a partition. From a
  // global perspective, the leaders are distributed evenly, so both send and recv traffic will be
  // 1Mb (512 * 2 peers). Multiplied by the subtasks concurrency, the total send/recv traffic will
  // be 10Mb, which is non-trival.
  LOG(INFO) << "Build Vector Index task is rate limited to " << FLAGS_rebuild_index_part_rate_limit
            << " for each subtask by default";
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> BuildVectorIndexTask::genSubTasks() {
  space_ = *ctx_.parameters_.space_id_ref();
  auto parts = *ctx_.parameters_.parts_ref();
  CHECK(ctx_.parameters_.task_specific_paras_ref().has_value());
  auto index = (*ctx_.parameters_.task_specific_paras_ref())[0];
  LOG(ERROR) << "Ann Index id: " << index;
  auto indexID = folly::to<IndexID>(index);
  auto indexRet = getIndex(space_, indexID);
  if (!indexRet.ok()) {
    LOG(INFO) << "Index not found: " << indexID;
    return nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
  }
  auto indexItem = indexRet.value();

  std::vector<AdminSubTask> tasks;
  for (auto it = env_->rebuildIndexGuard_->cbegin(); it != env_->rebuildIndexGuard_->cend(); ++it) {
    if (std::get<0>(it->first) == space_ && it->second != IndexState::FINISHED) {
      LOG(INFO) << "This space " << space_ << " is building index on part "
                << std::get<1>(it->first) << ", index status: " << static_cast<int32_t>(it->second);
      return nebula::cpp2::ErrorCode::E_REBUILD_INDEX_FAILED;
    }
  }

  for (const auto& part : parts) {
    env_->rebuildIndexGuard_->insert_or_assign(std::make_tuple(space_, part), IndexState::STARTING);
    TaskFunction task = [this, part, indexItem] { return invoke(space_, part, indexItem); };
    changedSpaceGuard_ = true;
    tasks.emplace_back(std::move(task));
  }
  return tasks;
}

nebula::cpp2::ErrorCode BuildVectorIndexTask::invoke(GraphSpaceID space,
                                                     PartitionID part,
                                                     const std::shared_ptr<AnnIndexItem>& item) {
  LOG(INFO) << "Start building vector index for space " << space << ", part " << part;
  // auto rateLimiter = std::make_unique<kvstore::RateLimiter>();
  // Create or rebuild vector index for this partition
  auto indexId = item->get_index_id();
  // Call the specific implementation for scanning vector data
  auto result = buildIndexGlobal(space, part, item, nullptr);
  if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Failed to scan vector data for index " << indexId << " in partition " << part;
    return result;
  }

  LOG(INFO) << folly::sformat(
      "BuildVectorIndexTask Finished, space={}, part={}, index={}", space, part, indexId);
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode BuildVectorIndexTask::writeData(GraphSpaceID space,
                                                        PartitionID part,
                                                        std::vector<kvstore::KV> data,
                                                        size_t batchSize,
                                                        kvstore::RateLimiter* rateLimiter) {
  folly::Baton<true, std::atomic> baton;
  auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
  UNUSED(batchSize);
  UNUSED(rateLimiter);
  // rateLimiter->consume(static_cast<double>(batchSize),                             // toConsume
  //                      static_cast<double>(FLAGS_rebuild_index_part_rate_limit),   // rate
  //                      static_cast<double>(FLAGS_rebuild_index_part_rate_limit));  // burstSize
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
}  // namespace storage
}  // namespace nebula
