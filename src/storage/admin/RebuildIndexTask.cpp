/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/Common.h"
#include "storage/StorageFlags.h"
#include "storage/admin/RebuildIndexTask.h"
#include "utils/OperationKeyUtils.h"

namespace nebula {
namespace storage {

ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>>
RebuildIndexTask::genSubTasks() {
    CHECK_NOTNULL(env_->kvstore_);
    space_ = ctx_.parameters_.space_id;
    auto parts = ctx_.parameters_.parts;

    auto itemsRet = getIndexes(space_);
    if (!itemsRet.ok()) {
        LOG(ERROR) << "Indexes Not Found";
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }

    auto items = itemsRet.value();
    if (items.empty()) {
        LOG(INFO) << "Index is empty";
        return cpp2::ErrorCode::SUCCEEDED;
    }

    std::vector<AdminSubTask> tasks;
    for (auto it = env_->rebuildIndexGuard_->cbegin();
         it != env_->rebuildIndexGuard_->cend(); ++it) {
        if (std::get<0>(it->first) == space_ && it->second != IndexState::FINISHED) {
            LOG(ERROR) << "This space is building index";
            return cpp2::ErrorCode::E_REBUILD_INDEX_FAILED;
        }
    }

    for (const auto& part : parts) {
        env_->rebuildIndexGuard_->insert_or_assign(std::make_tuple(space_, part),
                                                   IndexState::STARTING);
        std::function<kvstore::ResultCode()> task =
            std::bind(&RebuildIndexTask::invoke, this, space_, part, items);
        tasks.emplace_back(std::move(task));
    }
    return tasks;
}

kvstore::ResultCode RebuildIndexTask::invoke(GraphSpaceID space,
                                             PartitionID part,
                                             const IndexItems& items) {
    auto result = removeLegacyLogs(space, part);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Remove legacy logs at part: " << part << " failed";
        return kvstore::ResultCode::ERR_BUILD_INDEX_FAILED;
    } else {
        LOG(INFO) << "Remove legacy logs at part: " << part << " successful";
    }

    // todo(doodle): this place has potential bug is that we'd better lock the part at first,
    // then switch to BUILDING, otherwise some data won't build index in worst case.
    env_->rebuildIndexGuard_->assign(std::make_tuple(space, part), IndexState::BUILDING);

    LOG(INFO) << "Start building index";
    result = buildIndexGlobal(space, part, items);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Building index failed";
        return kvstore::ResultCode::ERR_BUILD_INDEX_FAILED;
    } else {
        LOG(INFO) << "Building index successful";
    }

    LOG(INFO) << "Processing operation logs";
    result = buildIndexOnOperations(space, part);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Building index with operation logs failed";
        return kvstore::ResultCode::ERR_INVALID_OPERATION;
    }

    env_->rebuildIndexGuard_->assign(std::make_tuple(space, part), IndexState::FINISHED);
    LOG(INFO) << "RebuildIndexTask Finished";
    return result;
}

kvstore::ResultCode RebuildIndexTask::buildIndexOnOperations(GraphSpaceID space,
                                                             PartitionID part) {
    if (canceled_) {
        LOG(ERROR) << "Rebuild index canceled";
        return kvstore::ResultCode::SUCCEEDED;
    }

    while (true) {
        std::unique_ptr<kvstore::KVIterator> operationIter;
        auto operationPrefix = OperationKeyUtils::operationPrefix(part);
        auto operationRet = env_->kvstore_->prefix(space,
                                                   part,
                                                   operationPrefix,
                                                   &operationIter);
        if (operationRet != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Processing Part " << part << " Failed";
            return operationRet;
        }

        std::vector<std::string> operations;
        operations.reserve(FLAGS_rebuild_index_batch_num);
        while (operationIter->valid()) {
            auto opKey = operationIter->key();
            auto opVal = operationIter->val();
            // replay operation record
            if (OperationKeyUtils::isModifyOperation(opKey)) {
                VLOG(3) << "Processing Modify Operation " << opKey;
                auto key = OperationKeyUtils::getOperationKey(opKey);
                std::vector<kvstore::KV> pairs;
                pairs.emplace_back(std::move(key), "");
                auto ret = writeData(space, part, std::move(pairs));
                if (kvstore::ResultCode::SUCCEEDED != ret) {
                    LOG(ERROR) << "Modify Playback Failed";
                    return ret;
                }
            } else if (OperationKeyUtils::isDeleteOperation(opKey)) {
                VLOG(3) << "Processing Delete Operation " << opVal;
                auto ret = removeData(space, part, opVal.str());
                if (kvstore::ResultCode::SUCCEEDED != ret) {
                    LOG(ERROR) << "Delete Playback Failed";
                    return ret;
                }
            } else {
                LOG(ERROR) << "Unknow Operation Type";
                return kvstore::ResultCode::ERR_INVALID_OPERATION;
            }

            operations.emplace_back(opKey);
            if (static_cast<int32_t>(operations.size()) == FLAGS_rebuild_index_batch_num) {
                auto ret = cleanupOperationLogs(space, part, operations);
                if (kvstore::ResultCode::SUCCEEDED != ret) {
                    LOG(ERROR) << "Delete Operation Failed";
                    return ret;
                }

                operations.clear();
            }
            operationIter->next();
        }

        auto ret = cleanupOperationLogs(space, part, operations);
        if (kvstore::ResultCode::SUCCEEDED != ret) {
            LOG(ERROR) << "Cleanup Operation Failed";
            return ret;
        }

        // When the processed operation number is less than the locked threshold,
        // we will mark the lock's building in StorageEnv and refuse writing for
        // a short piece of time.
        if (static_cast<int32_t>(operations.size()) < FLAGS_rebuild_index_locked_threshold) {
            // lock the part
            auto key = std::make_tuple(space, part);
            auto stateIter = env_->rebuildIndexGuard_->find(key);
            // If the state is LOCKED, we should wait the on flying request process finished.
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
    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode
RebuildIndexTask::removeLegacyLogs(GraphSpaceID space,
                                   PartitionID part) {
    std::unique_ptr<kvstore::KVIterator> operationIter;
    auto operationPrefix = OperationKeyUtils::operationPrefix(part);
    auto operationRet = env_->kvstore_->prefix(space,
                                               part,
                                               operationPrefix,
                                               &operationIter);
    if (operationRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Remove Legacy Log Failed";
        return operationRet;
    }

    std::vector<std::string> operations;
    operations.reserve(FLAGS_rebuild_index_batch_num);
    while (operationIter->valid()) {
        auto opKey = operationIter->key();
        operations.emplace_back(opKey);
        if (static_cast<int32_t>(operations.size()) == FLAGS_rebuild_index_batch_num) {
            auto ret = cleanupOperationLogs(space, part, operations);
            if (kvstore::ResultCode::SUCCEEDED != ret) {
                LOG(ERROR) << "Delete Operation Failed";
                return ret;
            }

            operations.clear();
        }
        operationIter->next();
    }

    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode
RebuildIndexTask::writeData(GraphSpaceID space,
                            PartitionID part,
                            std::vector<kvstore::KV> data) {
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode result = kvstore::ResultCode::SUCCEEDED;
    env_->kvstore_->asyncMultiPut(space, part, std::move(data),
                                  [&result, &baton](kvstore::ResultCode code) {
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Modify the index failed";
            result = code;
        }
        baton.post();
    });
    baton.wait();
    return result;
}

kvstore::ResultCode
RebuildIndexTask::removeData(GraphSpaceID space,
                             PartitionID part,
                             std::string&& key) {
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode result = kvstore::ResultCode::SUCCEEDED;
    env_->kvstore_->asyncRemove(space, part, std::move(key),
                                [&result, &baton](kvstore::ResultCode code) {
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Remove the index failed";
            result = code;
        }
        baton.post();
    });
    baton.wait();
    return result;
}

kvstore::ResultCode
RebuildIndexTask::cleanupOperationLogs(GraphSpaceID space,
                                       PartitionID part,
                                       std::vector<std::string> keys) {
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode result = kvstore::ResultCode::SUCCEEDED;
    env_->kvstore_->asyncMultiRemove(space, part, std::move(keys),
                                     [&result, &baton](kvstore::ResultCode code) {
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Cleanup the operation log failed";
            result = code;
        }
        baton.post();
    });
    baton.wait();
    return result;
}

}  // namespace storage
}  // namespace nebula
