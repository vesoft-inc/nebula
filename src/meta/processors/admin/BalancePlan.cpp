/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/BalancePlan.h"
#include <folly/synchronization/Baton.h>
#include "meta/processors/Common.h"

DEFINE_uint32(task_concurrency, 10, "The tasks number could be invoked simultaneously");

namespace nebula {
namespace meta {

const std::string kBalancePlanTable = "__b_plan__"; // NOLINT

void BalancePlan::dispatchTasks() {
    // Key -> spaceID + partID,  Val -> List of task index in tasks_;
    std::unordered_map<std::pair<GraphSpaceID, PartitionID>, std::vector<int32_t>> partTasks;
    int32_t index = 0;
    for (auto& task : tasks_) {
        partTasks[std::make_pair(task.spaceId_, task.partId_)].emplace_back(index++);
    }
    buckets_.resize(std::min(partTasks.size(), (size_t)FLAGS_task_concurrency));
    for (auto it = partTasks.begin(); it != partTasks.end(); it++) {
        size_t minNum = tasks_.size();
        int32_t i = 0, minIndex = 0;
        for (auto& bucket : buckets_) {
            if (bucket.size() < minNum) {
                minNum = bucket.size();
                minIndex = i;
            }
            i++;
        }
        for (auto taskIndex : it->second) {
            buckets_[minIndex].emplace_back(taskIndex);
        }
    }
}

void BalancePlan::invoke() {
    status_ = Status::IN_PROGRESS;
    dispatchTasks();
    for (size_t i = 0; i < buckets_.size(); i++) {
        for (size_t j = 0; j < buckets_[i].size(); j++) {
            auto taskIndex = buckets_[i][j];
            tasks_[taskIndex].onFinished_ = [this, i, j]() {
                bool finished = false;
                {
                    std::lock_guard<std::mutex> lg(lock_);
                    finishedTaskNum_++;
                    if (finishedTaskNum_ == tasks_.size()) {
                        finished = true;
                        if (status_ == Status::IN_PROGRESS) {
                            status_ = Status::SUCCEEDED;
                        }
                    }
                }
                if (finished) {
                    CHECK_EQ(j, this->buckets_[i].size() - 1);
                    saveInStore(true);
                    onFinished_();
                } else {
                    if (j + 1 < this->buckets_[i].size()) {
                        auto& task = this->tasks_[this->buckets_[i][j + 1]];
                        task.invoke();
                    }
                }
            };  // onFinished
            tasks_[taskIndex].onError_ = [this, i, j]() {
                bool finished = false;
                {
                    std::lock_guard<std::mutex> lg(lock_);
                    finishedTaskNum_++;
                    status_ = Status::FAILED;
                    if (finishedTaskNum_ == tasks_.size()) {
                        finished = true;
                    }
                }
                if (finished) {
                    CHECK_EQ(j, this->buckets_[i].size() - 1);
                    saveInStore(true);
                    onFinished_();
                } else {
                    if (j + 1 < this->buckets_[i].size()) {
                        auto& task = this->tasks_[this->buckets_[i][j + 1]];
                        task.invoke();
                    }
                }
            };  // onError
        }  // for (auto j = 0; j < buckets_[i].size(); j++)
    }  // for (auto i = 0; i < buckets_.size(); i++)

    for (auto& bucket : buckets_) {
        if (!bucket.empty()) {
            tasks_[bucket[0]].invoke();
        }
    }
    saveInStore(true);
}

bool BalancePlan::saveInStore(bool onlyPlan) {
    if (kv_) {
        std::vector<kvstore::KV> data;
        data.emplace_back(planKey(), planVal());
        if (!onlyPlan) {
            for (auto& task : tasks_) {
                data.emplace_back(task.taskKey(), task.taskVal());
            }
        }
        folly::Baton<true, std::atomic> baton;
        bool ret = false;
        kv_->asyncMultiPut(kDefaultSpaceId,
                           kDefaultPartId,
                           std::move(data),
                           [this, &baton, &ret] (kvstore::ResultCode code) {
            if (kvstore::ResultCode::SUCCEEDED == code) {
                ret = true;
            } else {
                LOG(ERROR) << "Can't write the kvstore, ret = " << static_cast<int32_t>(code);
            }
            baton.post();
        });
        baton.wait();
        return ret;
    }
    return true;
}

bool BalancePlan::recovery() {
    if (kv_) {
        const auto& prefix = BalanceTask::prefix(id_);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Can't access kvstore, ret = " << static_cast<int32_t>(ret);
            return false;
        }
        while (iter->valid()) {
            BalanceTask task;
            task.kv_ = kv_;
            task.client_ = client_;
            {
                auto tup = BalanceTask::parseKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                task.spaceId_ = std::get<1>(tup);
                task.partId_ = std::get<2>(tup);
                task.src_ = std::get<3>(tup);
                task.dst_ = std::get<4>(tup);
            }
            {
                auto tup = BalanceTask::parseVal(iter->val());
                task.status_ = std::get<0>(tup);
                task.ret_ = std::get<1>(tup);
                if (task.ret_ == BalanceTask::Result::FAILED) {
                    // Resume the failed task.
                    task.ret_ = BalanceTask::Result::IN_PROGRESS;
                }
                task.startTimeMs_ = std::get<2>(tup);
                task.endTimeMs_ = std::get<3>(tup);
            }
            tasks_.emplace_back(std::move(task));
            iter->next();
        }
    }
    return true;
}

std::string BalancePlan::planKey() const {
    std::string str;
    str.reserve(48);
    str.append(reinterpret_cast<const char*>(kBalancePlanTable.data()), kBalancePlanTable.size());
    str.append(reinterpret_cast<const char*>(&id_), sizeof(id_));
    return str;
}

std::string BalancePlan::planVal() const {
    std::string str;
    str.append(reinterpret_cast<const char*>(&status_), sizeof(status_));
    return str;
}

const std::string& BalancePlan::prefix() {
    return kBalancePlanTable;
}

BalanceID BalancePlan::id(const folly::StringPiece& rawKey) {
    return *reinterpret_cast<const BalanceID*>(rawKey.begin() + kBalancePlanTable.size());
}

BalancePlan::Status BalancePlan::status(const folly::StringPiece& rawVal) {
    return static_cast<Status>(*rawVal.begin());
}

}  // namespace meta
}  // namespace nebula

