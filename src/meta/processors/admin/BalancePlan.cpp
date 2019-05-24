/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/BalancePlan.h"
#include <folly/synchronization/Baton.h>
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

const std::string kBalancePlanTable = "__b_plan__"; // NOLINT

void BalancePlan::invoke() {
    status_ = Status::IN_PROGRESS;
    // TODO(heng) we want tasks for the same part to be invoked serially.
    for (auto& task : tasks_) {
        task.invoke();
    }
    saveInStore(true);
}

void BalancePlan::registerTaskCb() {
    for (auto& task : tasks_) {
        task.onFinished_ = [this]() {
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
                saveInStore(true);
                onFinished_();
            }
        };
        task.onError_ = [this]() {
            bool finished = false;
            {
                std::lock_guard<std::mutex> lg(lock_);
                finishedTaskNum_++;
                if (finishedTaskNum_ == tasks_.size()) {
                    finished = true;
                    status_ = Status::FAILED;
                }
            }
            if (finished) {
                saveInStore(true);
                onFinished_();
            }
        };
    }
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

