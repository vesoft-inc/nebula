/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/BalanceProcessor.h"
#include "meta/processors/admin/Balancer.h"

namespace nebula {
namespace meta {

void BalanceProcessor::process(const cpp2::BalanceReq& req) {
    if (req.get_space_id() != nullptr) {
        LOG(ERROR) << "Unsupport balance for specific space " << *req.get_space_id();
        handleErrorCode(cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
    }
    if (req.get_stop() != nullptr) {
        if (!(*req.get_stop())) {
            handleErrorCode(cpp2::ErrorCode::E_UNKNOWN);
            onFinished();
            return;
        }
        auto ret = Balancer::instance(kvstore_)->stop();
        if (!ret.ok()) {
            handleErrorCode(cpp2::ErrorCode::E_NO_RUNNING_BALANCE_PLAN);
            onFinished();
            return;
        }
        handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        resp_.set_id(ret.value());
        onFinished();
        return;
    }
    if (req.get_reset() != nullptr) {
        if (!(*req.get_reset())) {
            handleErrorCode(cpp2::ErrorCode::E_UNKNOWN);
            onFinished();
            return;
        }
        auto plan = Balancer::instance(kvstore_)->cleanLastInValidPlan();
        if (!ok(plan)) {
            handleErrorCode(error(plan));
            onFinished();
            return;
        }
        resp_.set_id(value(plan));
        handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        onFinished();
        return;
    }
    if (req.get_id() != nullptr) {
        auto ret = Balancer::instance(kvstore_)->show(*req.get_id());
        if (!ret.ok()) {
            LOG(ERROR) << "Balance ID not found: " << *req.get_id();
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        const auto& plan = ret.value();
        std::vector<cpp2::BalanceTask> thriftTasks;
        for (auto& task : plan.tasks()) {
            cpp2::BalanceTask t;
            t.set_id(task.taskIdStr());
            switch (task.result()) {
                case BalanceTaskResult::SUCCEEDED:
                    t.set_result(cpp2::TaskResult::SUCCEEDED);
                    break;
                case BalanceTaskResult::FAILED:
                    t.set_result(cpp2::TaskResult::FAILED);
                    break;
                case BalanceTaskResult::IN_PROGRESS:
                    t.set_result(cpp2::TaskResult::IN_PROGRESS);
                    break;
                case BalanceTaskResult::INVALID:
                    t.set_result(cpp2::TaskResult::INVALID);
                    break;
            }
            thriftTasks.emplace_back(std::move(t));
        }
        resp_.set_tasks(std::move(thriftTasks));
        onFinished();
        return;
    }
    std::unordered_set<HostAddr> hostDel;
    if (req.get_host_del() != nullptr) {
        hostDel.reserve(req.get_host_del()->size());
        for (const auto& host : *req.get_host_del()) {
            hostDel.emplace(host);
        }
    }
    auto hosts = ActiveHostsMan::getActiveHosts(kvstore_);
    if (hosts.empty()) {
        LOG(ERROR) << "There is no active hosts";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }
    auto ret = Balancer::instance(kvstore_)->balance(std::move(hostDel));
    if (!ok(ret)) {
        LOG(ERROR) << "Balance Failed: " << static_cast<int32_t>(ret.left());
        handleErrorCode(error(ret));
        onFinished();
        return;
    }
    resp_.set_id(value(ret));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula

