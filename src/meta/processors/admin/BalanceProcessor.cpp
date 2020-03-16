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
    if (req.get_id() != nullptr) {
        auto ret = Balancer::instance(kvstore_)->show(*req.get_id());
        if (!ret.ok()) {
            handleErrorCode(cpp2::ErrorCode::E_BAD_BALANCE_PLAN);
            onFinished();
            return;
        }
        handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        const auto& plan = ret.value();
        std::vector<cpp2::BalanceTask> thriftTasks;
        for (auto& task : plan.tasks()) {
            cpp2::BalanceTask t;
            t.set_id(task.taskIdStr());
            switch (task.result()) {
                case BalanceTask::Result::SUCCEEDED:
                    t.set_result(cpp2::TaskResult::SUCCEEDED);
                    break;
                case BalanceTask::Result::FAILED:
                    t.set_result(cpp2::TaskResult::FAILED);
                    break;
                case BalanceTask::Result::IN_PROGRESS:
                    t.set_result(cpp2::TaskResult::IN_PROGRESS);
                    break;
                case BalanceTask::Result::INVALID:
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
            hostDel.emplace(HostAddr(host.get_ip(), host.get_port()));
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

