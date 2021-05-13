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
        handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
    }

    if (req.get_stop() != nullptr) {
        if (!(*req.get_stop())) {
            handleErrorCode(nebula::cpp2::ErrorCode::E_UNKNOWN);
            onFinished();
            return;
        }
        auto ret = Balancer::instance(kvstore_)->stop();
        if (!nebula::ok(ret)) {
            handleErrorCode(nebula::error(ret));
            onFinished();
            return;
        }
        handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
        resp_.set_id(nebula::value(ret));
        onFinished();
        return;
    }

    if (req.get_reset() != nullptr) {
        if (!(*req.get_reset())) {
            handleErrorCode(nebula::cpp2::ErrorCode::E_UNKNOWN);
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
        handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
        onFinished();
        return;
    }

    if (req.get_id() != nullptr) {
        auto ret = Balancer::instance(kvstore_)->show(*req.get_id());
        if (!nebula::ok(ret)) {
            auto retCode = nebula::error(ret);
            LOG(ERROR) << "Show balance ID failed, error "
                       << apache::thrift::util::enumNameSafe(retCode);
            handleErrorCode(retCode);
            onFinished();
            return;
        }
        const auto& plan = nebula::value(ret);
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

    auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
    if (!nebula::ok(activeHostsRet)) {
        auto retCode = nebula::error(activeHostsRet);
        LOG(ERROR) << "Get active hosts failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto hosts = std::move(nebula::value(activeHostsRet));

    if (hosts.empty()) {
        LOG(ERROR) << "There is no active hosts";
        handleErrorCode(nebula::cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }

    std::vector<HostAddr> lostHosts;
    if (req.host_del_ref().has_value()) {
        lostHosts = *req.host_del_ref();
    }

    auto ret = Balancer::instance(kvstore_)->balance(std::move(lostHosts));
    if (!ok(ret)) {
        auto retCode = error(ret);
        LOG(ERROR) << "Balance Failed: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    resp_.set_id(value(ret));
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula

