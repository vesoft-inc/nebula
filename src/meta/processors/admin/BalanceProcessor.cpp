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
        resp_.set_code(cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
    }
    if (req.get_id() != nullptr) {
        LOG(ERROR) << "Unsupport show status for specific balance plan, id=" << *req.get_id();
        resp_.set_code(cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
    }
    auto hosts = ActiveHostsMan::getActiveHosts(kvstore_);
    if (hosts.empty()) {
        LOG(ERROR) << "There is no active hosts";
        resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }
    auto ret = Balancer::instance(kvstore_)->balance();
    if (!ret.ok()) {
        LOG(INFO) << "The balancer is running.";
        resp_.set_code(cpp2::ErrorCode::E_BALANCER_RUNNING);
        onFinished();
        return;
    }
    resp_.set_id(ret.value());
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}


}  // namespace meta
}  // namespace nebula

