/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveHostsProcessor.h"

namespace nebula {
namespace meta {


void RemoveHostsProcessor::process(const cpp2::RemoveHostsReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());

    std::vector<std::string> hostsKey;
    for (auto& h : req.get_hosts()) {
        hostsKey.emplace_back(MetaServiceUtils::hostKey(h.ip, h.port));
    }

    auto hostsRet = hostsExist(hostsKey);
    if (!hostsRet.ok()) {
        resp_.set_code(to(std::move(hostsRet)));
        onFinished();
        return;
    }

    LOG(INFO) << "Remove hosts ";
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    doMultiRemove(std::move(hostsKey));
}

}  // namespace meta
}  // namespace nebula

