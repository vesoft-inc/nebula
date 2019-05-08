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
        auto hostKey = MetaServiceUtils::hostKey(h.ip, h.port);
        auto ret = hostExist(hostKey);
        if (!ret.ok()) {
            LOG(WARNING) << "The host [" << h.ip << ":" << h.port << "] not existed!";
            resp_.set_code(to(ret));
            onFinished();
            return;
        }
        hostsKey.emplace_back(std::move(hostKey));
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    doMultiRemove(std::move(hostsKey));
}

}  // namespace meta
}  // namespace nebula

