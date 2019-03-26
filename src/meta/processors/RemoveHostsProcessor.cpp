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
        hostsKey.emplace_back(MetaUtils::hostKey(h.ip, h.port));
    }

    auto hostsRet = hostsExist(hostsKey);
    if (!hostsRet.ok()) {
        if (hostsRet == Status::HostNotFound()) {
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        } else {
            resp_.set_code(cpp2::ErrorCode::E_UNKNOWN);
        }
        onFinished();
        return;
    }

    for (auto& hostKey : hostsKey) {
        resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, hostKey,
                              [this] (kvstore::ResultCode code, HostAddr leader) {
            UNUSED(leader);
            if (code != kvstore::ResultCode::SUCCEEDED) {
                this->resp_.set_code(to(code));
                return;
            }
        });

        if (resp_.get_code() != cpp2::ErrorCode::SUCCEEDED) {
            this->onFinished();
            return;
        }
    }

    onFinished();
}

}  // namespace meta
}  // namespace nebula

