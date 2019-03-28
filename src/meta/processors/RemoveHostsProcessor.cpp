/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveHostsProcessor.h"

namespace nebula {
namespace meta {


void RemoveHostsProcessor::process(const cpp2::RemoveHostsReq& req) {
    guard_ = std::make_unique<std::lock_guard<std::mutex>>(
                              BaseProcessor<cpp2::ExecResp>::lock_);

    std::vector<std::string> hostsKey;
    for (auto& h : req.get_hosts()) {
        hostsKey.emplace_back(MetaUtils::hostKey(h.ip, h.port));
    }

    auto hostsRet = hostsExist(hostsKey);
    if (!hostsRet.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_SPACE_EXISTED);
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
                this->onFinished();
                return;
            }
        });

        if (resp_.get_code() != cpp2::ErrorCode::SUCCEEDED) {
            return;
        }
    }

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula

