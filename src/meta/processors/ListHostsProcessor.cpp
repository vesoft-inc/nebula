/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/ListHostsProcessor.h"

namespace nebula {
namespace meta {

void ListHostsProcessor::process(const cpp2::ListHostsReq& req) {
    UNUSED(req);
    guard_ = std::make_unique<std::lock_guard<std::mutex>>(
                                BaseProcessor<cpp2::ListHostsResp>::lock_);
    auto ret = allHosts();
    if (!ret.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_hosts(std::move(ret.value()));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

