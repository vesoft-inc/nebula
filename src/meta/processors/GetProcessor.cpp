/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */


#include "meta/processors/GetProcessor.h"

namespace nebula {
namespace meta {

void GetProcessor::process(const cpp2::GetReq& req) {
    guard_ = std::make_unique<std::lock_guard<std::mutex>>(
                                BaseProcessor<cpp2::GetResp>::lock_);
    auto result = doGet(req.get_key());
    if (!result.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_value(std::move(result.value()));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
