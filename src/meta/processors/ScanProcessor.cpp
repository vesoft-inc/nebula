/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/ScanProcessor.h"

namespace nebula {
namespace meta {

void ScanProcessor::process(const cpp2::ScanReq& req) {
    CHECK_KEY_PREFIX(req.get_start());
    CHECK_KEY_PREFIX(req.get_end());
    auto result = doScan(req.get_start(), req.get_end());
    if (!result.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_values(std::move(result.value()));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
