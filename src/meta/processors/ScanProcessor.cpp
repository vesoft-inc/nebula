/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/ScanProcessor.h"

namespace nebula {
namespace meta {

void ScanProcessor::process(const cpp2::ScanReq& req) {
    CHECK_SEGMENT(req.get_segment());
    auto start = MetaUtils::assembleSegmentKey(req.get_segment(), req.get_start());
    auto end   = MetaUtils::assembleSegmentKey(req.get_segment(), req.get_end());
    auto result = doScan(start, end);
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
