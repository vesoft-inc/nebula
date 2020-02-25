/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/customKV/ScanProcessor.h"

namespace nebula {
namespace meta {

void ScanProcessor::process(const cpp2::ScanReq& req) {
    auto start = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_start());
    auto end   = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_end());
    auto result = doScan(start, end);
    if (!result.ok()) {
        LOG(ERROR) << "Scan Failed: " << result.status();
        handleErrorCode(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_values(std::move(result.value()));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
