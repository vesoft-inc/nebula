/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/PartialScanProcessor.h"

namespace nebula {
namespace meta {

void PartialScanProcessor::process(const cpp2::PartialScanReq& req) {
    CHECK_SEGMENT(req.get_segment());
    auto start = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_start());
    auto end   = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_end());
    StatusOr<std::vector<std::string>> result;
    if (req.get_type() == "key") {
        result = doScanKey(start, end);
    } else if (req.get_type() == "value") {
        result = doScanValue(start, end);
    } else {
        LOG(ERROR) << "Partial Scan type should be key or value";
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    if (!result.ok()) {
        LOG(ERROR) << "Partial Scan Failed";
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

