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
    if (!nebula::ok(result)) {
        auto retCode = nebula::error(result);
        LOG(ERROR) << "Scan Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_values(std::move(nebula::value(result)));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
