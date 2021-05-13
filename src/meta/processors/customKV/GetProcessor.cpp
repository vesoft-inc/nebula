/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/customKV/GetProcessor.h"

namespace nebula {
namespace meta {

void GetProcessor::process(const cpp2::GetReq& req) {
    auto key = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_key());
    auto result = doGet(key);
    if (!nebula::ok(result)) {
        auto retCode = nebula::error(result);
        LOG(ERROR) << "Get Failed: " << key << " error: "
                   << apache::thrift::util::enumNameSafe(retCode);;
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_value(std::move(nebula::value(result)));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
