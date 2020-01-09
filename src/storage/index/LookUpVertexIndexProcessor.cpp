/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "LookUpVertexIndexProcessor.h"

namespace nebula {
namespace storage {

void LookUpVertexIndexProcessor::process(const cpp2::LookUpIndexRequest& req) {
    auto ret = prepareLookUp(req, true);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        putResultCodes(ret, req.get_parts());
        return;
    }

    ret = prepareExpr(req);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        putResultCodes(ret, req.get_parts());
        return;
    }
}
}  // namespace storage
}  // namespace nebula

