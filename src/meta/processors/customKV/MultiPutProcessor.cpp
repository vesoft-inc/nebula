/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/customKV/MultiPutProcessor.h"

namespace nebula {
namespace meta {

void MultiPutProcessor::process(const cpp2::MultiPutReq& req) {
    CHECK_SEGMENT(req.get_segment());
    std::vector<kvstore::KV> data;
    for (auto& pair : req.get_pairs()) {
        data.emplace_back(MetaServiceUtils::assembleSegmentKey(req.get_segment(), pair.key),
                          pair.value);
    }
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula
