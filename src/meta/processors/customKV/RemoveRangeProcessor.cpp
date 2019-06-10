/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/customKV/RemoveRangeProcessor.h"

namespace nebula {
namespace meta {

void RemoveRangeProcessor::process(const cpp2::RemoveRangeReq& req) {
    CHECK_SEGMENT(req.get_segment());
    auto start = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_start());
    auto end   = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_end());
    doRemoveRange(start, end);
}

}  // namespace meta
}  // namespace nebula
