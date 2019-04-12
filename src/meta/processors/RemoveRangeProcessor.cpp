/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveRangeProcessor.h"

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
