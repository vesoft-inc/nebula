/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/kv/RemoveRangeProcessor.h"

namespace nebula {
namespace meta {

void RemoveRangeProcessor::process(const cpp2::RemoveRangeReq& req) {
  CHECK_SEGMENT(req.get_segment());
  auto start = MetaKeyUtils::assembleSegmentKey(req.get_segment(), req.get_start());
  auto end = MetaKeyUtils::assembleSegmentKey(req.get_segment(), req.get_end());
  doRemoveRange(start, end);
}

}  // namespace meta
}  // namespace nebula
