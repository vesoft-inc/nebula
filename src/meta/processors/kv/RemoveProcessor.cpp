/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/kv/RemoveProcessor.h"

namespace nebula {
namespace meta {

void RemoveProcessor::process(const cpp2::RemoveReq& req) {
  CHECK_SEGMENT(req.get_segment());
  auto key = MetaKeyUtils::assembleSegmentKey(req.get_segment(), req.get_key());
  doRemove(key);
}

}  // namespace meta
}  // namespace nebula
