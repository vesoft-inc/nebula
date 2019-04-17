/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveProcessor.h"

namespace nebula {
namespace meta {

void RemoveProcessor::process(const cpp2::RemoveReq& req) {
    CHECK_SEGMENT(req.get_segment());
    auto key = MetaServiceUtils::assembleSegmentKey(req.get_segment(), req.get_key());
    doRemove(key);
}

}  // namespace meta
}  // namespace nebula
