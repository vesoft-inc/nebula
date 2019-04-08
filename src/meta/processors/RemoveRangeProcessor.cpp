/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveRangeProcessor.h"

namespace nebula {
namespace meta {

void RemoveRangeProcessor::process(const cpp2::RemoveRangeReq& req) {
    CHECK_KEY_PREFIX(req.get_start());
    CHECK_KEY_PREFIX(req.get_end());
    doRemoveRange(req.get_start(), req.get_end());
}

}  // namespace meta
}  // namespace nebula
