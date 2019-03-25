/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveRangeProcessor.h"

namespace nebula {
namespace meta {

void RemoveRangeProcessor::process(const cpp2::RemoveRangeReq& req) {
    guard_ = std::make_unique<std::lock_guard<std::mutex>>(
                                BaseProcessor<cpp2::RemoveRangeResp>::lock_);
    doRemoveRange(std::move(req.get_start()), std::move(req.get_end()));
}

}  // namespace meta
}  // namespace nebula
