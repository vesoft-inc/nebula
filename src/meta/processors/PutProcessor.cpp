/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/PutProcessor.h"

namespace nebula {
namespace meta {

void PutProcessor::process(const cpp2::PutReq& req) {
    guard_ = std::make_unique<std::lock_guard<std::mutex>>(
                                BaseProcessor<cpp2::PutResp>::lock_);
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(req.get_key()), std::move(req.get_value()));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula
