/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/MultiPutProcessor.h"

namespace nebula {
namespace meta {

void MultiPutProcessor::process(const cpp2::MultiPutReq& req) {
    std::vector<kvstore::KV> data;
    for (auto& pair : req.get_pairs()) {
        data.emplace_back(std::move(pair.get_key()), std::move(pair.get_value()));
    }
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula
