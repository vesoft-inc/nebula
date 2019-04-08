/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/AddHostsProcessor.h"

namespace nebula {
namespace meta {


void AddHostsProcessor::process(const cpp2::AddHostsReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    std::vector<kvstore::KV> data;
    for (auto& h : req.get_hosts()) {
        data.emplace_back(MetaUtils::hostKey(h.ip, h.port), MetaUtils::hostVal());
    }
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

