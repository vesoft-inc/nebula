/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/AddHostsProcessor.h"

namespace nebula {
namespace meta {

void AddHostsProcessor::process(const cpp2::AddHostsReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    std::vector<kvstore::KV> data;
    for (auto& h : req.get_hosts()) {
        data.emplace_back(MetaServiceUtils::hostKey(h.ip, h.port),
                          MetaServiceUtils::hostValOffline());
    }
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

