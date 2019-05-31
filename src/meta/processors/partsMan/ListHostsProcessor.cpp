/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/ListHostsProcessor.h"

namespace nebula {
namespace meta {

void ListHostsProcessor::process(const cpp2::ListHostsReq& req) {
    UNUSED(req);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto ret = allHosts();
    if (!ret.ok()) {
        LOG(ERROR) << "List Hosts Failed : No hosts";
        resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }
    resp_.set_hosts(std::move(ret.value()));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

