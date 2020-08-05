/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/indexMan/RebuildTagIndexProcessor.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void RebuildTagIndexProcessor::process(const cpp2::RebuildIndexReq& req) {
    folly::SharedMutex::ReadHolder indexHolder(LockUtils::tagIndexLock());
    processInternal(req);
}

folly::Future<Status>
RebuildTagIndexProcessor::caller(const HostAddr& address,
                                 GraphSpaceID space,
                                 IndexID indexID,
                                 std::vector<PartitionID> parts,
                                 bool isOffline) {
    return adminClient_->rebuildTagIndex(address, space, indexID, std::move(parts), isOffline);
}

}  // namespace meta
}  // namespace nebula
