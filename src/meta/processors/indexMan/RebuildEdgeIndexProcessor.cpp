/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/indexMan/RebuildEdgeIndexProcessor.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void RebuildEdgeIndexProcessor::process(const cpp2::RebuildIndexReq& req) {
    folly::SharedMutex::ReadHolder indexHolder(LockUtils::edgeIndexLock());
    processInternal(req);
}

folly::Future<Status>
RebuildEdgeIndexProcessor::caller(const HostAddr& address,
                                  GraphSpaceID space,
                                  IndexID indexID,
                                  std::vector<PartitionID> parts) {
    return adminClient_->rebuildEdgeIndex(address, space, indexID, std::move(parts));
}

}  // namespace meta
}  // namespace nebula


