/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/RebuildTagJobExecutor.h"

namespace nebula {
namespace meta {

folly::Future<Status> RebuildTagJobExecutor::caller(const HostAddr& address,
                                                    GraphSpaceID space,
                                                    IndexID indexID,
                                                    std::vector<PartitionID> parts,
                                                    bool isOffline) {
    return adminClient_->rebuildTagIndex(address, space, indexID, std::move(parts), isOffline);
}

}  // namespace meta
}  // namespace nebula
