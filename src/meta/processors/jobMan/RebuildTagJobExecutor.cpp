/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/RebuildTagJobExecutor.h"

namespace nebula {
namespace meta {

folly::Future<Status>
RebuildTagJobExecutor::executeInternal(HostAddr&& address,
                                       std::vector<PartitionID>&& parts) {
    return adminClient_->addTask(cpp2::AdminCmd::REBUILD_TAG_INDEX, jobId_, taskId_++,
                                 space_, {std::move(address)}, taskParameters_,
                                 std::move(parts), concurrency_);
}

}  // namespace meta
}  // namespace nebula
