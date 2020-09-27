/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/RebuildEdgeJobExecutor.h"

namespace nebula {
namespace meta {

folly::Future<Status>
RebuildEdgeJobExecutor::executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) {
    std::vector<std::string> taskParameters;
    taskParameters.emplace_back(folly::to<std::string>(indexId_));
    return adminClient_->addTask(cpp2::AdminCmd::REBUILD_EDGE_INDEX, jobId_, taskId_++,
                                 space_, {std::move(address)}, std::move(taskParameters),
                                 std::move(parts), concurrency_);
}

}  // namespace meta
}  // namespace nebula
