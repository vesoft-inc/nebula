/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/RebuildFTJobExecutor.h"

namespace nebula {
namespace meta {

folly::Future<Status> RebuildFTJobExecutor::executeInternal(HostAddr&& address,
                                                            std::vector<PartitionID>&& parts) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  adminClient_
      ->addTask(cpp2::JobType::REBUILD_FULLTEXT_INDEX,
                jobId_,
                taskId_++,
                space_,
                std::move(address),
                taskParameters_,
                std::move(parts))
      .then([pro = std::move(pro)](auto&& t) mutable {
        CHECK(!t.hasException());
        auto status = std::move(t).value();
        if (status.ok()) {
          pro.setValue(Status::OK());
        } else {
          pro.setValue(status.status());
        }
      });
  return f;
}

}  // namespace meta
}  // namespace nebula
