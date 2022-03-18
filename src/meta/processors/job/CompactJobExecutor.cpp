/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/CompactJobExecutor.h"

namespace nebula {
namespace meta {

CompactJobExecutor::CompactJobExecutor(JobID jobId,
                                       kvstore::KVStore* kvstore,
                                       AdminClient* adminClient,
                                       const std::vector<std::string>& paras)
    : SimpleConcurrentJobExecutor(jobId, kvstore, adminClient, paras) {}

folly::Future<Status> CompactJobExecutor::executeInternal(HostAddr&& address,
                                                          std::vector<PartitionID>&& parts) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  adminClient_
      ->addTask(cpp2::JobType::COMPACT,
                jobId_,
                taskId_++,
                space_,
                std::move(address),
                {},
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
