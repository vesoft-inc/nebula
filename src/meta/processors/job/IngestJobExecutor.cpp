/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/IngestJobExecutor.h"

#include "common/utils/MetaKeyUtils.h"
#include "meta/MetaServiceUtils.h"

namespace nebula {
namespace meta {

IngestJobExecutor::IngestJobExecutor(GraphSpaceID space,
                                     JobID jobId,
                                     kvstore::KVStore* kvstore,
                                     AdminClient* adminClient,
                                     const std::vector<std::string>& paras)
    : SimpleConcurrentJobExecutor(space, jobId, kvstore, adminClient, paras) {}

nebula::cpp2::ErrorCode IngestJobExecutor::check() {
  return paras_.empty() ? nebula::cpp2::ErrorCode::SUCCEEDED
                        : nebula::cpp2::ErrorCode::E_INVALID_JOB;
}

nebula::cpp2::ErrorCode IngestJobExecutor::prepare() {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<Status> IngestJobExecutor::executeInternal(HostAddr&& address,
                                                         std::vector<PartitionID>&& parts) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  adminClient_
      ->addTask(cpp2::JobType::INGEST,
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
