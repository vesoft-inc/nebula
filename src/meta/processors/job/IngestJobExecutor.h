/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_INGESTJOBEXECUTOR_H_
#define META_INGESTJOBEXECUTOR_H_

#include "meta/processors/job/MetaJobExecutor.h"
#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {

/**
 * @brief Executor for ingest job, always called by job manager.
 */
class IngestJobExecutor : public SimpleConcurrentJobExecutor {
 public:
  IngestJobExecutor(GraphSpaceID space,
                    JobID jobId,
                    kvstore::KVStore* kvstore,
                    AdminClient* adminClient,
                    const std::vector<std::string>& params);

  nebula::cpp2::ErrorCode check() override;

  nebula::cpp2::ErrorCode prepare() override;

  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_INGESTJOBEXECUTOR_H_
