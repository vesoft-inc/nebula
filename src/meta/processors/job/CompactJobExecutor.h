/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_COMPACTJOBEXECUTOR_H_
#define META_COMPACTJOBEXECUTOR_H_

#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {

/**
 * @brief Executor for compact job, always called by job manager
 */
class CompactJobExecutor : public SimpleConcurrentJobExecutor {
 public:
  CompactJobExecutor(JobID jobId,
                     kvstore::KVStore* kvstore,
                     AdminClient* adminClient,
                     const std::vector<std::string>& params);

  /**
   * @brief
   *
   * @param address the host that task command send to
   * @param parts parts that the host contains
   * @return
   */
  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_COMPACTJOBEXECUTOR_H_
