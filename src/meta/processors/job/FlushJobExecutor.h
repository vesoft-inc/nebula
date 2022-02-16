/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_FLUSHJOBEXECUTOR_H_
#define META_FLUSHJOBEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <string>  // for string
#include <vector>  // for vector

#include "common/thrift/ThriftTypes.h"                        // for JobID
#include "meta/processors/job/SimpleConcurrentJobExecutor.h"  // for SimpleC...

namespace nebula {
class Status;
namespace kvstore {
class KVStore;
}  // namespace kvstore
namespace meta {
class AdminClient;
}  // namespace meta
struct HostAddr;

class Status;
namespace kvstore {
class KVStore;
}  // namespace kvstore
struct HostAddr;

namespace meta {
class AdminClient;

/**
 * @brief Executor for flush job, always called by job manager
 */
class FlushJobExecutor : public SimpleConcurrentJobExecutor {
 public:
  FlushJobExecutor(JobID jobId,
                   kvstore::KVStore* kvstore,
                   AdminClient* adminClient,
                   const std::vector<std::string>& params);

  /**
   * @brief
   *
   * @param address The host that task command send to
   * @param parts Parts that the host contains
   * @return
   */
  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_FLUSHJOBEXECUTOR_H_
