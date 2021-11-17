/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_FLUSHJOBEXECUTOR_H_
#define META_FLUSHJOBEXECUTOR_H_

#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {

class FlushJobExecutor : public SimpleConcurrentJobExecutor {
 public:
  FlushJobExecutor(JobID jobId,
                   kvstore::KVStore* kvstore,
                   AdminClient* adminClient,
                   const std::vector<std::string>& params);

  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_FLUSHJOBEXECUTOR_H_
