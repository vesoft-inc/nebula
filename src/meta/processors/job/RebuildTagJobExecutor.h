/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_REBUILDTAGJOBEXECUTOR_H_
#define META_REBUILDTAGJOBEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <string>   // for string
#include <utility>  // for move
#include <vector>   // for vector

#include "common/thrift/ThriftTypes.h"               // for JobID, PartitionID
#include "meta/processors/job/RebuildJobExecutor.h"  // for RebuildJobExecutor

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

class RebuildTagJobExecutor : public RebuildJobExecutor {
 public:
  RebuildTagJobExecutor(JobID jobId,
                        kvstore::KVStore* kvstore,
                        AdminClient* adminClient,
                        const std::vector<std::string>& paras)
      : RebuildJobExecutor(jobId, kvstore, adminClient, std::move(paras)) {}

 protected:
  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDTAGJOBEXECUTOR_H_
