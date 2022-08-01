/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_REBUILDFTJOBEXECUTOR_H_
#define META_REBUILDFTJOBEXECUTOR_H_

#include "meta/processors/job/RebuildJobExecutor.h"

namespace nebula {
namespace meta {

class RebuildFTJobExecutor : public RebuildJobExecutor {
 public:
  RebuildFTJobExecutor(GraphSpaceID space,
                       JobID jobId,
                       kvstore::KVStore* kvstore,
                       AdminClient* adminClient,
                       const std::vector<std::string>& paras)
      : RebuildJobExecutor(space, jobId, kvstore, adminClient, std::move(paras)) {
    toHost_ = TargetHosts::LISTENER;
  }

  nebula::cpp2::ErrorCode stop() override {
    // Unlike rebuild tag/edge idnex, rebuild full text job is not stoppable
    return nebula::cpp2::ErrorCode::E_JOB_NOT_STOPPABLE;
  }

 protected:
  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDFTJOBEXECUTOR_H_
