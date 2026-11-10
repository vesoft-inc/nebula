/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_REBUILDTAGOFFLINEJOBEXECUTOR_H_
#define META_REBUILDTAGOFFLINEJOBEXECUTOR_H_

#include "meta/processors/job/RebuildJobExecutor.h"

namespace nebula {
namespace meta {

class RebuildTagOfflineJobExecutor : public RebuildJobExecutor {
 public:
  RebuildTagOfflineJobExecutor(GraphSpaceID space,
                        JobID jobId,
                        kvstore::KVStore* kvstore,
                        AdminClient* adminClient,
                        const std::vector<std::string>& paras)
      : RebuildJobExecutor(space, jobId, kvstore, adminClient, std::move(paras)) {
        toHost_ = TargetHosts::DEFAULT;
      }

 protected:
  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDTAGOFFLINEJOBEXECUTOR_H_
