/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_BUILDTAGVECTORINDEXJOBEXECUTOR_H_
#define META_BUILDTAGVECTORINDEXJOBEXECUTOR_H_

#include "meta/processors/job/BuildVectorIndexJobExecutor.h"

namespace nebula {
namespace meta {

class BuildTagVectorIndexJobExecutor : public BuildVectorIndexJobExecutor {
 public:
  BuildTagVectorIndexJobExecutor(GraphSpaceID space,
                                 JobID jobId,
                                 kvstore::KVStore* kvstore,
                                 AdminClient* adminClient,
                                 const std::vector<std::string>& paras)
      : BuildVectorIndexJobExecutor(space, jobId, kvstore, adminClient, paras) {}

  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BUILDTAGVECTORINDEXJOBEXECUTOR_H_
