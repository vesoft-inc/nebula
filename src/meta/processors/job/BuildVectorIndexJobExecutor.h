/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_BUILDVECTORINDEXJOBEXECUTOR_H_
#define META_BUILDVECTORINDEXJOBEXECUTOR_H_

#include "interface/gen-cpp2/common_types.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/StorageJobExecutor.h"

namespace nebula {
namespace meta {

/**
 * @brief Build Vector Index Job Executor
 *
 * This executor is responsible for:
 * 1. Validating vector index metadata (name, tag/edge, properties, dimensions, ANN type, params)
 * 2. Persisting vector index metadata to Meta Engine via Raft
 * 3. Distributing index building subtasks to corresponding Storage partition nodes
 * (Leader/Follower)
 * 4. Tracking progress and status of local index building on Storage nodes
 * 5. Updating index status from REBUILDING to ONLINE when all partitions complete
 */
class BuildVectorIndexJobExecutor : public StorageJobExecutor {
 public:
  BuildVectorIndexJobExecutor(GraphSpaceID space,
                              JobID jobId,
                              kvstore::KVStore* kvstore,
                              AdminClient* adminClient,
                              const std::vector<std::string>& paras)
      : StorageJobExecutor(space, jobId, kvstore, adminClient, paras) {
    toHost_ = TargetHosts::LEADER;
  }

  nebula::cpp2::ErrorCode check() override;

  nebula::cpp2::ErrorCode prepare() override;

 protected:
  // Vector index parameters parsed from paras_
  std::string indexName_;
  std::string annType_;  // ANN algorithm type (e.g., "HNSW", "IVF")
  std::vector<std::string> taskParameters_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BUILDVECTORINDEXJOBEXECUTOR_H_
