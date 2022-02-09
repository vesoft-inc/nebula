/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_REBUILDJOBEXECUTOR_H_
#define META_REBUILDJOBEXECUTOR_H_

#include "interface/gen-cpp2/common_types.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/StorageJobExecutor.h"

namespace nebula {
namespace meta {

class RebuildJobExecutor : public StorageJobExecutor {
 public:
  RebuildJobExecutor(JobID jobId,
                     kvstore::KVStore* kvstore,
                     AdminClient* adminClient,
                     const std::vector<std::string>& paras)
      : StorageJobExecutor(jobId, kvstore, adminClient, paras) {
    toHost_ = TargetHosts::LEADER;
  }

  bool check() override;

  nebula::cpp2::ErrorCode prepare() override;

  nebula::cpp2::ErrorCode stop() override;

 protected:
  std::vector<std::string> taskParameters_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDJOBEXECUTOR_H_
