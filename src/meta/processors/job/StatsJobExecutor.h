/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_STATSJOBEXECUTOR_H_
#define META_STATSJOBEXECUTOR_H_

#include "interface/gen-cpp2/meta_types.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/MetaJobExecutor.h"

namespace nebula {
namespace meta {

class StatsJobExecutor : public MetaJobExecutor {
 public:
  StatsJobExecutor(JobID jobId,
                   kvstore::KVStore* kvstore,
                   AdminClient* adminClient,
                   const std::vector<std::string>& paras)
      : MetaJobExecutor(jobId, kvstore, adminClient, paras) {
    toHost_ = TargetHosts::LEADER;
  }

  bool check() override;

  nebula::cpp2::ErrorCode prepare() override;

  nebula::cpp2::ErrorCode stop() override;

  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;

  // Summarize the results of statsItem_
  nebula::cpp2::ErrorCode finish(bool exeSuccessed) override;

  nebula::cpp2::ErrorCode saveSpecialTaskStatus(const cpp2::ReportTaskReq& req) override;

 private:
  // Stats job writes an additional data.
  // The additional data is written when the statis job passes the check
  // function. Update this additional data when job finishes.
  nebula::cpp2::ErrorCode save(const std::string& key, const std::string& val);

  void addStats(cpp2::StatsItem& lhs, const cpp2::StatsItem& rhs);

  std::string toTempKey(int32_t jobId);

  nebula::cpp2::ErrorCode doRemove(const std::string& key);

 private:
  // Stats results
  std::unordered_map<HostAddr, cpp2::StatsItem> statsItem_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_STATSJOBEXECUTOR_H_
