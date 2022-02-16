/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_STATSJOBEXECUTOR_H_
#define META_STATSJOBEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future
#include <stdint.h>                // for int32_t

#include <string>  // for string
#include <vector>  // for vector

#include "common/thrift/ThriftTypes.h"        // for JobID, PartitionID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/StorageJobExecutor.h"  // for StorageJobExecutor

namespace nebula {
class Status;
namespace kvstore {
class KVStore;
}  // namespace kvstore
namespace meta {
class AdminClient;
namespace cpp2 {
class ReportTaskReq;
class StatsItem;
}  // namespace cpp2
}  // namespace meta
struct HostAddr;

class Status;
namespace kvstore {
class KVStore;
}  // namespace kvstore
struct HostAddr;

namespace meta {
class AdminClient;
namespace cpp2 {
class ReportTaskReq;
class StatsItem;
}  // namespace cpp2

class StatsJobExecutor : public StorageJobExecutor {
 public:
  StatsJobExecutor(JobID jobId,
                   kvstore::KVStore* kvstore,
                   AdminClient* adminClient,
                   const std::vector<std::string>& paras)
      : StorageJobExecutor(jobId, kvstore, adminClient, paras) {
    toHost_ = TargetHosts::LEADER;
  }

  bool check() override;

  nebula::cpp2::ErrorCode prepare() override;

  nebula::cpp2::ErrorCode stop() override;

  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;

  /**
   * @brief Summarize the results of statsItem_
   *
   * @param exeSuccessed
   * @return
   */
  nebula::cpp2::ErrorCode finish(bool exeSuccessed) override;

  nebula::cpp2::ErrorCode saveSpecialTaskStatus(const cpp2::ReportTaskReq& req) override;

 private:
  /**
   * @brief Stats job writes an additional data.
   * The additional data is written when the stats job passes the check
   * function. Update this additional data when job finishes.
   *
   * @param key
   * @param val
   * @return
   */
  nebula::cpp2::ErrorCode save(const std::string& key, const std::string& val);

  void addStats(cpp2::StatsItem& lhs, const cpp2::StatsItem& rhs);

  std::string toTempKey(int32_t jobId);

  nebula::cpp2::ErrorCode doRemove(const std::string& key);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_STATSJOBEXECUTOR_H_
