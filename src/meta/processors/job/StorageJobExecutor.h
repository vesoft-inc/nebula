/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_STORAGEJOBEXECUTOR_H_
#define META_STORAGEJOBEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future
#include <limits.h>                // for INT_MAX, INT_MIN
#include <stdint.h>                // for int32_t
#include <thrift/lib/cpp/util/EnumUtils.h>

#include <algorithm>           // for max
#include <condition_variable>  // for condition_variable
#include <mutex>               // for mutex
#include <string>              // for string, basic_string
#include <utility>             // for pair
#include <vector>              // for vector

#include "common/base/ErrorOr.h"              // for ErrorOr
#include "common/datatypes/HostAddr.h"        // for HostAddr
#include "common/thrift/ThriftTypes.h"        // for PartitionID, GraphSpaceID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode::S...
#include "interface/gen-cpp2/meta_types.h"    // for ListenerType, ReportTas...
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/JobDescription.h"
#include "meta/processors/job/JobExecutor.h"  // for JobExecutor

namespace nebula {
class Status;
namespace kvstore {
class KVStore;
}  // namespace kvstore
namespace meta {
class AdminClient;
}  // namespace meta

class Status;
namespace kvstore {
class KVStore;
}  // namespace kvstore

namespace meta {
class AdminClient;

using PartsOfHost = std::pair<HostAddr, std::vector<PartitionID>>;
using ErrOrHosts = ErrorOr<nebula::cpp2::ErrorCode, std::vector<PartsOfHost>>;

class StorageJobExecutor : public JobExecutor {
 public:
  enum class TargetHosts { LEADER = 0, LISTENER, DEFAULT };

  StorageJobExecutor(JobID jobId,
                     kvstore::KVStore* kvstore,
                     AdminClient* adminClient,
                     const std::vector<std::string>& paras)
      : JobExecutor(kvstore), jobId_(jobId), adminClient_(adminClient), paras_(paras) {}

  virtual ~StorageJobExecutor() = default;

  /**
   * @brief Check the arguments about the job.
   *
   * @return
   */
  bool check() override {
    return true;
  }

  /**
   * @brief Prepare the Job info from the arguments.
   *
   * @return
   */
  nebula::cpp2::ErrorCode prepare() override {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  /**
   * @brief The skeleton to run the job.
   * You should rewrite the executeInternal to trigger the calling.
   *
   * @return
   */
  nebula::cpp2::ErrorCode execute() override;

  void interruptExecution(JobID jobId);

  /**
   * @brief Stop the job when the user cancel it.
   *
   * @return
   */
  nebula::cpp2::ErrorCode stop() override {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode finish(bool) override {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  void setSpaceId(GraphSpaceID spaceId) override {
    space_ = spaceId;
  }

  nebula::cpp2::ErrorCode saveSpecialTaskStatus(const cpp2::ReportTaskReq&) override {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  bool isMetaJob() override {
    return false;
  }

  nebula::cpp2::ErrorCode recovery() override {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

 protected:
  ErrOrHosts getTargetHost(GraphSpaceID space);

  ErrOrHosts getLeaderHost(GraphSpaceID space);

  ErrOrHosts getListenerHost(GraphSpaceID space, cpp2::ListenerType type);

  virtual folly::Future<Status> executeInternal(HostAddr&& address,
                                                std::vector<PartitionID>&& parts) = 0;

 protected:
  JobID jobId_{INT_MIN};
  TaskID taskId_{0};
  AdminClient* adminClient_{nullptr};
  GraphSpaceID space_;
  std::vector<std::string> paras_;
  TargetHosts toHost_{TargetHosts::DEFAULT};
  int32_t concurrency_{INT_MAX};
  volatile bool stopped_{false};
  std::mutex muInterrupt_;
  std::condition_variable condInterrupt_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_STORAGEJOBEXECUTOR_H_
