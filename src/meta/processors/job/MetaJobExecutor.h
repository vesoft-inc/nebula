/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_METAJOBEXECUTOR_H_
#define META_METAJOBEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future
#include <limits.h>                // for INT_MAX, INT_MIN
#include <stdint.h>                // for int32_t
#include <thrift/lib/cpp/util/EnumUtils.h>

#include <algorithm>           // for max
#include <condition_variable>  // for condition_variable
#include <functional>          // for function
#include <mutex>               // for mutex
#include <string>              // for string, basic_string
#include <vector>              // for vector

#include "common/base/ErrorOr.h"
#include "common/thrift/ThriftTypes.h"        // for GraphSpaceID, JobID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode::S...
#include "interface/gen-cpp2/meta_types.h"    // for JobStatus, ReportTaskRe...
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

class MetaJobExecutor : public JobExecutor {
 public:
  MetaJobExecutor(JobID jobId,
                  kvstore::KVStore* kvstore,
                  AdminClient* adminClient,
                  const std::vector<std::string>& paras)
      : JobExecutor(kvstore), jobId_(jobId), adminClient_(adminClient), paras_(paras) {
    executorOnFinished_ = [](meta::cpp2::JobStatus) { return nebula::cpp2::ErrorCode::SUCCEEDED; };
  }

  virtual ~MetaJobExecutor() = default;

  /**
   * @brief Check the arguments about the job.
   *
   * @return
   */
  bool check() override;

  /**
   * @brief Prepare the Job info from the arguments.
   *
   * @return
   */
  nebula::cpp2::ErrorCode prepare() override;

  /**
   * @brief  The skeleton to run the job.
   * You should rewrite the executeInternal to trigger the calling.
   *
   * @return
   */
  nebula::cpp2::ErrorCode execute() override;

  /**
   * @brief Stop the job when the user cancel it.
   *
   * @return
   */
  nebula::cpp2::ErrorCode stop() override;

  nebula::cpp2::ErrorCode finish(bool) override;

  void setSpaceId(GraphSpaceID spaceId) override;

  bool isMetaJob() override;

  nebula::cpp2::ErrorCode recovery() override;

  void setFinishCallBack(
      std::function<nebula::cpp2::ErrorCode(meta::cpp2::JobStatus)> func) override;

  nebula::cpp2::ErrorCode saveSpecialTaskStatus(const cpp2::ReportTaskReq&) override;

 protected:
  virtual folly::Future<Status> executeInternal();

 protected:
  JobID jobId_{INT_MIN};
  TaskID taskId_{0};
  AdminClient* adminClient_{nullptr};
  GraphSpaceID space_;
  std::vector<std::string> paras_;
  int32_t concurrency_{INT_MAX};
  volatile bool stopped_{false};
  std::mutex muInterrupt_;
  std::condition_variable condInterrupt_;
  std::function<nebula::cpp2::ErrorCode(meta::cpp2::JobStatus)> executorOnFinished_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAJOBEXECUTOR_H_
