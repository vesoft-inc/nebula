/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_JOBEXECUTOR_H_
#define META_JOBEXECUTOR_H_

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/ErrorOr.h"
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/JobDescription.h"

namespace nebula {
namespace meta {

class JobExecutor {
 public:
  JobExecutor() = default;
  JobExecutor(kvstore::KVStore* kv, GraphSpaceID space) : kvstore_(kv), space_(space) {}
  virtual ~JobExecutor() = default;

  /**
   * @brief Check the arguments about the job.
   *
   * @return
   */
  virtual nebula::cpp2::ErrorCode check() = 0;

  /**
   * @brief Prepare the Job info from the arguments.
   *
   * @return
   */
  virtual nebula::cpp2::ErrorCode prepare() = 0;

  /**
   * @brief The skeleton to run the job.
   * You should rewrite the executeInternal to trigger the calling.
   *
   * @return
   */
  virtual folly::Future<nebula::cpp2::ErrorCode> execute() = 0;

  /**
   * @brief Stop the job when the user cancel it.
   *
   * @return
   */
  virtual nebula::cpp2::ErrorCode stop() = 0;

  /**
   * @brief Called when job finished or failed
   *
   * @param ret True means finished and false means failed
   * @return
   */
  virtual nebula::cpp2::ErrorCode finish(bool ret) = 0;

  /**
   * @brief Called when recover a job
   *
   * @return
   */
  virtual nebula::cpp2::ErrorCode recovery() = 0;

  virtual bool isMetaJob() = 0;

  virtual JobDescription getJobDescription() = 0;

  /**
   * @brief Provide an extra status for some special tasks
   *
   * @return
   */
  virtual nebula::cpp2::ErrorCode saveSpecialTaskStatus(const cpp2::ReportTaskReq&) = 0;

  /**
   * @brief Determine whether the current job executor is running
   *
   * @return
   */
  virtual bool isRunning() = 0;

  virtual void resetRunningStatus() = 0;

 protected:
  nebula::cpp2::ErrorCode spaceExist();

 protected:
  kvstore::KVStore* kvstore_{nullptr};

  GraphSpaceID space_;

  std::atomic<bool> isRunning_{false};
};

class JobExecutorFactory {
 public:
  virtual ~JobExecutorFactory() = default;
  virtual std::unique_ptr<JobExecutor> createJobExecutor(const JobDescription& jd,
                                                         kvstore::KVStore* store,
                                                         AdminClient* client);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOBEXECUTOR_H_
