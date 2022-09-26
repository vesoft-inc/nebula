/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_METAJOBEXECUTOR_H_
#define META_METAJOBEXECUTOR_H_

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/ErrorOr.h"
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/JobDescription.h"
#include "meta/processors/job/JobExecutor.h"

namespace nebula {
namespace meta {

class MetaJobExecutor : public JobExecutor {
 public:
  MetaJobExecutor(JobDescription jobDescription,
                  kvstore::KVStore* kvstore,
                  AdminClient* adminClient,
                  const std::vector<std::string>& paras)
      : JobExecutor(kvstore, jobDescription.getSpace()),
        jobDescription_(jobDescription),
        jobId_(jobDescription.getJobId()),
        adminClient_(adminClient),
        paras_(paras) {}

  virtual ~MetaJobExecutor() = default;

  /**
   * @brief Check the arguments about the job.
   *
   * @return
   */
  nebula::cpp2::ErrorCode check() override;

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
  folly::Future<nebula::cpp2::ErrorCode> execute() override;

  /**
   * @brief Stop the job when the user cancel it.
   *
   * @return
   */
  nebula::cpp2::ErrorCode stop() override;

  nebula::cpp2::ErrorCode finish(bool) override;

  bool isMetaJob() override;

  nebula::cpp2::ErrorCode recovery() override;

  nebula::cpp2::ErrorCode saveSpecialTaskStatus(const cpp2::ReportTaskReq&) override;

  /**
   * @brief return job description
   *
   */
  JobDescription getJobDescription() override {
    return jobDescription_;
  }

  bool isRunning() override {
    return isRunning_.load();
  }
  void resetRunningStatus() override {
    isRunning_.store(false);
  }

 protected:
  virtual folly::Future<nebula::cpp2::ErrorCode> executeInternal();

 protected:
  JobDescription jobDescription_;
  JobID jobId_{INT_MIN};
  TaskID taskId_{0};
  AdminClient* adminClient_{nullptr};
  std::vector<std::string> paras_;
  volatile bool stopped_{false};
  std::mutex muInterrupt_;
  std::condition_variable condInterrupt_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAJOBEXECUTOR_H_
