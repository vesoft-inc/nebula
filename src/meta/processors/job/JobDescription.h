/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_KVJOBDESCRIPTION_H_
#define META_KVJOBDESCRIPTION_H_

#include <gtest/gtest_prod.h>

#include "common/base/Base.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/JobStatus.h"

namespace nebula {
namespace meta {

class JobDescription {
  using Status = cpp2::JobStatus;

 public:
  JobDescription() = default;

  JobDescription(GraphSpaceID space,
                 JobID id,
                 cpp2::JobType type,
                 std::vector<std::string> paras = {},
                 Status status = Status::QUEUE,
                 int64_t startTime = 0,
                 int64_t stopTime = 0);

  /**
   * @brief Return the JobDescription if both key & val is valid
   *
   * @param key
   * @param val
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, JobDescription> makeJobDescription(
      folly::StringPiece key, folly::StringPiece val);

  /**
   * @brief Get the SpaceId
   *
   * @return GraphSpaceID
   */
  GraphSpaceID getSpace() const {
    return space_;
  }

  /**
   * @brief Get the Job Id
   *
   * @return JobID
   */
  JobID getJobId() const {
    return jobId_;
  }

  /**
   * @brief Return the command for this job. (e.g. compact, flush ...)
   *
   * @return
   */
  cpp2::JobType getJobType() const {
    return type_;
  }

  /**
   * @brief Return the paras for this job. (e.g. space name for compact/flush)
   *
   * @return
   */
  std::vector<std::string> getParas() const {
    return paras_;
  }

  /**
   * @brief Return the status (e.g. Queue, running, finished, failed, stopped);
   *
   * @return
   */
  Status getStatus() const {
    return status_;
  }

  /**
   * @brief
   * Set the internal status
   * Will check if newStatus is later than curr Status
   * e.g. set running to a finished job is forbidden
   *
   * Will set start time if newStatus is running
   * Will set stop time if newStatus is finished / failed / stopped
   *
   * @param newStatus
   * @param force Set status fored and ignore checking
   * @return
   */
  bool setStatus(Status newStatus, bool force = false);

  /**
   * @brief Get the Start Time
   *
   * @return int64_t
   */
  int64_t getStartTime() {
    return startTime_;
  }

  /**
   * @brief Get the Stop Time
   *
   * @return int64_t
   */
  int64_t getStopTime() {
    return stopTime_;
  }

  /**
   * @brief
   * Get a existed job from kvstore, return folly::none if there isn't
   *
   * @param space the spaceId of the job we would load
   * @param iJob Id of the job we would load
   * @param kv Where we load the job from
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, JobDescription> loadJobDescription(
      GraphSpaceID space, JobID iJob, nebula::kvstore::KVStore* kv);

  /**
   * @brief
   * Write out all job details in human readable strings
   * if a job is
   * =====================================================================================
   * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time
   * |
   * =====================================================================================
   * | 27             | flush nba     | finished | 12/09/19 11:09:40 | 12/09/19
   * 11:09:40 |
   * -------------------------------------------------------------------------------------
   * Then, the vector should be
   * {27, flush nba, finished, 12/09/19 11:09:40, 12/09/19 11:09:40}
   *
   * @return
   */
  cpp2::JobDesc toJobDesc();

  bool operator==(const JobDescription& that) const {
    return space_ == that.space_ && type_ == that.type_ && paras_ == that.paras_ &&
           status_ == that.status_;
  }

  bool operator!=(const JobDescription& that) const {
    return !(*this == that);
  }

 private:
  // Because all jobs are space-level, spaceName is not in paras_.
  GraphSpaceID space_;
  JobID jobId_;
  cpp2::JobType type_;
  std::vector<std::string> paras_;
  Status status_;
  int64_t startTime_;
  int64_t stopTime_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBDESCRIPTION_H_
