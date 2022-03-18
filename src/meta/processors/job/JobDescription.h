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
  FRIEND_TEST(JobDescriptionTest, ParseKey);
  FRIEND_TEST(JobDescriptionTest, ParseVal);
  FRIEND_TEST(JobManagerTest, BuildJobDescription);
  FRIEND_TEST(JobManagerTest, AddJob);
  FRIEND_TEST(JobManagerTest, StatsJob);
  FRIEND_TEST(JobManagerTest, LoadJobDescription);
  FRIEND_TEST(JobManagerTest, ShowJobs);
  FRIEND_TEST(JobManagerTest, ShowJob);
  FRIEND_TEST(JobManagerTest, ShowJobsFromMultiSpace);
  FRIEND_TEST(JobManagerTest, ShowJobInOtherSpace);
  FRIEND_TEST(JobManagerTest, BackupJob);
  FRIEND_TEST(JobManagerTest, RecoverJob);
  FRIEND_TEST(GetStatsTest, StatsJob);
  FRIEND_TEST(GetStatsTest, MockSingleMachineTest);
  FRIEND_TEST(GetStatsTest, MockMultiMachineTest);

  using Status = cpp2::JobStatus;

 public:
  JobDescription() = default;
  JobDescription(JobID id,
                 cpp2::JobType type,
                 std::vector<std::string> paras,
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

  JobID getJobId() const {
    return id_;
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
   * @brief Return the key write to kv store
   *
   * @return
   */
  std::string jobKey() const;

  /**
   * @brief Return the val write to kv store
   *
   * @return
   */
  std::string jobVal() const;

  /**
   * @brief Return the key write to kv store, while calling "backup job"
   *
   * @return
   */
  std::string archiveKey();

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
   * @brief
   * Get a existed job from kvstore, reture std::nullopt if there isn't
   *
   * @param iJob Id of the job we would load
   * @param kv Where we load the job from
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, JobDescription> loadJobDescription(
      JobID iJob, nebula::kvstore::KVStore* kv);

  /**
   * @brief
   * Compose a key write to kvstore, according to the given job id
   *
   * @param iJob
   * @return
   */
  static std::string makeJobKey(JobID iJob);

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

  /**
   * @brief Decode key from kvstore, return job id
   *
   * @param rawKey
   * @return
   */
  static int32_t parseKey(const folly::StringPiece& rawKey);

  /**
   * @brief
   * Decode val from kvstore, return
   * {command, paras, status, start time, stop time}
   *
   * @param rawVal
   * @return
   */
  static std::tuple<cpp2::JobType, std::vector<std::string>, Status, int64_t, int64_t> parseVal(
      const folly::StringPiece& rawVal);

  /**
   * @brief
   * Check if the given rawKey is a valid JobKey
   *
   * @param rawKey
   * @return
   */
  static bool isJobKey(const folly::StringPiece& rawKey);

  bool operator==(const JobDescription& that) const {
    return this->type_ == that.type_ && this->paras_ == that.paras_ &&
           this->status_ == that.status_;
  }

  bool operator!=(const JobDescription& that) const {
    return !(*this == that);
  }

 private:
  static bool isSupportedValue(const folly::StringPiece& val);

  /**
   * @brief
   * Decode val if it stored in data ver.1, return
   * {command, paras, status, start time, stop time}
   *
   * @param rawVal
   * @return
   */
  static std::tuple<cpp2::JobType, std::vector<std::string>, Status, int64_t, int64_t> decodeValV1(
      const folly::StringPiece& rawVal);

 private:
  JobID id_;
  cpp2::JobType type_;
  std::vector<std::string> paras_;
  Status status_;
  int64_t startTime_;
  int64_t stopTime_;

  // old job may have different format,
  // will ignore some job if it is too old
  // use a hard coded int mark data ver
  static int32_t minDataVer_;
  static int32_t currDataVer_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBDESCRIPTION_H_
