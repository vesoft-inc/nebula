/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_JOBMANAGER_H_
#define META_JOBMANAGER_H_

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <gtest/gtest_prod.h>

#include <boost/core/noncopyable.hpp>

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/stats/StatsManager.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/job/JobDescription.h"
#include "meta/processors/job/JobStatus.h"
#include "meta/processors/job/StorageJobExecutor.h"
#include "meta/processors/job/TaskDescription.h"

namespace nebula {
namespace meta {
extern stats::CounterId kNumRunningJobs;

class JobManager : public boost::noncopyable, public nebula::cpp::NonMovable {
  friend class JobManagerTest;
  friend class GetStatsTest;
  FRIEND_TEST(JobManagerTest, AddJob);
  FRIEND_TEST(JobManagerTest, StatsJob);
  FRIEND_TEST(JobManagerTest, JobPriority);
  FRIEND_TEST(JobManagerTest, JobDeduplication);
  FRIEND_TEST(JobManagerTest, LoadJobDescription);
  FRIEND_TEST(JobManagerTest, ShowJobs);
  FRIEND_TEST(JobManagerTest, ShowJobsFromMultiSpace);
  FRIEND_TEST(JobManagerTest, ShowJob);
  FRIEND_TEST(JobManagerTest, ShowJobInOtherSpace);
  FRIEND_TEST(JobManagerTest, RecoverJob);
  FRIEND_TEST(JobManagerTest, AddRebuildTagIndexJob);
  FRIEND_TEST(JobManagerTest, AddRebuildEdgeIndexJob);
  FRIEND_TEST(GetStatsTest, StatsJob);
  FRIEND_TEST(GetStatsTest, MockSingleMachineTest);
  FRIEND_TEST(GetStatsTest, MockMultiMachineTest);
  friend struct JobCallBack;

 public:
  ~JobManager();
  static JobManager* getInstance();

  enum class JbmgrStatus {
    NOT_START,
    IDLE,  // Job manager started, no running any job
    BUSY,  // Job manager is running job
    STOPPED,
  };

  enum class JbOp {
    ADD,
    RECOVER,
  };

  /**
   * @brief Init task queue, kvStore and schedule thread
   *
   * @param store
   * @return
   */
  bool init(nebula::kvstore::KVStore* store);

  /**
   * @brief Called when receive a system signal
   */
  void shutDown();

  /**
   * @brief Load job description from kvstore
   *
   * @param jobDesc
   * @param client
   * @return
   */
  nebula::cpp2::ErrorCode addJob(const JobDescription& jobDesc, AdminClient* client);

  /**
   * @brief The same job is in jobMap
   *
   * @param type
   * @param paras
   * @param iJob
   * @return
   */
  bool checkJobExist(const cpp2::JobType& type, const std::vector<std::string>& paras, JobID& iJob);

  /**
   * @brief Load all jobs of the space from kvStore and convert to cpp2::JobDesc
   *
   * @param spaceName
   * @return
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::JobDesc>> showJobs(
      const std::string& spaceName);

  /**
   * @brief Load one job and related tasks from kvStore and convert to cpp2::JobDesc
   *
   * @param iJob
   * @param spaceName
   * @return
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>>> showJob(
      JobID iJob, const std::string& spaceName);

  /**
   * @brief Stop a job when user cancel it
   *
   * @param iJob
   * @param spaceName
   * @return
   */
  nebula::cpp2::ErrorCode stopJob(JobID iJob, const std::string& spaceName);

  /**
   * @brief
   *
   * @param spaceName
   * @param client
   * @param jobIds
   * @return Return error/recovered job num
   */
  ErrorOr<nebula::cpp2::ErrorCode, uint32_t> recoverJob(const std::string& spaceName,
                                                        AdminClient* client,
                                                        const std::vector<int32_t>& jobIds = {});

  /**
   * @brief Persist job executed result, and do the cleanup
   *
   * @param jobId
   * @param jobStatus
   * @return cpp2::ErrorCode if error when write to kv store
   */
  nebula::cpp2::ErrorCode jobFinished(JobID jobId, cpp2::JobStatus jobStatus);

  /**
   * @brief Report task finished.
   *
   * @param req
   * @return
   */
  nebula::cpp2::ErrorCode reportTaskFinish(const cpp2::ReportTaskReq& req);

  /**
   * @brief Only used for Test
   * The number of jobs in lowPriorityQueue_ a and highPriorityQueue_
   *
   * @return
   */
  size_t jobSize() const;

  /**
   * @brief Tries to extract an element from the front of the highPriorityQueue_,
   * If failed, then extract an element from lowPriorityQueue_.
   * If the element is obtained, return true, otherwise return false.
   *
   * @param opJobId
   * @return
   */
  bool try_dequeue(std::pair<JbOp, JobID>& opJobId);

  /**
   * @brief Enter different priority queues according to the command type
   *
   * @param op Recover a job or add a new one
   * @param jobId Id of the job
   * @param jobType Type of the job
   */
  void enqueue(const JbOp& op, const JobID& jobId, const cpp2::JobType& jobType);

  /**
   * @brief Check if there is a rebuild_tag_index or rebuild_edge_index running
   *
   * @return
   */
  ErrorOr<nebula::cpp2::ErrorCode, bool> checkIndexJobRunning();

  /**
   * @brief Load jobs that are running before crashed and add them into queue
   *
   * @return
   */
  nebula::cpp2::ErrorCode handleRemainingJobs();

 private:
  JobManager() = default;

  void scheduleThread();

  /**
   * @brief Dispatch all tasks of one job
   *
   * @param jobDesc
   * @param op
   * @return true if all task dispatched, else false.
   */
  bool runJobInternal(const JobDescription& jobDesc, JbOp op);

  ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> getSpaceId(const std::string& name);

  nebula::cpp2::ErrorCode save(const std::string& k, const std::string& v);

  static bool isExpiredJob(const cpp2::JobDesc& jobDesc);

  static bool isRunningJob(const JobDescription& jobDesc);

  /**
   * @brief Remove jobs of the given keys
   *
   * @param jobKeys
   * @return
   */
  nebula::cpp2::ErrorCode removeExpiredJobs(std::vector<std::string>&& jobKeys);

  /**
   * @brief Get all tasks of this job
   *
   * @param jobId
   * @return
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::list<TaskDescription>> getAllTasks(JobID jobId);

  /**
   * @brief Remove a job from the queue and running map
   *
   * @param jobId
   */
  void cleanJob(JobID jobId);

  /**
   * @brief Save a task into kvStore
   *
   * @param td
   * @param req
   * @return
   */
  nebula::cpp2::ErrorCode saveTaskStatus(TaskDescription& td, const cpp2::ReportTaskReq& req);

  /**
   * @brief Cas operation to set status
   *
   * @param expected
   * @param desired
   */
  void compareChangeStatus(JbmgrStatus expected, JbmgrStatus desired);

 private:
  // Todo(pandasheep)
  // When folly is upgraded, PriorityUMPSCQueueSet can be used
  // Use two queues to simulate priority queue, Divide by job type
  std::unique_ptr<folly::UMPSCQueue<std::pair<JbOp, JobID>, true>> lowPriorityQueue_;
  std::unique_ptr<folly::UMPSCQueue<std::pair<JbOp, JobID>, true>> highPriorityQueue_;
  std::map<JobID, std::unique_ptr<JobExecutor>> runningJobs_;

  // The job in running or queue
  folly::ConcurrentHashMap<JobID, JobDescription> inFlightJobs_;
  std::thread bgThread_;
  nebula::kvstore::KVStore* kvStore_{nullptr};
  AdminClient* adminClient_{nullptr};

  std::mutex muReportFinish_;
  // The reason of using recursive_mutex is that, it's possible for a meta job try to get this lock
  // in finish-callback in the same thread with runJobInternal
  std::recursive_mutex muJobFinished_;
  std::atomic<JbmgrStatus> status_ = JbmgrStatus::NOT_START;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOBMANAGER_H_
