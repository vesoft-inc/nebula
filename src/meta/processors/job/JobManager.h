/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_JOBMANAGER_H_
#define META_JOBMANAGER_H_

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/concurrency/PriorityUnboundedQueueSet.h>
#include <gtest/gtest_prod.h>

#include <boost/core/noncopyable.hpp>
#include <memory>

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/stats/StatsManager.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/job/JobDescription.h"
#include "meta/processors/job/JobExecutor.h"
#include "meta/processors/job/JobStatus.h"
#include "meta/processors/job/StorageJobExecutor.h"
#include "meta/processors/job/TaskDescription.h"

namespace nebula {
namespace meta {
extern stats::CounterId kNumRunningJobs;

class JobManager : public boost::noncopyable, public nebula::cpp::NonMovable {
  friend class JobManagerTest;
  friend class GetStatsTest;
  friend class CreateBackupProcessorTest;
  friend class SnapshotProcessorsTest;
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
  FRIEND_TEST(JobManagerTest, DownloadJob);
  FRIEND_TEST(JobManagerTest, IngestJob);
  FRIEND_TEST(JobManagerTest, StopJob);
  FRIEND_TEST(JobManagerTest, NotStoppableJob);
  FRIEND_TEST(JobManagerTest, StoppableJob);
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

  enum class JbPriority : size_t {
    kHIGH = 0x00,
    kLOW = 0x01,
  };

  /**
   * @brief Init task queue, kvStore and schedule thread
   * @param store
   * @param adminClient
   * @return true if the init is successful
   */
  bool init(nebula::kvstore::KVStore* store,
            AdminClient* adminClient,
            std::shared_ptr<JobExecutorFactory> factory = std::make_shared<JobExecutorFactory>());

  /**
   * @brief Called when receive a system signal
   */
  void shutDown();

  /**
   * @brief Load job description from kvstore
   *
   * @param jobDesc
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode addJob(JobDescription jobDesc);

  /**
   * @brief The same job in inFlightJobs_.
   * Jobs in inFlightJobs_ have three status:
   * QUEUE: when adding a job
   * FAILED or STOPPED: when recover job
   *
   * @param spaceId
   * @param type
   * @param paras
   * @param jobId If the job exists, jobId is the id of the existing job
   * @return True if job exists.
   */
  bool checkOnRunningJobExist(GraphSpaceID spaceId,
                              const cpp2::JobType& type,
                              const std::vector<std::string>& paras,
                              JobID& jobId);
  /**
   * @brief In the current space, if there is a failed data balance job,
   * need to recover the job first, otherwise cannot add this type of job.
   *
   * @param spaceId
   * @param jobType
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode checkNeedRecoverJobExist(GraphSpaceID spaceId,
                                                   const cpp2::JobType& jobType);

  /**
   * @brief Load all jobs of the space from kvStore and convert to cpp2::JobDesc
   *
   * @param spaceId
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::JobDesc>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::JobDesc>> showJobs(GraphSpaceID spaceId);

  /**
   * @brief Load one job and related tasks from kvStore and convert to cpp2::JobDesc
   *
   * @param spaceId
   * @param jobId
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>>> showJob(
      GraphSpaceID spaceId, JobID jobId);

  /**
   * @brief Stop a job when user cancel it
   *
   * @param spaceId
   * @param jobId
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode stopJob(GraphSpaceID spaceId, JobID jobId);

  /**
   * @brief Number of recovered jobs
   *
   * @param spaceId
   * @param client
   * @param jobIds
   * @return Return error/recovered job num
   */
  ErrorOr<nebula::cpp2::ErrorCode, uint32_t> recoverJob(GraphSpaceID spaceId,
                                                        const std::vector<int32_t>& jobIds = {});

  /**
   * @brief Persist job executed result, and do the cleanup
   *
   * @param spaceId
   * @param jobId
   * @param jobStatus Will be one of the FINISHED, FAILED or STOPPED.
   * @param jobErrorCode Will be specified when the job failed before any tasks executed, e.g. when
   * check or prepare.
   * @return cpp2::ErrorCode if error when write to kv store
   */
  nebula::cpp2::ErrorCode jobFinished(GraphSpaceID spaceId,
                                      JobID jobId,
                                      cpp2::JobStatus jobStatus,
                                      std::optional<nebula::cpp2::ErrorCode> jobErrorCode =
                                          std::optional<nebula::cpp2::ErrorCode>());

  /**
   * @brief Report task finished.
   *
   * @param req
   * @return cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode reportTaskFinish(const cpp2::ReportTaskReq& req);

  /**
   * @brief Only used for Test
   * The number of jobs in all spaces
   *
   * @return
   */
  size_t jobSize() const;

  /**
   * @brief Traverse from priorityQueues_, and find the priorityQueue of the space
   * that is not executing the job. Then take a job from the queue according to the priority.
   *
   * @param opJobId
   * @return return true if the element is obtained, otherwise return false.
   */
  bool tryDequeue(std::tuple<JbOp, JobID, GraphSpaceID>& opJobId);

  /**
   * @brief Enter different priority queues according to the command type and space
   *
   * @param spaceId SpaceId of the job
   * @param jobId Id of the job
   * @param op Recover a job or add a new one
   * @param jobType Type of the job
   */
  void enqueue(GraphSpaceID spaceId, JobID jobId, const JbOp& op, const cpp2::JobType& jobType);

  /**
   * @brief Check whether the job of the specified type in all spaces is running
   *
   * @param jobTypes Cmd types of the job
   * @return ErrorOr<nebula::cpp2::ErrorCode, bool>
   */
  ErrorOr<nebula::cpp2::ErrorCode, bool> checkTypeJobRunning(
      std::unordered_set<cpp2::JobType>& jobTypes);

  /**
   * @brief Load jobs that are running before crashed, set status to QUEUE
   * Notice: Only the meta leader can save successfully.
   * Set the QUEUE state for later recover this job.
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
   * @return error code
   */
  folly::Future<nebula::cpp2::ErrorCode> runJobInternal(const JobDescription& jobDesc, JbOp op);

  nebula::cpp2::ErrorCode prepareRunJob(JobExecutor* jobExec,
                                        const JobDescription& jobDesc,
                                        JbOp op);

  ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> getSpaceId(const std::string& name);

  nebula::cpp2::ErrorCode save(const std::string& k, const std::string& v);

  static bool isExpiredJob(JobDescription& jobDesc);

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
  ErrorOr<nebula::cpp2::ErrorCode, std::list<TaskDescription>> getAllTasks(GraphSpaceID spaceId,
                                                                           JobID jobId);

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

  /**
   * @brief reset spaceRunning of spaceId to false
   *
   * @param spaceId
   * @return
   */
  void resetSpaceRunning(GraphSpaceID spaceId);

  /**
   * @brief check if the space exist
   *
   * @return
   */
  bool spaceExist(GraphSpaceID spaceId);

 private:
  using PriorityQueue = folly::PriorityUMPSCQueueSet<std::tuple<JbOp, JobID, GraphSpaceID>, true>;
  // Each PriorityQueue contains high and low priority queues.
  // The lower the value, the higher the priority.
  folly::ConcurrentHashMap<GraphSpaceID, std::unique_ptr<PriorityQueue>> priorityQueues_;

  // Identify whether the current space is running a job
  folly::ConcurrentHashMap<GraphSpaceID, std::atomic<bool>> spaceRunningJobs_;

  folly::ConcurrentHashMap<JobID, std::unique_ptr<JobExecutor>> runningJobs_;
  // The job in running or queue
  folly::ConcurrentHashMap<JobID, JobDescription> inFlightJobs_;
  std::thread bgThread_;
  nebula::kvstore::KVStore* kvStore_{nullptr};
  AdminClient* adminClient_{nullptr};

  folly::ConcurrentHashMap<GraphSpaceID, std::unique_ptr<std::mutex>> muReportFinish_;
  // Start & stop & finish a job need mutual exclusion
  // The reason of using recursive_mutex is that, it's possible for a meta job try to get this lock
  // in finish-callback in the same thread with runJobInternal
  folly::ConcurrentHashMap<GraphSpaceID, std::unique_ptr<std::recursive_mutex>> muJobFinished_;
  std::atomic<JbmgrStatus> status_ = JbmgrStatus::NOT_START;

  std::unique_ptr<folly::Executor> executor_;
  std::shared_ptr<JobExecutorFactory> executorFactory_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOBMANAGER_H_
