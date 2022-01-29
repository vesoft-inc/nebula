/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_JOBMANAGER_H_
#define META_JOBMANAGER_H_

#include <boost/core/noncopyable.hpp>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <gtest/gtest_prod.h>


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
  FRIEND_TEST(JobManagerTest, reserveJobId);
  FRIEND_TEST(JobManagerTest, buildJobDescription);
  FRIEND_TEST(JobManagerTest, addJob);
  FRIEND_TEST(JobManagerTest, StatsJob);
  FRIEND_TEST(JobManagerTest, JobPriority);
  FRIEND_TEST(JobManagerTest, JobDeduplication);
  FRIEND_TEST(JobManagerTest, loadJobDescription);
  FRIEND_TEST(JobManagerTest, showJobs);
  FRIEND_TEST(JobManagerTest, showJobsFromMultiSpace);
  FRIEND_TEST(JobManagerTest, showJob);
  FRIEND_TEST(JobManagerTest, showJobInOtherSpace);
  FRIEND_TEST(JobManagerTest, recoverJob);
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

  bool init(nebula::kvstore::KVStore* store);

  void shutDown();

  /*
   * Load job description from kvstore
   * */
  nebula::cpp2::ErrorCode addJob(const JobDescription& jobDesc, AdminClient* client);

  /*
   * The same job is in jobMap
   */
  bool checkJobExist(const cpp2::AdminCmd& cmd, const std::vector<std::string>& paras, JobID& iJob);

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::JobDesc>> showJobs(
      const std::string& spaceName);

  ErrorOr<nebula::cpp2::ErrorCode, std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>>> showJob(
      JobID iJob, const std::string& spaceName);

  nebula::cpp2::ErrorCode stopJob(JobID iJob, const std::string& spaceName);

  // return error/recovered job num
  ErrorOr<nebula::cpp2::ErrorCode, uint32_t> recoverJob(const std::string& spaceName,
                                                        AdminClient* client,
                                                        const std::vector<int32_t>& jobIds = {});

  /**
   * @brief persist job executed result, and do the cleanup
   * @return cpp2::ErrorCode if error when write to kv store
   */
  nebula::cpp2::ErrorCode jobFinished(JobID jobId, cpp2::JobStatus jobStatus);

  // report task finished.
  nebula::cpp2::ErrorCode reportTaskFinish(const cpp2::ReportTaskReq& req);

  // Only used for Test
  // The number of jobs in lowPriorityQueue_ a and highPriorityQueue_
  size_t jobSize() const;

  // Tries to extract an element from the front of the highPriorityQueue_,
  // if failed, then extract an element from lowPriorityQueue_.
  // If the element is obtained, return true, otherwise return false.
  bool try_dequeue(std::pair<JbOp, JobID>& opJobId);

  // Enter different priority queues according to the command type
  void enqueue(const JbOp& op, const JobID& jobId, const cpp2::AdminCmd& cmd);

  ErrorOr<nebula::cpp2::ErrorCode, bool> checkIndexJobRunning();

  nebula::cpp2::ErrorCode handleRemainingJobs();

 private:
  JobManager() = default;

  void scheduleThread();
  void scheduleThreadOld();

  bool runJobInternal(const JobDescription& jobDesc, JbOp op);
  bool runJobInternalOld(const JobDescription& jobDesc);

  ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> getSpaceId(const std::string& name);

  nebula::cpp2::ErrorCode save(const std::string& k, const std::string& v);

  static bool isExpiredJob(const cpp2::JobDesc& jobDesc);

  static bool isRunningJob(const JobDescription& jobDesc);

  nebula::cpp2::ErrorCode removeExpiredJobs(std::vector<std::string>&& jobKeys);

  ErrorOr<nebula::cpp2::ErrorCode, std::list<TaskDescription>> getAllTasks(JobID jobId);

  void cleanJob(JobID jobId);

  nebula::cpp2::ErrorCode saveTaskStatus(TaskDescription& td, const cpp2::ReportTaskReq& req);

  void compareChangeStatus(JbmgrStatus expected, JbmgrStatus desired);

 private:
  // Todo(pandasheep)
  // When folly is upgraded, PriorityUMPSCQueueSet can be used
  // Use two queues to simulate priority queue, Divide by job cmd
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
