/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_JOBMANAGER_H_
#define META_JOBMANAGER_H_

#include <boost/core/noncopyable.hpp>
#include <string>
#include <gtest/gtest_prod.h>
#include <folly/concurrency/UnboundedQueue.h>
#include "base/Base.h"
#include "base/ErrorOr.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/jobMan/JobStatus.h"
#include "meta/processors/jobMan/JobDescription.h"

#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

class JobManager : public nebula::cpp::NonCopyable, public nebula::cpp::NonMovable {
    FRIEND_TEST(JobManagerTest, reserveJobId);
    FRIEND_TEST(JobManagerTest, buildJobDescription);
    FRIEND_TEST(JobManagerTest, addJob);
    FRIEND_TEST(JobManagerTest, loadJobDescription);
    FRIEND_TEST(JobManagerTest, showJobs);
    FRIEND_TEST(JobManagerTest, showJob);
    FRIEND_TEST(JobManagerTest, backupJob);
    FRIEND_TEST(JobManagerTest, recoverJob);

public:
    ~JobManager();
    static JobManager* getInstance();

    bool init(nebula::kvstore::KVStore* store);

    void shutDown();

    /*
     * Build job description and save it in kvstore.
     * */
    StatusOr<JobDescription> buildJobDescription(const std::vector<std::string>& args);
    /*
     * Load job description from kvstore
     * */
    int32_t addJob(const JobDescription& jobDesc);
    StatusOr<std::vector<cpp2::JobDetails>> showJobs();
    StatusOr<std::pair<cpp2::JobDetails, std::vector<cpp2::TaskDetails>>> showJob(int iJob);
    nebula::Status stopJob(int32_t iJob);
    std::pair<int, int> backupJob(int iBegin, int iEnd);
    int32_t recoverJob();

private:
    JobManager() = default;
    void runJobBackground();
    bool runJobInternal(const JobDescription& jobDesc);
    StatusOr<int32_t> reserveJobId();
    int getSpaceId(const std::string& name);
    nebula::kvstore::ResultCode save(const std::string& k, const std::string& v);

    bool                shutDown_{false};
    std::unique_ptr<folly::UMPSCQueue<int32_t, true>> queue_;
    std::unique_ptr<thread::GenericWorker> bgThread_;

    nebula::kvstore::KVStore* kvStore_{nullptr};
    std::unique_ptr<nebula::thread::GenericThreadPool> pool_{nullptr};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOBMANAGER_H_

