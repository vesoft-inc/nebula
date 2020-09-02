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
    FRIEND_TEST(JobManagerTest, recoverJob);

    using ResultCode = nebula::kvstore::ResultCode;

public:
    ~JobManager();
    static JobManager* getInstance();

    enum class Status {
        NOT_START,
        RUNNING,
        STOPPED,
    };

    bool init(nebula::kvstore::KVStore* store);

    void shutDown();

    /*
     * Load job description from kvstore
     * */
    ResultCode addJob(const JobDescription& jobDesc);
    ErrorOr<ResultCode, std::vector<cpp2::JobDesc>> showJobs();
    ErrorOr<ResultCode, std::pair<cpp2::JobDesc, std::vector<cpp2::TaskDesc>>> showJob(int iJob);
    ResultCode stopJob(int32_t iJob);
    ErrorOr<ResultCode, int32_t> recoverJob();

private:
    JobManager() = default;
    void runJobBackground();
    bool runJobInternal(const JobDescription& jobDesc);
    int getSpaceId(const std::string& name);
    nebula::kvstore::ResultCode save(const std::string& k, const std::string& v);

    static bool isExpiredJob(const cpp2::JobDesc& jobDesc);
    void removeExpiredJobs(const std::vector<std::string>& jobKeys);
    std::unique_ptr<folly::UMPSCQueue<int32_t, true>> queue_;
    std::unique_ptr<thread::GenericWorker> bgThread_;

    std::mutex  statusGuard_;
    Status status_{Status::NOT_START};
    nebula::kvstore::KVStore* kvStore_{nullptr};
    std::unique_ptr<nebula::thread::GenericThreadPool> pool_{nullptr};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOBMANAGER_H_

