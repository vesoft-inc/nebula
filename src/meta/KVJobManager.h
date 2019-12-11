/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVJOBMANAGER_H_
#define META_KVJOBMANAGER_H_

#include <string>
#include <gtest/gtest_prod.h>
#include "base/Base.h"
#include "base/ErrorOr.h"
#include "kvstore/NebulaStore.h"
#include "meta/JobDescription.h"

#include "meta/SimpleBlockingQueue.h"

namespace nebula {
namespace meta {

class KVJobManager{
    using ErrOrString = ErrorOr<nebula::kvstore::ResultCode, std::string>;
    using ErrOrInt = ErrorOr<nebula::kvstore::ResultCode, int>;
    FRIEND_TEST(JobFooTest, InternalHelper);
    FRIEND_TEST(JobFooTest, AddJob);
    FRIEND_TEST(JobFooTest, ShowJobs);
    FRIEND_TEST(JobFooTest, StopJob);
    FRIEND_TEST(JobFooTest, BackupJob);

public:
    ~KVJobManager();

    static KVJobManager* getInstance();

    bool init(nebula::kvstore::KVStore* store, nebula::thread::GenericThreadPool *pool);

    void shutDown();

    ErrOrString runJob(const std::string& op, std::vector<std::string>& paras);

private:
    ErrOrString addJobWrapper(std::vector<std::string>& paras);
    ErrOrString showJobsWrapper();
    ErrOrString showJobWrapper(std::vector<std::string>& paras);
    ErrOrString stopJobWrapper(std::vector<std::string>& paras);
    ErrOrString backupJobWrapper(std::vector<std::string>& paras);
    ErrOrString recoverJobWrapper(std::vector<std::string>& paras);

    ErrOrInt addJob(const std::string& type, const std::string& para = 0);
    // int addJob(const std::string& type, const std::string& para = 0);
    std::vector<std::string> showJobs();
    std::vector<std::string> showJob(int iJob);
    int stopJob(int iJob);
    ErrOrInt recoverJob(int iJob);

    void runBackground();
    bool runInternal(int iJob);
    std::pair<int, int> backupJob(int iBegin, int iEnd);

    // simple util functions
    std::string encodeJobKey(int iJob, int iTask = -1);
    std::pair<int, int> decodeJobKey(const std::string& key);

    std::string encodeLenVal(const std::string& src);
    std::pair<int, int> decodeLenVal(const std::string& len_val);

    std::string timeNow();

    // kvstore related
    nebula::kvstore::ResultCode setSingleVal(const std::string& k, const std::string& v);
    ErrorOr<nebula::kvstore::ResultCode, int> reserveJobId();
    int getSpaceId(const std::string& name);
    std::shared_ptr<JobDescription> getJobDescription(int iJob, int iTask = -1);
    nebula::kvstore::ResultCode setJobStatus(int iJob, int iTask,
                                             const std::string& status);
    nebula::kvstore::ResultCode setTaskStatus(int iJob, int iTask, const std::string& dest,
                                              const std::string& status);

private:
    KVJobManager();

    GraphSpaceID        kSpace{0};
    PartitionID         kPart{0};
    std::string         delim_{","};
    std::string         kRowDelimeter{"\n"};
    std::string         kColDelimeter{","};
    std::string         kJobPrefix{"__job_mgr_"};
    std::string         kJobKey;
    std::string         kJobArchive;
    std::string         kCurrId;

    std::thread         executor_;

    SimpleBlockingQueue<int> queue_;
    nebula::kvstore::KVStore* kvStore_;
    nebula::thread::GenericThreadPool *pool_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBMANAGER_H_

