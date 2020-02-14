/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVJOBDESCRIPTION_H_
#define META_KVJOBDESCRIPTION_H_

#include <string>
#include <vector>

#include <folly/Range.h>
#include <gtest/gtest_prod.h>

#include "meta/processors/jobMan/JobStatus.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/KVStore.h"

namespace nebula {
namespace meta {

class JobDescription {
    FRIEND_TEST(JobDescriptionTest, parseKey);
    FRIEND_TEST(JobDescriptionTest, parseVal);
    FRIEND_TEST(JobManagerTest, buildJobDescription);
    FRIEND_TEST(JobManagerTest, addJob);
    FRIEND_TEST(JobManagerTest, loadJobDescription);
    FRIEND_TEST(JobManagerTest, showJobs);
    FRIEND_TEST(JobManagerTest, showJob);
    FRIEND_TEST(JobManagerTest, backupJob);
    FRIEND_TEST(JobManagerTest, recoverJob);

    using Status = cpp2::JobStatus;

public:
    JobDescription() = default;
    JobDescription(int32_t id,
                   std::string type,
                   std::vector<std::string> paras,
                   Status status = Status::QUEUE,
                   int64_t = 0,
                   int64_t = 0);

    /*
     * return the JobDescription if both key & val is valid
     * */
    static folly::Optional<JobDescription>
    makeJobDescription(folly::StringPiece key, folly::StringPiece val);

    int32_t getJobId() const { return id_; }

    /*
     * return the command for this job. (e.g. compact, flush ...)
     * */
    std::string getCmd() const { return cmd_; }

    /*
     * return the paras for this job. (e.g. space name for compact/flush)
     * */
    std::vector<std::string> getParas() const { return paras_; }

    /*
     * return the status (e.g. Queue, running, finished, failed, stopped);
     * */
    Status getStatus() { return status_; }

    /*
     * return the key write to kv store
     * */
    std::string jobKey() const;

    /*
     * return the val write to kv store
     * */
    std::string jobVal() const;

    /*
     * return the key write to kv store, while calling "backup job"
     * */
    std::string archiveKey();

    /*
     * set the internal status
     * will check if newStatus is later than curr Status
     * e.g. set running to a finished job is forbidden
     *
     * will set start time if newStatus is running
     * will set stop time if newStatus is finished / failed / stopped
     * */
    bool setStatus(Status newStatus);

    /*
     * get a existed job from kvstore, return folly::none if there isn't
     * */
    static folly::Optional<JobDescription>
    loadJobDescription(int32_t iJob, nebula::kvstore::KVStore* kv);

    /*
     * compose a key write to kvstore, according to the given job id
     * */
    static std::string makeJobKey(int32_t iJob);

    /*
     * write out all job details in human readable strings
     * if a job is
     * =====================================================================================
     * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time         |
     * =====================================================================================
     * | 27             | flush nba     | finished | 12/09/19 11:09:40 | 12/09/19 11:09:40 |
     * -------------------------------------------------------------------------------------
     * then, the vector should be
     * {27, flush nba, finished, 12/09/19 11:09:40, 12/09/19 11:09:40}
     * */
    cpp2::JobDesc toJobDesc();

    /*
     * decode key from kvstore, return job id
     * */
    static int32_t parseKey(const folly::StringPiece& rawKey);

    /*
     * decode val from kvstore, return
     * {command, paras, status, start time, stop time}
     * */
    static std::tuple<std::string, std::vector<std::string>, Status, int64_t, int64_t>
    parseVal(const folly::StringPiece& rawVal);

    /*
     * check if the given rawKey is a valid JobKey
     * */
    static bool isJobKey(const folly::StringPiece& rawKey);

private:
    int32_t                         id_;
    std::string                     cmd_;  // compact, flush ...
    std::vector<std::string>        paras_;
    Status                          status_;
    int64_t                         startTime_;
    int64_t                         stopTime_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBDESCRIPTION_H_
