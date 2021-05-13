/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVJOBDESCRIPTION_H_
#define META_KVJOBDESCRIPTION_H_

#include "common/base/Base.h"
#include <gtest/gtest_prod.h>
#include "common/interface/gen-cpp2/meta_types.h"
#include "meta/processors/jobMan/JobStatus.h"
#include "meta/processors/admin/AdminClient.h"
#include "kvstore/KVStore.h"

namespace nebula {
namespace meta {

class JobDescription {
    FRIEND_TEST(JobDescriptionTest, parseKey);
    FRIEND_TEST(JobDescriptionTest, parseVal);
    FRIEND_TEST(JobManagerTest, buildJobDescription);
    FRIEND_TEST(JobManagerTest, addJob);
    FRIEND_TEST(JobManagerTest, StatisJob);
    FRIEND_TEST(JobManagerTest, loadJobDescription);
    FRIEND_TEST(JobManagerTest, showJobs);
    FRIEND_TEST(JobManagerTest, showJob);
    FRIEND_TEST(JobManagerTest, backupJob);
    FRIEND_TEST(JobManagerTest, recoverJob);
    FRIEND_TEST(GetStatisTest, StatisJob);
    FRIEND_TEST(GetStatisTest, MockSingleMachineTest);
    FRIEND_TEST(GetStatisTest, MockMultiMachineTest);

    using Status = cpp2::JobStatus;

public:
    JobDescription() = default;
    JobDescription(JobID id,
                   cpp2::AdminCmd cmd,
                   std::vector<std::string> paras,
                   Status status = Status::QUEUE,
                   int64_t startTime = 0,
                   int64_t stopTime  = 0);

    /*
     * return the JobDescription if both key & val is valid
     * */
    static ErrorOr<nebula::cpp2::ErrorCode, JobDescription>
    makeJobDescription(folly::StringPiece key, folly::StringPiece val);

    JobID getJobId() const { return id_; }

    /*
     * return the command for this job. (e.g. compact, flush ...)
     * */
    cpp2::AdminCmd getCmd() const { return cmd_; }

    /*
     * return the paras for this job. (e.g. space name for compact/flush)
     * */
    std::vector<std::string> getParas() const { return paras_; }

    /*
     * return the status (e.g. Queue, running, finished, failed, stopped);
     * */
    Status getStatus() const { return status_; }

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
    static ErrorOr<nebula::cpp2::ErrorCode, JobDescription>
    loadJobDescription(JobID iJob, nebula::kvstore::KVStore* kv);

    /*
     * compose a key write to kvstore, according to the given job id
     * */
    static std::string makeJobKey(JobID iJob);

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
    static std::tuple<cpp2::AdminCmd,
                      std::vector<std::string>,
                      Status, int64_t, int64_t>
    parseVal(const folly::StringPiece& rawVal);

    /*
     * check if the given rawKey is a valid JobKey
     * */
    static bool isJobKey(const folly::StringPiece& rawKey);

    bool operator==(const JobDescription& that) const {
        return this->cmd_ == that.cmd_ &&
               this->paras_ == that.paras_ &&
               this->status_ == that.status_;
    }

    bool operator!=(const JobDescription& that) const {
        return !(*this == that);
    }

private:
    static bool isSupportedValue(const folly::StringPiece& val);
    /*
     * decode val if it stored in data ver.1, return
     * {command, paras, status, start time, stop time}
     * */
    static std::tuple<cpp2::AdminCmd,
                      std::vector<std::string>,
                      Status, int64_t, int64_t>
    decodeValV1(const folly::StringPiece& rawVal);

private:
    JobID                           id_;
    cpp2::AdminCmd                  cmd_;
    std::vector<std::string>        paras_;
    Status                          status_;
    int64_t                         startTime_;
    int64_t                         stopTime_;

    // old job may have different format,
    // will ignore some job if it is too old
    // use a hard coded int mark data ver
    static int32_t                  minDataVer_;
    static int32_t                  currDataVer_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBDESCRIPTION_H_
