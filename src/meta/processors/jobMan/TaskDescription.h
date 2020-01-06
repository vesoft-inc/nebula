/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_TASKDESCRIPTION_H_
#define META_TASKDESCRIPTION_H_

#include <ctime>
#include <vector>
#include <string>
#include <folly/Range.h>
#include <gtest/gtest_prod.h>
#include "meta/processors/jobMan/JobStatus.h"

/*
 * =====================================================================================
 * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time         |
 * =====================================================================================
 * | 27             | flush nba     | finished | 12/09/19 11:09:40 | 12/09/19 11:09:40 |
 * -------------------------------------------------------------------------------------
 * | 27-0           | 192.168.8.5   | finished | 12/09/19 11:09:40 | 12/09/19 11:09:40 |
 * -------------------------------------------------------------------------------------
 * */

namespace nebula {
namespace meta {

class TaskDescription {
    FRIEND_TEST(TaskDescriptionTest, ctor);
    FRIEND_TEST(TaskDescriptionTest, parseKey);
    FRIEND_TEST(TaskDescriptionTest, parseVal);
    FRIEND_TEST(TaskDescriptionTest, dump);
    FRIEND_TEST(TaskDescriptionTest, ctor2);
    FRIEND_TEST(JobManagerTest, showJob);

public:
    TaskDescription(int32_t iJob, int32_t iTask, const std::string& dest);
    TaskDescription(const folly::StringPiece& key, const folly::StringPiece& val);

    /*
     * encoded key going to write to kvstore
     * kJobKey+jobid+taskid
     * */
    std::string taskKey();

    /*
     * decode jobid and taskid from kv store
     * */
    static std::tuple<int32_t, int32_t> parseKey(const folly::StringPiece& rawKey);

    /*
     * encode task val to write to kvstore
     * */
    std::string taskVal();

    /*
     * decode task val from kvstore
     * should be
     * {host, status, start time, stop time}
     * */
    static std::tuple<std::string, JobStatus::Status,
                      folly::Optional<std::time_t>,
                      folly::Optional<std::time_t>>
    parseVal(const folly::StringPiece& rawVal);

    /*
     * encoded key when dba called "backup jobs"
     * */
    std::string archiveKey();

    /*
     * write out task details in human readable strings
     * if a task is
     * =====================================================================================
     * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time         |
     * =====================================================================================
     * | 27-0           | 192.168.8.5   | finished | 12/09/19 11:09:40 | 12/09/19 11:09:40 |
     * -------------------------------------------------------------------------------------
     *  then the vector should be
     * {27-0, 192.168.8.5, finished, 12/09/19 11:09:40, 12/09/19 11:09:40}
     * */
    std::vector<std::string> dump();

    /*
     * set the internal status
     * will check if newStatus is later than curr Status
     * e.g. set running to a finished job is forbidden
     *
     * will set start time if newStatus is running
     * will set stop time if newStatus is finished / failed / stopped
     * */
    bool setStatus(JobStatus::Status newStatus);

private:
    int32_t                         iJob_;
    int32_t                         iTask_;
    std::string                     dest_;
    JobStatus::Status               status_;
    folly::Optional<std::time_t>    startTime_;
    folly::Optional<std::time_t>    stopTime_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TASKDESCRIPTION_H_
