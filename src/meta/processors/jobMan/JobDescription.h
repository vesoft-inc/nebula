/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVJOBDESCRIPTION_H_
#define META_KVJOBDESCRIPTION_H_

#include <vector>
#include <string>

#include <gtest/gtest_prod.h>
#include "meta/processors/jobMan/JobStatus.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

class JobDescription {
    FRIEND_TEST(JobDescriptionTest, parseKey);
    FRIEND_TEST(JobDescriptionTest, parseVal);
    FRIEND_TEST(JobManagerTest, buildJobDescription);
    FRIEND_TEST(JobManagerTest, loadJobDescription);
    FRIEND_TEST(JobManagerTest, showJobs);
    FRIEND_TEST(JobManagerTest, showJob);

public:
    JobDescription() {}
    JobDescription(int32_t id, const std::string& cmd, std::vector<std::string>& paras);
    JobDescription(const folly::StringPiece& key, const folly::StringPiece& val);

    int32_t getJobId() { return id_; }
    std::string getCommand() { return cmd_; }
    std::vector<std::string> getParas() { return paras_; }
    JobStatus::Status getStatus() { return status_; }

    std::string jobKey() const;
    std::string jobVal() const;
    std::string archiveKey();

    bool setStatus(JobStatus::Status newStatus);

    static const std::string& jobPrefix();
    static const std::string& currJobKey();
    static std::string makeJobKey(int32_t iJob);
    std::vector<std::string> dump();

    static int32_t parseKey(const folly::StringPiece& rawKey);

    static std::tuple<std::string,
                      std::vector<std::string>,
                      JobStatus::Status,
                      std::time_t,
                      std::time_t>
    parseVal(const folly::StringPiece& rawVal);

    static bool isJobKey(const folly::StringPiece& rawKey);

private:
    int32_t                         id_;
    std::string                     cmd_;
    std::vector<std::string>        paras_;
    JobStatus::Status               status_;
    std::time_t                     startTime_;
    std::time_t                     stopTime_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_KVJOBDESCRIPTION_H_
