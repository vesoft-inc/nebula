/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <string>
#include <vector>
#include "common/base/StatusOr.h"
#include "meta/processors/jobMan/AdminJobProcessor.h"
#include "meta/processors/jobMan/JobManager.h"
#include "meta/processors/jobMan/JobDescription.h"

namespace nebula {
namespace meta {

void AdminJobProcessor::process(const cpp2::AdminJobReq& req) {
    using vType = std::vector<std::string>;
    StatusOr<vType> result;
    std::stringstream oss;
    for (auto& p : req.get_paras()) { oss << p << " "; }
    LOG(INFO) << __PRETTY_FUNCTION__ << " paras=" << oss.str();

    JobManager* jobMgr = JobManager::getInstance();
    switch (req.get_op()) {
        case nebula::meta::cpp2::AdminJobOp::ADD:
        {
            auto jobDesc = jobMgr->buildJobDescription(req.get_paras());
            if (!jobDesc.ok()) {
                result.value().emplace_back(jobDesc.status().toString());
                break;
            }
            auto iJob = jobMgr->addJob(jobDesc.value());
            result.value().emplace_back(std::to_string(iJob));
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::SHOW_All:
        {
            result = jobMgr->showJobs();
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::SHOW:
        {
            int iJob = atoi(req.get_paras()[0].c_str());
            if (iJob == 0) { break; }
            result = jobMgr->showJob(iJob);
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::STOP:
        {
            int iJob = atoi(req.get_paras()[0].c_str());
            if (iJob == 0) { break; }
            if (jobMgr->stopJob(iJob) == Status::OK()) {
                result.value().emplace_back("job " + std::to_string(iJob) + " stopped");
            } else {
                result.value().emplace_back("invalid job id " + std::to_string(iJob));
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::BACKUP:
        {
            int32_t iStart = atoi(req.get_paras()[0].c_str());
            int32_t iStop = atoi(req.get_paras()[1].c_str());
            auto nJobTask =  jobMgr->backupJob(iStart, iStop);
            std::string msg = folly::stringPrintf("backup %d job%s %d task%s",
                                                  nJobTask.first,
                                                  nJobTask.first > 0 ? "s" : "",
                                                  nJobTask.second,
                                                  nJobTask.second > 0 ? "s" : "");
            result.value().emplace_back(msg);
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::RECOVER:
        {
            result =  jobMgr->recoverJob();
            break;
        }
        default:
            break;
    }

    if (!result.ok()) {
        resp_.set_code(to(result.status()));
        onFinished();
        return;
    }
    resp_.set_result(std::move(result.value()));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

