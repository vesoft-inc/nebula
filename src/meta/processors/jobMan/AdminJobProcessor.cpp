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
    Status status;
    std::vector<std::string> result;
    std::stringstream oss;
    for (auto& p : req.get_paras()) { oss << p << " "; }
    LOG(INFO) << __PRETTY_FUNCTION__ << " paras=" << oss.str();

    JobManager* jobMgr = JobManager::getInstance();
    switch (req.get_op()) {
        case nebula::meta::cpp2::AdminJobOp::ADD:
        {
            auto jobDesc = jobMgr->buildJobDescription(req.get_paras());
            if (jobDesc.ok()) {
                result.emplace_back(std::to_string(jobMgr->addJob(jobDesc.value())));
            } else {
                status = jobDesc.status();
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::SHOW_All:
        {
            auto r = jobMgr->showJobs();
            if (r.ok()) {
                result.swap(r.value());
            } else {
                status = r.status();
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::SHOW:
        {
            if (req.get_paras().empty()) {
                status = Status::SyntaxError("show job needs para");
            }
            int iJob = atoi(req.get_paras()[0].c_str());
            if (iJob == 0) {
                break;
            }
            auto r = jobMgr->showJob(iJob);
            if (r.ok()) {
                result.swap(r.value());
            } else {
                status = r.status();
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::STOP:
        {
            if (req.get_paras().empty()) {
                status = Status::SyntaxError("stop job needs para");
            }
            int iJob = atoi(req.get_paras()[0].c_str());
            if (iJob == 0) {
                break;
            }
            status = jobMgr->stopJob(iJob);
            if (status.ok()) {
                result.emplace_back("job " + std::to_string(iJob) + " stopped");
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::BACKUP:
        {
            if (req.get_paras().size() < 2) {
                status = Status::SyntaxError("invalid paras for backup job");
            }
            int32_t iStart = atoi(req.get_paras()[0].c_str());
            int32_t iStop = atoi(req.get_paras()[1].c_str());
            auto nJobTask = jobMgr->backupJob(iStart, iStop);
            std::string msg = folly::stringPrintf("backup %d job%s %d task%s",
                                                  nJobTask.first,
                                                  nJobTask.first > 0 ? "s" : "",
                                                  nJobTask.second,
                                                  nJobTask.second > 0 ? "s" : "");
            result.emplace_back(msg);
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::RECOVER:
        {
            int32_t jobRecovered = jobMgr->recoverJob();
            result.emplace_back(folly::stringPrintf("%d job recovered", jobRecovered));
            break;
        }
        default:
            break;
    }

    if (!status.ok()) {
        resp_.set_code(to(status));
        onFinished();
        return;
    }
    resp_.set_result(std::move(result));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

