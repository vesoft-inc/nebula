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
    cpp2::AdminJobResult result;
    cpp2::ErrorCode errorCode = cpp2::ErrorCode::SUCCEEDED;
    std::stringstream oss;
    for (auto& p : req.get_paras()) {
        oss << p << " ";
    }
    LOG(INFO) << __PRETTY_FUNCTION__ << " paras = " << oss.str();

    JobManager* jobMgr = JobManager::getInstance();
    switch (req.get_op()) {
        case nebula::meta::cpp2::AdminJobOp::ADD:
        {
            auto jobId = autoIncrementId();
            if (!nebula::ok(jobId)) {
                errorCode = nebula::error(jobId);
                break;
            }

            std::vector<std::string> cmdAndParas = req.get_paras();
            if (cmdAndParas.empty()) {
                errorCode = cpp2::ErrorCode::E_INVALID_PARM;
                break;
            }

            std::string cmd = cmdAndParas[0];
            std::vector<std::string> paras(cmdAndParas.begin() + 1,
                                           cmdAndParas.end());

            JobDescription jobDesc(nebula::value(jobId), cmd, paras);
            auto rc = jobMgr->addJob(jobDesc);
            if (rc == nebula::kvstore::SUCCEEDED) {
                result.set_job_id(nebula::value(jobId));
            } else {
                errorCode = MetaCommon::to(rc);
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::SHOW_All:
        {
            auto ret = jobMgr->showJobs();
            if (nebula::ok(ret)) {
                result.set_job_desc(nebula::value(ret));
            } else {
                errorCode = MetaCommon::to(nebula::error(ret));
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::SHOW:
        {
            if (req.get_paras().empty()) {
                errorCode = cpp2::ErrorCode::E_INVALID_PARM;
                break;
            }

            int iJob = atoi(req.get_paras()[0].c_str());
            if (iJob == 0) {
                errorCode = cpp2::ErrorCode::E_INVALID_PARM;
                break;
            }

            auto ret = jobMgr->showJob(iJob);
            if (nebula::ok(ret)) {
                result.set_job_desc(std::vector<cpp2::JobDesc>{nebula::value(ret).first});
                result.set_task_desc(nebula::value(ret).second);
            } else {
                errorCode = MetaCommon::to(nebula::error(ret));
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::STOP:
        {
            if (req.get_paras().empty()) {
                errorCode = cpp2::ErrorCode::E_INVALID_PARM;
                break;
            }
            int iJob = atoi(req.get_paras()[0].c_str());
            if (iJob == 0) {
                errorCode = cpp2::ErrorCode::E_INVALID_PARM;
                break;
            }
            auto ret = jobMgr->stopJob(iJob);
            if (nebula::kvstore::ResultCode::SUCCEEDED != ret) {
                errorCode = MetaCommon::to(ret);
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::RECOVER:
        {
            auto ret = jobMgr->recoverJob();
            if (nebula::ok(ret)) {
                result.set_recovered_job_num(nebula::value(ret));
            } else {
                errorCode = MetaCommon::to(nebula::error(ret));
            }
            break;
        }
        default:
            errorCode = cpp2::ErrorCode::E_INVALID_PARM;
            break;
    }

    if (errorCode != cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(errorCode);
        onFinished();
        return;
    }
    resp_.set_result(std::move(result));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

