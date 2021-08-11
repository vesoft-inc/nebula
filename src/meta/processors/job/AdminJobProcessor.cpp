/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/StatusOr.h"
#include "meta/processors/jobMan/AdminJobProcessor.h"
#include "meta/processors/jobMan/JobManager.h"
#include "meta/processors/jobMan/JobDescription.h"

namespace nebula {
namespace meta {

void AdminJobProcessor::process(const cpp2::AdminJobReq& req) {
    cpp2::AdminJobResult result;
    auto errorCode = nebula::cpp2::ErrorCode::SUCCEEDED;
    std::stringstream oss;
    oss << "op = " << apache::thrift::util::enumNameSafe(req.get_op());
    if (req.get_op() == nebula::meta::cpp2::AdminJobOp::ADD) {
        oss << ", cmd = " << apache::thrift::util::enumNameSafe(req.get_cmd());
    }
    oss << ", paras.size()=" << req.get_paras().size();
    for (auto& p : req.get_paras()) {
        oss << " " << p;
    }
    LOG(INFO) << __func__ << "() " << oss.str();

    JobManager* jobMgr = JobManager::getInstance();
    switch (req.get_op()) {
        case nebula::meta::cpp2::AdminJobOp::ADD:
        {
            auto cmd = req.get_cmd();
            auto paras = req.get_paras();
            if (cmd == cpp2::AdminCmd::REBUILD_TAG_INDEX ||
                cmd == cpp2::AdminCmd::REBUILD_EDGE_INDEX ||
                cmd == cpp2::AdminCmd::STATS) {
                if (paras.empty()) {
                    LOG(ERROR) << "Parameter should be not empty";
                    errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
                    break;
                }
            }

            JobID jId = 0;
            auto jobExist = jobMgr->checkJobExist(cmd, paras, jId);
            if (jobExist) {
                LOG(INFO) << "Job has already exists: " << jId;
                result.set_job_id(jId);
                break;
            }

            // Job not exists
            auto jobId = autoIncrementId();
            if (!nebula::ok(jobId)) {
                errorCode = nebula::error(jobId);
                break;
            }

            JobDescription jobDesc(nebula::value(jobId), cmd, paras);
            errorCode = jobMgr->addJob(jobDesc, adminClient_);
            if (errorCode == nebula::cpp2::ErrorCode::SUCCEEDED) {
                result.set_job_id(nebula::value(jobId));
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::SHOW_All:
        {
            auto ret = jobMgr->showJobs();
            if (nebula::ok(ret)) {
                result.set_job_desc(nebula::value(ret));
            } else {
                errorCode = nebula::error(ret);
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::SHOW:
        {
            if (req.get_paras().empty()) {
                LOG(ERROR) << "Parameter should be not empty";
                errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
                break;
            }

            int iJob = atoi(req.get_paras()[0].c_str());
            if (iJob == 0) {
                LOG(ERROR) << "Show job should have parameter as the job ID";
                errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
                break;
            }

            auto ret = jobMgr->showJob(iJob);
            if (nebula::ok(ret)) {
                result.set_job_desc(std::vector<cpp2::JobDesc>{nebula::value(ret).first});
                result.set_task_desc(nebula::value(ret).second);
            } else {
                errorCode = nebula::error(ret);
            }
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::STOP:
        {
            if (req.get_paras().empty()) {
                LOG(ERROR) << "Parameter should be not empty";
                errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
                break;
            }
            int iJob = atoi(req.get_paras()[0].c_str());
            if (iJob == 0) {
                LOG(ERROR) << "Stop job should have parameter as the job ID";
                errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
                break;
            }
            errorCode = jobMgr->stopJob(iJob);
            break;
        }
        case nebula::meta::cpp2::AdminJobOp::RECOVER:
        {
            auto ret = jobMgr->recoverJob();
            if (nebula::ok(ret)) {
                result.set_recovered_job_num(nebula::value(ret));
            } else {
                errorCode = nebula::error(ret);
            }
            break;
        }
        default:
            errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
            break;
    }

    if (errorCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(errorCode);
        onFinished();
        return;
    }
    resp_.set_result(std::move(result));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

