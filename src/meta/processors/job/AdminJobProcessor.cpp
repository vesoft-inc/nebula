/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/AdminJobProcessor.h"

#include "common/base/StatusOr.h"
#include "common/stats/StatsManager.h"
#include "meta/processors/job/JobDescription.h"

namespace nebula {
namespace meta {

void AdminJobProcessor::process(const cpp2::AdminJobReq& req) {
  std::stringstream oss;
  oss << "op = " << apache::thrift::util::enumNameSafe(req.get_op());
  if (req.get_op() == nebula::meta::cpp2::JobOp::ADD) {
    oss << ", type = " << apache::thrift::util::enumNameSafe(req.get_type());
  }

  auto paras = req.get_paras();
  oss << ", paras.size()=" << paras.size();
  for (auto& p : paras) {
    oss << " " << p;
  }
  LOG(INFO) << __func__ << "() " << oss.str();

  // Check if space not exists
  spaceId_ = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId_);

  cpp2::AdminJobResult result;
  auto errorCode = nebula::cpp2::ErrorCode::SUCCEEDED;
  jobMgr_ = JobManager::getInstance();

  switch (req.get_op()) {
    case nebula::meta::cpp2::JobOp::ADD: {
      errorCode = addJobProcess(req, result);
      break;
    }
    case nebula::meta::cpp2::JobOp::SHOW_All: {
      auto ret = jobMgr_->showJobs(spaceId_);
      if (nebula::ok(ret)) {
        result.job_desc_ref() = nebula::value(ret);
      } else {
        errorCode = nebula::error(ret);
      }
      break;
    }

    case nebula::meta::cpp2::JobOp::SHOW: {
      // show job jobId should have a parameter as jobId
      if (paras.size() != 1) {
        LOG(INFO) << "Show job jobId can only have one parameter.";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }

      int jobId = atoi(paras[0].c_str());
      if (jobId == 0) {
        LOG(INFO) << "Show job jobId should have one parameter as the job ID";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }
      auto ret = jobMgr_->showJob(spaceId_, jobId);
      if (nebula::ok(ret)) {
        result.job_desc_ref() = std::vector<cpp2::JobDesc>{nebula::value(ret).first};
        result.task_desc_ref() = nebula::value(ret).second;
      } else {
        errorCode = nebula::error(ret);
      }
      break;
    }

    case nebula::meta::cpp2::JobOp::STOP: {
      // stop job should have a parameter as jobId
      if (paras.size() != 1) {
        LOG(INFO) << "Stop job jobId can only have one parameter";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }
      JobID jobId = atoi(paras[0].c_str());
      if (jobId == 0) {
        LOG(INFO) << "Stop job should have one parameter as the job ID";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }
      errorCode = jobMgr_->stopJob(spaceId_, jobId);
      break;
    }
    case nebula::meta::cpp2::JobOp::RECOVER: {
      // Note that the last parameter is no longer spaceName
      std::vector<int32_t> jobIds;
      jobIds.reserve(paras.size());
      for (size_t i = 0; i < paras.size(); i++) {
        jobIds.push_back(std::stoi(paras[i]));
      }
      auto ret = jobMgr_->recoverJob(spaceId_, adminClient_, jobIds);
      if (nebula::ok(ret)) {
        result.recovered_job_num_ref() = nebula::value(ret);
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
  resp_.result_ref() = std::move(result);
  onFinished();
}

nebula::cpp2::ErrorCode AdminJobProcessor::addJobProcess(const cpp2::AdminJobReq& req,
                                                         cpp2::AdminJobResult& result) {
  auto type = req.get_type();
  auto paras = req.get_paras();

  // Check if job not exists
  JobID jId = 0;
  auto runningJobExist = jobMgr_->checkOnRunningJobExist(spaceId_, type, paras, jId);
  if (runningJobExist) {
    LOG(INFO) << "Job has already exists: " << jId;
    result.job_id_ref() = jId;
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  auto retCode = jobMgr_->checkNeedRecoverJobExist(spaceId_, type);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "There is a failed data balance job, need to recover or stop it firstly!";
    return retCode;
  }

  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto jobId = autoIncrementId();
  if (!nebula::ok(jobId)) {
    return nebula::error(jobId);
  }

  JobDescription jobDesc(spaceId_, nebula::value(jobId), type, paras);
  auto errorCode = jobMgr_->addJob(std::move(jobDesc), adminClient_);
  if (errorCode == nebula::cpp2::ErrorCode::SUCCEEDED) {
    result.job_id_ref() = nebula::value(jobId);
  }
  return errorCode;
}

}  // namespace meta
}  // namespace nebula
