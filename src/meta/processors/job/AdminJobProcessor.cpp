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
  oss << ", paras.size()=" << req.get_paras().size();
  for (auto& p : req.get_paras()) {
    oss << " " << p;
  }
  LOG(INFO) << __func__ << "() " << oss.str();

  cpp2::AdminJobResult result;
  auto errorCode = nebula::cpp2::ErrorCode::SUCCEEDED;
  jobMgr_ = JobManager::getInstance();

  switch (req.get_op()) {
    case nebula::meta::cpp2::JobOp::ADD: {
      errorCode = addJobProcess(req, result);
      break;
    }
    case nebula::meta::cpp2::JobOp::SHOW_All: {
      auto ret = jobMgr_->showJobs(req.get_paras().back());
      if (nebula::ok(ret)) {
        result.job_desc_ref() = nebula::value(ret);
      } else {
        errorCode = nebula::error(ret);
      }
      break;
    }
    case nebula::meta::cpp2::JobOp::SHOW: {
      static const size_t kShowArgsNum = 2;
      if (req.get_paras().size() != kShowArgsNum) {
        LOG(INFO) << "Parameter number not matched";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }

      int iJob = atoi(req.get_paras()[0].c_str());
      if (iJob == 0) {
        LOG(INFO) << "Show job should have parameter as the job ID";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }
      auto ret = jobMgr_->showJob(iJob, req.get_paras().back());
      if (nebula::ok(ret)) {
        result.job_desc_ref() = std::vector<cpp2::JobDesc>{nebula::value(ret).first};
        result.task_desc_ref() = nebula::value(ret).second;
      } else {
        errorCode = nebula::error(ret);
      }
      break;
    }
    case nebula::meta::cpp2::JobOp::STOP: {
      static const size_t kStopJobArgsNum = 2;
      if (req.get_paras().size() != kStopJobArgsNum) {
        LOG(INFO) << "Parameter number not matched";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }
      int iJob = atoi(req.get_paras()[0].c_str());
      if (iJob == 0) {
        LOG(INFO) << "Stop job should have parameter as the job ID";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }
      errorCode = jobMgr_->stopJob(iJob, req.get_paras().back());
      break;
    }
    case nebula::meta::cpp2::JobOp::RECOVER: {
      const std::vector<std::string>& paras = req.get_paras();
      const std::string& spaceName = req.get_paras().back();
      std::vector<int32_t> jobIds;
      jobIds.reserve(paras.size() - 1);
      for (size_t i = 0; i < paras.size() - 1; i++) {
        jobIds.push_back(std::stoi(paras[i]));
      }
      auto ret = jobMgr_->recoverJob(spaceName, adminClient_, jobIds);
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

  // All jobs here are the space level, so the last parameter is spaceName.
  if (paras.empty()) {
    LOG(INFO) << "Parameter should be not empty";
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }

  // Check if space not exists
  auto spaceName = paras.back();
  auto spaceRet = getSpaceId(spaceName);
  if (!nebula::ok(spaceRet)) {
    auto retCode = nebula::error(spaceRet);
    LOG(INFO) << "Get space failed, space name: " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  // Check if job not exists
  JobID jId = 0;
  auto jobExist = jobMgr_->checkJobExist(type, paras, jId);
  if (jobExist) {
    LOG(INFO) << "Job has already exists: " << jId;
    result.job_id_ref() = jId;
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto jobId = autoIncrementId();
  if (!nebula::ok(jobId)) {
    return nebula::error(jobId);
  }

  JobDescription jobDesc(nebula::value(jobId), type, paras);
  auto errorCode = jobMgr_->addJob(jobDesc, adminClient_);
  if (errorCode == nebula::cpp2::ErrorCode::SUCCEEDED) {
    result.job_id_ref() = nebula::value(jobId);
  }
  return errorCode;
}

}  // namespace meta
}  // namespace nebula
