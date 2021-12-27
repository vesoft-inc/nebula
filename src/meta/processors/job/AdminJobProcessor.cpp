/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/AdminJobProcessor.h"

#include "common/base/StatusOr.h"
#include "common/stats/StatsManager.h"
#include "meta/processors/job/JobDescription.h"
#include "meta/processors/job/JobManager.h"

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
    case nebula::meta::cpp2::AdminJobOp::ADD: {
      auto cmd = req.get_cmd();
      auto paras = req.get_paras();
      if (cmd == cpp2::AdminCmd::REBUILD_TAG_INDEX || cmd == cpp2::AdminCmd::REBUILD_EDGE_INDEX ||
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
        result.job_id_ref() = jId;
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
        result.job_id_ref() = nebula::value(jobId);
      }
      break;
    }
    case nebula::meta::cpp2::AdminJobOp::SHOW_All: {
      auto ret = jobMgr->showJobs(req.get_paras().back());
      if (nebula::ok(ret)) {
        result.job_desc_ref() = nebula::value(ret);
      } else {
        errorCode = nebula::error(ret);
      }
      break;
    }
    case nebula::meta::cpp2::AdminJobOp::SHOW: {
      static const size_t kShowArgsNum = 2;
      if (req.get_paras().size() != kShowArgsNum) {
        LOG(ERROR) << "Parameter number not matched";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }

      int iJob = atoi(req.get_paras()[0].c_str());
      if (iJob == 0) {
        LOG(ERROR) << "Show job should have parameter as the job ID";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }
      auto ret = jobMgr->showJob(iJob, req.get_paras().back());
      if (nebula::ok(ret)) {
        result.job_desc_ref() = std::vector<cpp2::JobDesc>{nebula::value(ret).first};
        result.task_desc_ref() = nebula::value(ret).second;
      } else {
        errorCode = nebula::error(ret);
      }
      break;
    }
    case nebula::meta::cpp2::AdminJobOp::STOP: {
      static const size_t kStopJobArgsNum = 2;
      if (req.get_paras().size() != kStopJobArgsNum) {
        LOG(ERROR) << "Parameter number not matched";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }
      int iJob = atoi(req.get_paras()[0].c_str());
      if (iJob == 0) {
        LOG(ERROR) << "Stop job should have parameter as the job ID";
        errorCode = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        break;
      }
      errorCode = jobMgr->stopJob(iJob, req.get_paras().back());
      break;
    }
    case nebula::meta::cpp2::AdminJobOp::RECOVER: {
      const std::vector<std::string>& paras = req.get_paras();
      const std::string& spaceName = req.get_paras().back();
      std::vector<int32_t> jobIds;
      jobIds.reserve(paras.size() - 1);
      for (size_t i = 0; i < paras.size() - 1; i++) {
        jobIds.push_back(std::stoi(paras[i]));
      }
      auto ret = jobMgr->recoverJob(spaceName, adminClient_, jobIds);
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

}  // namespace meta
}  // namespace nebula
