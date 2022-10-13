// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/SubmitJobExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/time/TimeUtils.h"
#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> SubmitJobExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *sjNode = asNode<SubmitJob>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  auto jobOp = sjNode->jobOp();
  auto jobType = sjNode->jobType();
  auto params = sjNode->params();

  return qctx()
      ->getMetaClient()
      ->submitJob(spaceId, jobOp, jobType, params)
      .via(runner())
      .thenValue([jobOp, spaceId, this](StatusOr<meta::cpp2::AdminJobResult> &&resp) {
        SCOPED_TIMER(&execTime_);

        if (!resp.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId << ", Submit job fail: " << resp.status();
          return std::move(resp).status();
        }
        auto status = buildResult(jobOp, std::move(resp).value());
        NG_RETURN_IF_ERROR(status);
        return finish(std::move(status).value());
      });
}

StatusOr<DataSet> SubmitJobExecutor::buildResult(meta::cpp2::JobOp jobOp,
                                                 meta::cpp2::AdminJobResult &&resp) {
  switch (jobOp) {
    case meta::cpp2::JobOp::ADD: {
      nebula::DataSet v({"New Job Id"});
      DCHECK(resp.job_id_ref().has_value());
      if (!resp.job_id_ref().has_value()) {
        return Status::Error("Response unexpected.");
      }
      v.emplace_back(nebula::Row({*resp.job_id_ref()}));
      return v;
    }
    case meta::cpp2::JobOp::RECOVER: {
      nebula::DataSet v({"Recovered job num"});
      DCHECK(resp.recovered_job_num_ref().has_value());
      if (!resp.recovered_job_num_ref().has_value()) {
        return Status::Error("Response unexpected.");
      }
      v.emplace_back(nebula::Row({*resp.recovered_job_num_ref()}));
      return v;
    }
    case meta::cpp2::JobOp::SHOW: {
      DCHECK(resp.job_desc_ref().has_value());
      if (!resp.job_desc_ref().has_value()) {
        return Status::Error("Response unexpected.");
      }
      DCHECK(resp.task_desc_ref().has_value());
      if (!resp.task_desc_ref().has_value()) {
        return Status::Error("Response unexpected");
      }
      auto &jobDesc = *resp.job_desc_ref();
      return buildShowResultData(jobDesc.front(), *resp.get_task_desc());
    }
    case meta::cpp2::JobOp::SHOW_All: {
      nebula::DataSet v({"Job Id", "Command", "Status", "Start Time", "Stop Time"});
      DCHECK(resp.job_desc_ref().has_value());
      if (!resp.job_desc_ref().has_value()) {
        return Status::Error("Response unexpected");
      }
      const auto &jobsDesc = *resp.job_desc_ref();
      for (const auto &jobDesc : jobsDesc) {
        // show zone balance as data balance
        auto type = jobDesc.get_type();
        if (type == meta::cpp2::JobType::ZONE_BALANCE) {
          type = meta::cpp2::JobType::DATA_BALANCE;
        }
        v.emplace_back(nebula::Row({
            jobDesc.get_job_id(),
            apache::thrift::util::enumNameSafe(type),
            apache::thrift::util::enumNameSafe(jobDesc.get_status()),
            convertJobTimestampToDateTime(jobDesc.get_start_time()),
            convertJobTimestampToDateTime(jobDesc.get_stop_time()),
        }));
      }
      return v;
    }
    case meta::cpp2::JobOp::STOP: {
      nebula::DataSet v({"Result"});
      v.emplace_back(nebula::Row({"Job stopped"}));
      return v;
    }
      // no default so the compiler will warning when lack
  }
  DLOG(FATAL) << "Unknown job operation " << static_cast<int>(jobOp);
  return Status::Error("Unknown job job operation %d.", static_cast<int>(jobOp));
}

Value SubmitJobExecutor::convertJobTimestampToDateTime(int64_t timestamp) {
  return timestamp > 0 ? Value(time::TimeConversion::unixSecondsToDateTime(timestamp))
                       : Value::kEmpty;
}

nebula::DataSet SubmitJobExecutor::buildShowResultData(
    const nebula::meta::cpp2::JobDesc &jd, const std::vector<nebula::meta::cpp2::TaskDesc> &td) {
  if (jd.get_type() == meta::cpp2::JobType::DATA_BALANCE ||
      jd.get_type() == meta::cpp2::JobType::ZONE_BALANCE) {
    // The job which executed on meta, aka balance data, is a litte different from others. In order
    // to be consistent with other jobs, the task result is set in paras in JobDesc serialized by
    // thrift. The reason that we can't use the list of TaskDesc is that the state of balance task
    // is saved in BalanceTask, which is different from TaskDesc.
    nebula::DataSet v({"Job Id(spaceId:partId)",
                       "Command(src->dst)",
                       "Status",
                       "Start Time",
                       "Stop Time",
                       "Error Code"});
    const auto &paras = jd.get_paras();
    size_t index = std::stoul(paras.back());
    uint32_t total = paras.size() - index - 1, succeeded = 0, failed = 0, inProgress = 0,
             invalid = 0;
    v.emplace_back(Row({jd.get_job_id(),
                        apache::thrift::util::enumNameSafe(meta::cpp2::JobType::DATA_BALANCE),
                        apache::thrift::util::enumNameSafe(jd.get_status()),
                        convertJobTimestampToDateTime(jd.get_start_time()).toString(),
                        convertJobTimestampToDateTime(jd.get_stop_time()).toString(),
                        apache::thrift::util::enumNameSafe(jd.get_code())}));
    for (size_t i = index; i < paras.size() - 1; i++) {
      meta::cpp2::BalanceTask tsk;
      apache::thrift::CompactSerializer::deserialize(paras[i], tsk);
      switch (tsk.get_result()) {
        case meta::cpp2::TaskResult::FAILED:
          ++failed;
          break;
        case meta::cpp2::TaskResult::IN_PROGRESS:
          ++inProgress;
          break;
        case meta::cpp2::TaskResult::INVALID:
          ++invalid;
          break;
        case meta::cpp2::TaskResult::SUCCEEDED:
          ++succeeded;
          break;
      }
      v.emplace_back(Row({std::move(tsk).get_id(),
                          std::move(tsk).get_command(),
                          apache::thrift::util::enumNameSafe(tsk.get_result()),
                          convertJobTimestampToDateTime(std::move(tsk).get_start_time()),
                          convertJobTimestampToDateTime(std::move(tsk).get_stop_time()),
                          jd.get_code() == nebula::cpp2::ErrorCode::E_JOB_SUBMITTED
                              ? ""
                              : apache::thrift::util::enumNameSafe(jd.get_code())}));
    }
    v.emplace_back(Row({folly::sformat("Total:{}", total),
                        folly::sformat("Succeeded:{}", succeeded),
                        folly::sformat("Failed:{}", failed),
                        folly::sformat("In Progress:{}", inProgress),
                        folly::sformat("Invalid:{}", invalid),
                        ("")}));
    return v;
  } else {
    nebula::DataSet v(
        {"Job Id(TaskId)", "Command(Dest)", "Status", "Start Time", "Stop Time", "Error Code"});
    v.emplace_back(nebula::Row({
        jd.get_job_id(),
        apache::thrift::util::enumNameSafe(jd.get_type()),
        apache::thrift::util::enumNameSafe(jd.get_status()),
        convertJobTimestampToDateTime(jd.get_start_time()),
        convertJobTimestampToDateTime(jd.get_stop_time()),
        apache::thrift::util::enumNameSafe(jd.get_code()),
    }));
    // As for normal type of job, there will only be one of the three state: running, succeeded or
    // failed.
    uint32_t total = td.size(), succeeded = 0, failed = 0, inProgress = 0;
    // tasks desc
    for (const auto &taskDesc : td) {
      switch (taskDesc.get_status()) {
        case meta::cpp2::JobStatus::RUNNING:
          ++inProgress;
          break;
        case meta::cpp2::JobStatus::FAILED:
          ++failed;
          break;
        case meta::cpp2::JobStatus::FINISHED:
          ++succeeded;
          break;
        default:
          break;
      }
      v.emplace_back(nebula::Row({
          taskDesc.get_task_id(),
          taskDesc.get_host().host,
          apache::thrift::util::enumNameSafe(taskDesc.get_status()),
          convertJobTimestampToDateTime(taskDesc.get_start_time()),
          convertJobTimestampToDateTime(taskDesc.get_stop_time()),
          jd.get_code() == nebula::cpp2::ErrorCode::E_JOB_SUBMITTED
              ? ""
              : apache::thrift::util::enumNameSafe(jd.get_code()),
      }));
    }
    v.emplace_back(Row({folly::sformat("Total:{}", total),
                        folly::sformat("Succeeded:{}", succeeded),
                        folly::sformat("Failed:{}", failed),
                        folly::sformat("In Progress:{}", inProgress),
                        (""),
                        ("")}));
    return v;
  }
}
}  // namespace graph
}  // namespace nebula
