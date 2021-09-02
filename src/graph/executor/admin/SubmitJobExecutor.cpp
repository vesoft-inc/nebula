/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/admin/SubmitJobExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/time/TimeUtils.h"
#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Admin.h"
#include "graph/util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> SubmitJobExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *sjNode = asNode<SubmitJob>(node());
  auto jobOp = sjNode->jobOp();
  auto cmd = sjNode->cmd();
  auto params = sjNode->params();

  return qctx()
      ->getMetaClient()
      ->submitJob(jobOp, cmd, params)
      .via(runner())
      .thenValue([jobOp, this](StatusOr<meta::cpp2::AdminJobResult> &&resp) {
        SCOPED_TIMER(&execTime_);

        if (!resp.ok()) {
          LOG(ERROR) << resp.status().toString();
          return std::move(resp).status();
        }
        auto status = buildResult(jobOp, std::move(resp).value());
        NG_RETURN_IF_ERROR(status);
        return finish(std::move(status).value());
      });
}

StatusOr<DataSet> SubmitJobExecutor::buildResult(meta::cpp2::AdminJobOp jobOp,
                                                 meta::cpp2::AdminJobResult &&resp) {
  switch (jobOp) {
    case meta::cpp2::AdminJobOp::ADD: {
      nebula::DataSet v({"New Job Id"});
      DCHECK(resp.job_id_ref().has_value());
      if (!resp.job_id_ref().has_value()) {
        return Status::Error("Response unexpected.");
      }
      v.emplace_back(nebula::Row({*resp.job_id_ref()}));
      return v;
    }
    case meta::cpp2::AdminJobOp::RECOVER: {
      nebula::DataSet v({"Recovered job num"});
      DCHECK(resp.recovered_job_num_ref().has_value());
      if (!resp.recovered_job_num_ref().has_value()) {
        return Status::Error("Response unexpected.");
      }
      v.emplace_back(nebula::Row({*resp.recovered_job_num_ref()}));
      return v;
    }
    case meta::cpp2::AdminJobOp::SHOW: {
      nebula::DataSet v({"Job Id(TaskId)", "Command(Dest)", "Status", "Start Time", "Stop Time"});
      DCHECK(resp.job_desc_ref().has_value());
      if (!resp.job_desc_ref().has_value()) {
        return Status::Error("Response unexpected.");
      }
      DCHECK(resp.task_desc_ref().has_value());
      if (!resp.task_desc_ref().has_value()) {
        return Status::Error("Response unexpected");
      }
      auto &jobDesc = *resp.job_desc_ref();
      // job desc
      v.emplace_back(nebula::Row({
          jobDesc.front().get_id(),
          apache::thrift::util::enumNameSafe(jobDesc.front().get_cmd()),
          apache::thrift::util::enumNameSafe(jobDesc.front().get_status()),
          convertJobTimestampToDateTime(jobDesc.front().get_start_time()),
          convertJobTimestampToDateTime(jobDesc.front().get_stop_time()),
      }));
      // tasks desc
      auto &tasksDesc = *resp.get_task_desc();
      for (const auto &taskDesc : tasksDesc) {
        v.emplace_back(nebula::Row({
            taskDesc.get_task_id(),
            taskDesc.get_host().host,
            apache::thrift::util::enumNameSafe(taskDesc.get_status()),
            convertJobTimestampToDateTime(taskDesc.get_start_time()),
            convertJobTimestampToDateTime(taskDesc.get_stop_time()),
        }));
      }
      return v;
    }
    case meta::cpp2::AdminJobOp::SHOW_All: {
      nebula::DataSet v({"Job Id", "Command", "Status", "Start Time", "Stop Time"});
      DCHECK(resp.job_desc_ref().has_value());
      if (!resp.job_desc_ref().has_value()) {
        return Status::Error("Response unexpected");
      }
      const auto &jobsDesc = *resp.job_desc_ref();
      for (const auto &jobDesc : jobsDesc) {
        v.emplace_back(nebula::Row({
            jobDesc.get_id(),
            apache::thrift::util::enumNameSafe(jobDesc.get_cmd()),
            apache::thrift::util::enumNameSafe(jobDesc.get_status()),
            convertJobTimestampToDateTime(jobDesc.get_start_time()),
            convertJobTimestampToDateTime(jobDesc.get_stop_time()),
        }));
      }
      return v;
    }
    case meta::cpp2::AdminJobOp::STOP: {
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
}  // namespace graph
}  // namespace nebula
