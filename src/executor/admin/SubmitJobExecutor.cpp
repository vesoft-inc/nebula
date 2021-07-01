/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "executor/admin/SubmitJobExecutor.h"

#include "common/time/TimeUtils.h"
#include "context/QueryContext.h"
#include "planner/plan/Admin.h"
#include "util/ScopedTimer.h"

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
            switch (jobOp) {
                case meta::cpp2::AdminJobOp::ADD: {
                    nebula::DataSet v({"New Job Id"});
                    DCHECK(resp.value().job_id_ref().has_value());
                    if (!resp.value().job_id_ref().has_value()) {
                        return Status::Error("Response unexpected.");
                    }
                    v.emplace_back(nebula::Row({*resp.value().job_id_ref()}));
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::RECOVER: {
                    nebula::DataSet v({"Recovered job num"});
                    DCHECK(resp.value().recovered_job_num_ref().has_value());
                    if (!resp.value().recovered_job_num_ref().has_value()) {
                        return Status::Error("Response unexpected.");
                    }
                    v.emplace_back(nebula::Row({*resp.value().recovered_job_num_ref()}));
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::SHOW: {
                    nebula::DataSet v(
                        {"Job Id(TaskId)", "Command(Dest)", "Status", "Start Time", "Stop Time"});
                    DCHECK(resp.value().job_desc_ref().has_value());
                    if (!resp.value().job_desc_ref().has_value()) {
                        return Status::Error("Response unexpected.");
                    }
                    DCHECK(resp.value().task_desc_ref().has_value());
                    if (!resp.value().task_desc_ref().has_value()) {
                        return Status::Error("Response unexpected");
                    }
                    auto &jobDesc = *resp.value().job_desc_ref();
                    // job desc
                    v.emplace_back(nebula::Row({
                        jobDesc.front().get_id(),
                        apache::thrift::util::enumNameSafe(jobDesc.front().get_cmd()),
                        apache::thrift::util::enumNameSafe(jobDesc.front().get_status()),
                        time::TimeConversion::unixSecondsToDateTime(
                            jobDesc.front().get_start_time()),
                        time::TimeConversion::unixSecondsToDateTime(
                            jobDesc.front().get_stop_time()),
                    }));
                    // tasks desc
                    auto &tasksDesc = *resp.value().get_task_desc();
                    for (const auto &taskDesc : tasksDesc) {
                        v.emplace_back(nebula::Row({
                            taskDesc.get_task_id(),
                            taskDesc.get_host().host,
                            apache::thrift::util::enumNameSafe(taskDesc.get_status()),
                            time::TimeConversion::unixSecondsToDateTime(taskDesc.get_start_time()),
                            time::TimeConversion::unixSecondsToDateTime(taskDesc.get_stop_time()),
                        }));
                    }
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::SHOW_All: {
                    nebula::DataSet v({"Job Id", "Command", "Status", "Start Time", "Stop Time"});
                    DCHECK(resp.value().job_desc_ref().has_value());
                    if (!resp.value().job_desc_ref().has_value()) {
                        return Status::Error("Response unexpected");
                    }
                    const auto &jobsDesc = *resp.value().job_desc_ref();
                    for (const auto &jobDesc : jobsDesc) {
                        v.emplace_back(nebula::Row({
                            jobDesc.get_id(),
                            apache::thrift::util::enumNameSafe(jobDesc.get_cmd()),
                            apache::thrift::util::enumNameSafe(jobDesc.get_status()),
                            time::TimeConversion::unixSecondsToDateTime(jobDesc.get_start_time()),
                            time::TimeConversion::unixSecondsToDateTime(jobDesc.get_stop_time()),
                        }));
                    }
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::STOP: {
                    nebula::DataSet v({"Result"});
                    v.emplace_back(nebula::Row({"Job stopped"}));
                    return finish(std::move(v));
                }
                    // no default so the compiler will warning when lack
            }
            DLOG(FATAL) << "Unknown job operation " << static_cast<int>(jobOp);
            return Status::Error("Unknown job job operation %d.", static_cast<int>(jobOp));
        });
}

}   // namespace graph
}   // namespace nebula
