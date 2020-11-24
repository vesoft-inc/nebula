/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/SubmitJobExecutor.h"

#include "planner/Admin.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> SubmitJobExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *sjNode = asNode<SubmitJob>(node());
    auto jobOp = sjNode->jobOp();
    std::vector<std::string> jobArguments;

    meta::cpp2::AdminCmd cmd = meta::cpp2::AdminCmd::UNKNOWN;
    if (jobOp == meta::cpp2::AdminJobOp::ADD) {
        auto spaceName = qctx()->rctx()->session()->space().name;
        jobArguments.emplace_back(std::move(spaceName));
        auto& params = sjNode->params();
        if (params[0] == "compact") {
            cmd = meta::cpp2::AdminCmd::COMPACT;
        } else if (params[0] == "flush") {
            cmd = meta::cpp2::AdminCmd::FLUSH;
        } else if (params[0] == "rebuild") {
            if (params[1] == "tag") {
                cmd = meta::cpp2::AdminCmd::REBUILD_TAG_INDEX;
                jobArguments.emplace_back(params[3]);
            } else if (params[1] == "edge") {
                cmd = meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX;
                jobArguments.emplace_back(params[3]);
            } else {
                LOG(ERROR) << "Unknown job command rebuild " << params[1];
                return Status::Error("Unknown job command rebuild %s", params[1].c_str());
            }
        } else if (params[0] == "stats") {
            cmd = meta::cpp2::AdminCmd::STATS;
        } else {
            LOG(ERROR) << "Unknown job command " << params[0].c_str();
            return Status::Error("Unknown job command %s", params[0].c_str());
        }
    } else if (jobOp == meta::cpp2::AdminJobOp::SHOW || jobOp == meta::cpp2::AdminJobOp::STOP) {
        auto& params = sjNode->params();
        DCHECK_GT(params.size(), 0UL);
        jobArguments.emplace_back(params[0]);
    }

    return qctx()->getMetaClient()->submitJob(jobOp, cmd, jobArguments)
        .via(runner())
        .then([jobOp, this](StatusOr<meta::cpp2::AdminJobResult> &&resp) {
            SCOPED_TIMER(&execTime_);

            if (!resp.ok()) {
                LOG(ERROR) << resp.status().toString();
                return std::move(resp).status();
            }
            switch (jobOp) {
                case meta::cpp2::AdminJobOp::ADD: {
                    nebula::DataSet v({"New Job Id"});
                    DCHECK(resp.value().__isset.job_id);
                    if (!resp.value().__isset.job_id) {
                        return Status::Error("Response unexpected.");
                    }
                    v.emplace_back(nebula::Row({*DCHECK_NOTNULL(resp.value().get_job_id())}));
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::RECOVER: {
                    nebula::DataSet v({"Recovered job num"});
                    DCHECK(resp.value().__isset.recovered_job_num);
                    if (!resp.value().__isset.recovered_job_num) {
                        return Status::Error("Response unexpected.");
                    }
                    v.emplace_back(
                        nebula::Row({*DCHECK_NOTNULL(resp.value().get_recovered_job_num())}));
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::SHOW: {
                    nebula::DataSet v(
                        {"Job Id(TaskId)", "Command(Dest)", "Status", "Start Time", "Stop Time"});
                    DCHECK(resp.value().__isset.job_desc);
                    if (!resp.value().__isset.job_desc) {
                        return Status::Error("Response unexpected.");
                    }
                    DCHECK(resp.value().__isset.task_desc);
                    if (!resp.value().__isset.task_desc) {
                        return Status::Error("Response unexpected");
                    }
                    auto &jobDesc = *resp.value().get_job_desc();
                    // job desc
                    v.emplace_back(
                        nebula::Row(
                            {jobDesc.front().get_id(),
                            meta::cpp2::_AdminCmd_VALUES_TO_NAMES.at(jobDesc.front().get_cmd()),
                            meta::cpp2::_JobStatus_VALUES_TO_NAMES.at(jobDesc.front().get_status()),
                            jobDesc.front().get_start_time(),
                            jobDesc.front().get_stop_time(),
                            }));
                    // tasks desc
                    auto &tasksDesc = *resp.value().get_task_desc();
                    for (const auto & taskDesc : tasksDesc) {
                        v.emplace_back(nebula::Row({
                            taskDesc.get_task_id(),
                            taskDesc.get_host().host,
                            meta::cpp2::_JobStatus_VALUES_TO_NAMES.at(taskDesc.get_status()),
                            taskDesc.get_start_time(),
                            taskDesc.get_stop_time(),
                        }));
                    }
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::SHOW_All: {
                    nebula::DataSet v({"Job Id", "Command", "Status", "Start Time", "Stop Time"});
                    DCHECK(resp.value().__isset.job_desc);
                    if (!resp.value().__isset.job_desc) {
                        return Status::Error("Response unexpected");
                    }
                    const auto &jobsDesc = *resp.value().get_job_desc();
                    for (const auto &jobDesc : jobsDesc) {
                        v.emplace_back(nebula::Row({
                            jobDesc.get_id(),
                            meta::cpp2::_AdminCmd_VALUES_TO_NAMES.at(jobDesc.get_cmd()),
                            meta::cpp2::_JobStatus_VALUES_TO_NAMES.at(jobDesc.get_status()),
                            jobDesc.get_start_time(),
                            jobDesc.get_stop_time(),
                        }));
                    }
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::STOP: {
                    nebula::DataSet v({"Result"});
                    v.emplace_back(nebula::Row({
                        "Job stopped"
                    }));
                    return finish(std::move(v));
                }
            // no default so the compiler will warning when lack
            }
            DLOG(FATAL) << "Unknown job operation " << static_cast<int>(jobOp);
            return Status::Error("Unkown job job operation %d.", static_cast<int>(jobOp));
        });
}

}   // namespace graph
}   // namespace nebula
