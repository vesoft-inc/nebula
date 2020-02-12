/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "gen-cpp2/meta_types.h"
#include "http/HttpClient.h"
#include "graph/AdminJobExecutor.h"
#include "process/ProcessUtils.h"
#include "webservice/Common.h"

#include <folly/executors/Async.h>
#include <folly/futures/Future.h>

namespace nebula {
namespace graph {

AdminJobExecutor::AdminJobExecutor(Sentence *sentence,
        ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AdminSentence*>(sentence);
}

void AdminJobExecutor::execute() {
    LOG(INFO) << __func__ << " enter";
    auto opEnum = toAdminJobOp(sentence_->getType());
    auto paras = sentence_->getParas();

    if (opNeedsSpace(opEnum)) {
        auto status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            DCHECK(onError_);
            onError_(std::move(status));
            return;
        }
        paras.emplace_back(ectx()->rctx()->session()->spaceName());
    }

    auto future = ectx()->getMetaClient()->submitJob(opEnum, paras);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this, opEnum] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::forward<nebula::Status>(resp.status()));
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        auto header = getHeader(opEnum);
        resp_->set_column_names(std::move(header));

        auto result = toRowValues(opEnum, std::move(resp.value()));
        resp_->set_rows(std::move(result));

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

Status AdminJobExecutor::prepare() {
    return Status::OK();
}

bool AdminJobExecutor::opNeedsSpace(nebula::meta::cpp2::AdminJobOp op) {
    return op == nebula::meta::cpp2::AdminJobOp::ADD;
}

void AdminJobExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

std::vector<std::string>
AdminJobExecutor::getHeader(nebula::meta::cpp2::AdminJobOp op, bool succeed) {
    if (!succeed) {
        return {"Error"};
    }
    switch (op) {
    case nebula::meta::cpp2::AdminJobOp::ADD:
        return {"New Job Id"};
    case nebula::meta::cpp2::AdminJobOp::SHOW_All:
        return {"Job Id", "Command", "Status", "Start Time", "Stop Time"};
    case nebula::meta::cpp2::AdminJobOp::SHOW:
        return {"Job Id(TaskId)", "Command(Dest)", "Status", "Start Time", "Stop Time"};
    case nebula::meta::cpp2::AdminJobOp::STOP:
        return {"Result"};
    case nebula::meta::cpp2::AdminJobOp::RECOVER:
        return {"Recovered job num"};
    default:
        return {"Result"};
    }
}

nebula::meta::cpp2::AdminJobOp
AdminJobExecutor::toAdminJobOp(const std::string& op) {
    if (op == "add_job") {
        return nebula::meta::cpp2::AdminJobOp::ADD;
    } else if (op == "show_jobs") {
        return nebula::meta::cpp2::AdminJobOp::SHOW_All;
    } else if (op == "show_job") {
        return nebula::meta::cpp2::AdminJobOp::SHOW;
    } else if (op == "stop_job") {
        return nebula::meta::cpp2::AdminJobOp::STOP;
    } else if (op == "recover_job") {
        return nebula::meta::cpp2::AdminJobOp::RECOVER;
    }
    return nebula::meta::cpp2::AdminJobOp::INVALID;
}

std::string AdminJobExecutor::time2string(int64_t t) {
    std::string ret;
    if (t == 0) {
        return ret;
    }
    std::time_t tm = t;
    char mbstr[50];
    int len = std::strftime(mbstr, sizeof(mbstr), "%x %X", std::localtime(&tm));

    if (len != 0) {
        ret = std::string(&mbstr[0], len);
    }
    return ret;
}

cpp2::RowValue
AdminJobExecutor::toRowValue(const nebula::meta::cpp2::JobDesc& job) {
    cpp2::RowValue ret;
    std::vector<cpp2::ColumnValue> row(5);
    row[0].set_str(std::to_string(job.get_id()));
    std::stringstream oss;
    oss << job.get_cmd() << " ";
    for (auto& p : job.get_paras()) {
        oss << p << " ";
    }
    row[1].set_str(oss.str());
    row[2].set_str(toString(job.get_status()));
    row[3].set_str(time2string(job.get_start_time()));
    row[4].set_str(time2string(job.get_stop_time()));

    ret.set_columns(std::move(row));
    return ret;
}

cpp2::RowValue
AdminJobExecutor::toRowValue(const nebula::meta::cpp2::TaskDesc& task) {
    cpp2::RowValue ret;
    std::vector<cpp2::ColumnValue> row(5);
    row[0].set_str(folly::stringPrintf("%d-%d", task.get_job_id(), task.get_task_id()));
    row[1].set_str(toString(task.get_host()));
    row[2].set_str(toString(task.get_status()));
    row[3].set_str(time2string(task.get_start_time()));
    row[4].set_str(time2string(task.get_stop_time()));

    ret.set_columns(std::move(row));
    return ret;
}

cpp2::RowValue AdminJobExecutor::toRowValue(std::string&& msg) {
    cpp2::RowValue row;
    std::vector<cpp2::ColumnValue> cols(1);
    cols.back().set_str(std::move(msg));

    row.set_columns(std::move(cols));
    return row;
}

std::vector<cpp2::RowValue>
AdminJobExecutor::toRowValues(nebula::meta::cpp2::AdminJobOp op,
                              nebula::meta::cpp2::AdminJobResult &&resp) {
    std::vector<cpp2::RowValue> ret;
    switch (op) {
    case nebula::meta::cpp2::AdminJobOp::ADD:
        {
            ret.emplace_back(toRowValue(std::to_string(*resp.get_job_id())));
        }
        break;
    case nebula::meta::cpp2::AdminJobOp::SHOW_All:
        {
            for (auto& job : *resp.get_job_desc()) {
                ret.emplace_back(toRowValue(job));
            }
        }
        break;
    case nebula::meta::cpp2::AdminJobOp::SHOW:
        {
            for (auto& job : *resp.get_job_desc()) {
                ret.emplace_back(toRowValue(job));
            }
            for (auto& task : *resp.get_task_desc()) {
                ret.emplace_back(toRowValue(task));
            }
        }
        break;
    case nebula::meta::cpp2::AdminJobOp::STOP:
        {
            ret.emplace_back(toRowValue("Job stopped"));
        }
        break;
    case nebula::meta::cpp2::AdminJobOp::RECOVER:
        {
            auto msg = folly::stringPrintf("recoverd job num: %d",
                                              *resp.get_recovered_job_num());
            ret.emplace_back(toRowValue(std::move(msg)));
        }
        break;
    default:
        return ret;
    }
    return ret;
}

std::string
AdminJobExecutor::toString(nebula::meta::cpp2::JobStatus st) {
    switch (st) {
    case nebula::meta::cpp2::JobStatus::QUEUE:
        return "queue";
    case nebula::meta::cpp2::JobStatus::RUNNING:
        return "running";
    case nebula::meta::cpp2::JobStatus::FINISHED:
        return "finished";
    case nebula::meta::cpp2::JobStatus::FAILED:
        return "failed";
    case nebula::meta::cpp2::JobStatus::STOPPED:
        return "stopped";
    case nebula::meta::cpp2::JobStatus::INVALID:
        return "invalid";
    }
    return "invalid st";
}

std::string AdminJobExecutor::toString(nebula::cpp2::HostAddr host) {
    auto ip = network::NetworkUtils::intToIPv4(host.get_ip());
    auto ret = folly::stringPrintf("%s:%d", ip.c_str(), host.get_port());
    return ret;
}

}   // namespace graph
}   // namespace nebula
