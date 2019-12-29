/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

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

    if (opNeedsSpace(opEnum)) {
        auto status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            DCHECK(onError_);
            onError_(std::move(status));
            return;
        }
        const std::string& spaceName = ectx()->rctx()->session()->spaceName();
        sentence_->addPara(spaceName);
    }

    auto future = ectx()->getMetaClient()->runAdminJob(opEnum, sentence_->getParas());
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this, opEnum] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::forward<nebula::Status>(resp.status()));
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header = getHeader(opEnum);
        resp_->set_column_names(std::move(header));

        auto resultSet = std::forward<std::vector<std::string>>(resp.value());
        std::vector<cpp2::RowValue> table;
        for (auto& result : resultSet) {
            std::vector<std::string> ret_cols;
            folly::split(",", result, ret_cols, true);

            std::vector<cpp2::ColumnValue> row;
            for (auto& col : ret_cols) {
                row.emplace_back();
                row.back().set_str(col);
            }

            table.emplace_back();
            table.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(table));

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
    case nebula::meta::cpp2::AdminJobOp::SHOW_ONE:
        return {"Job Id(TaskId)", "Command(Dest)", "Status", "Start Time", "Stop Time"};
    case nebula::meta::cpp2::AdminJobOp::STOP:
        return {"Result"};
    case nebula::meta::cpp2::AdminJobOp::BACKUP:
        return {"BACKUP Result"};
    case nebula::meta::cpp2::AdminJobOp::RECOVER:
        return {"New Job Id"};
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
        return nebula::meta::cpp2::AdminJobOp::SHOW_ONE;
    } else if (op == "stop_job") {
        return nebula::meta::cpp2::AdminJobOp::STOP;
    } else if (op == "backup_job") {
        return nebula::meta::cpp2::AdminJobOp::BACKUP;
    } else if (op == "recover_job") {
        return nebula::meta::cpp2::AdminJobOp::RECOVER;
    }
    return nebula::meta::cpp2::AdminJobOp::INVALID;
}

}   // namespace graph
}   // namespace nebula
