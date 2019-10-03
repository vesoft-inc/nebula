/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/BalanceExecutor.h"

namespace nebula {
namespace graph {

BalanceExecutor::BalanceExecutor(Sentence *sentence,
                                 ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<BalanceSentence*>(sentence);
}

Status BalanceExecutor::prepare() {
    return Status::OK();
}

void BalanceExecutor::execute() {
    auto showType = sentence_->subType();
    switch (showType) {
        case BalanceSentence::SubType::kLeader:
            balanceLeader();
            break;
        case BalanceSentence::SubType::kData:
            balanceData();
            break;
        case BalanceSentence::SubType::kShowBalancePlan:
            showBalancePlan();
            break;
        case BalanceSentence::SubType::kUnknown:
            onError_(Status::Error("Type unknown"));
            break;
    }
}

void BalanceExecutor::balanceLeader() {
    auto future = ectx()->getMetaClient()->balanceLeader();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }
        auto ret = std::move(resp).value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("Balance leader failed"));
            return;
        }
        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void BalanceExecutor::balanceData() {
    auto future = ectx()->getMetaClient()->balance();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }
        auto balanceId = std::move(resp).value();
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"ID"};
        resp_->set_column_names(std::move(header));

        std::vector<cpp2::RowValue> rows;
        std::vector<cpp2::ColumnValue> row;
        row.resize(1);
        row[0].set_integer(balanceId);
        rows.emplace_back();

        rows.back().set_columns(std::move(row));

        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void BalanceExecutor::showBalancePlan() {
    auto id = sentence_->balanceId();
    auto future = ectx()->getMetaClient()->showBalance(id);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }
        auto tasks = std::move(resp).value();
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"balanceId, spaceId:partId, src->dst", "status"};
        resp_->set_column_names(std::move(header));

        std::vector<cpp2::RowValue> rows;
        rows.reserve(tasks.size());
        for (auto& task : tasks) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(2);
            row[0].set_str(std::move(task.get_id()));
            switch (task.get_result()) {
                case meta::cpp2::TaskResult::SUCCEEDED:
                    row[1].set_str("succeeded");
                    break;
                case meta::cpp2::TaskResult::FAILED:
                    row[1].set_str("failed");
                    break;
                case meta::cpp2::TaskResult::IN_PROGRESS:
                    row[1].set_str("in progress");
                    break;
                case meta::cpp2::TaskResult::INVALID:
                    row[1].set_str("invalid");
                    break;
            }
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));
        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void BalanceExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_) {
        resp = std::move(*resp_);
    } else {
        Executor::setupResponse(resp);
    }
}

}   // namespace graph
}   // namespace nebula
