/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/BalanceExecutor.h"

namespace nebula {
namespace graph {

BalanceExecutor::BalanceExecutor(Sentence *sentence,
                                 ExecutionContext *ectx)
    : Executor(ectx, "balance") {
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
            balanceData(false, false);
            break;
        case BalanceSentence::SubType::kDataStop:
            balanceData(true, false);
            break;
        case BalanceSentence::SubType::kDataReset:
            balanceData(false, true);
            break;
        case BalanceSentence::SubType::kShowBalancePlan:
            showBalancePlan();
            break;
        case BalanceSentence::SubType::kUnknown:
            doError(Status::Error("Type unknown"));
            break;
    }
}

void BalanceExecutor::balanceLeader() {
    auto future = ectx()->getMetaClient()->balanceLeader();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(std::move(resp).status());
            return;
        }
        auto ret = std::move(resp).value();
        if (!ret) {
            doError(Status::Error("Balance leader failed"));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Balance leader exception: " << e.what();
        doError(Status::Error("Balance leader exception: %s", e.what().c_str()));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void BalanceExecutor::balanceData(bool isStop, bool isReset) {
    std::vector<HostAddr> hostDelList;
    auto hostDel = sentence_->hostDel();
    if (hostDel != nullptr) {
        hostDelList = hostDel->hosts();
    }
    auto future = ectx()->getMetaClient()->balance(std::move(hostDelList),
                                                   isStop,
                                                   isReset);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(std::move(resp).status());
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

        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Balance data exception: " << e.what();
        doError(Status::Error("Balance data exception: %s", e.what().c_str()));
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
            doError(std::move(resp).status());
            return;
        }
        auto tasks = std::move(resp).value();
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"balanceId, spaceId:partId, src->dst", "status"};
        resp_->set_column_names(std::move(header));

        std::vector<cpp2::RowValue> rows;
        rows.reserve(tasks.size());
        int32_t succeeded = 0;
        int32_t failed = 0;
        int32_t inProgress = 0;
        int32_t invalid = 0;
        for (auto& task : tasks) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(2);
            row[0].set_str(std::move(task.get_id()));
            switch (task.get_result()) {
                case meta::cpp2::TaskResult::SUCCEEDED:
                    row[1].set_str("succeeded");
                    succeeded++;
                    break;
                case meta::cpp2::TaskResult::FAILED:
                    row[1].set_str("failed");
                    failed++;
                    break;
                case meta::cpp2::TaskResult::IN_PROGRESS:
                    row[1].set_str("in progress");
                    inProgress++;
                    break;
                case meta::cpp2::TaskResult::INVALID:
                    row[1].set_str("invalid");
                    invalid++;
                    break;
            }
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        int32_t total = static_cast<int32_t>(rows.size());
        std::vector<cpp2::ColumnValue> row;
        row.resize(2);
        row[0].set_str(
            folly::stringPrintf("Total:%d, Succeeded:%d, Failed:%d, In Progress:%d, Invalid:%d",
                                total, succeeded, failed, inProgress, invalid));
        row[1].set_str(folly::stringPrintf("%f%%",
                       total == 0 ? 100 : (100 - static_cast<float>(inProgress) / total * 100)));
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        resp_->set_rows(std::move(rows));
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Show balance plan exception: " << e.what();
        doError(Status::Error("Show balance plan exception: %s", e.what().c_str()));
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
