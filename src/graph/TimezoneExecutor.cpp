/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/TimezoneExecutor.h"

namespace nebula {
namespace graph {

TimezoneExecutor::TimezoneExecutor(Sentence *sentence, ExecutionContext *ectx)
        : Executor(ectx) {
    sentence_ = static_cast<TimezoneSentence*>(sentence);
}


Status TimezoneExecutor::prepare() {
    if (sentence_->getType() == TimezoneSentence::TimezoneType::kSetTimezone) {
        auto *timezone = sentence_->getTimezone();
        static const std::regex reg("^([-|+])(((0[0-9]|1[0-2]):([0-5]\\d))|((13):(00)))$");
        std::smatch result;
        if (!std::regex_match(*timezone, result, reg)) {
            return Status::SyntaxError("Invalid timezone type");
        }
        timezone_.eastern = result[1].str()[0];
        timezone_.hour = atoi(result[2].str().substr(0, 2).c_str());
        timezone_.minute = atoi(result[2].str().substr(2, 2).c_str());
    }
    return Status::OK();
}


void TimezoneExecutor::execute() {
    switch (sentence_->getType()) {
        case TimezoneSentence::TimezoneType::kSetTimezone:
            return setTimezone();
        case TimezoneSentence::TimezoneType::kGetTimezone:
            return getTimezone();
        default:
            onError_(Status::Error("Unknown type: %d", sentence_->kind()));
            return;
    }
}


void TimezoneExecutor::getTimezone() {
    auto *mc = ectx()->getMetaClient();
    auto future = mc->getTimezone();
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("GetTimezone fail"));
            return;
        }
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"Timezone"};
        resp_->set_column_names(std::move(header));
        std::vector<cpp2::RowValue> rows;
        std::vector<cpp2::ColumnValue> row;
        row.resize(1);
        row[0].set_str(folly::stringPrintf("%c%02d:%02d",
                resp.value().get_eastern() == 0 ? '+' : resp.value().get_eastern(),
                resp.value().get_hour(), resp.value().get_minute()));
        rows.emplace_back();
        rows.back().set_columns(std::move(row));

        resp_->set_rows(std::move(rows));
        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void TimezoneExecutor::setTimezone() {
    auto *mc = ectx()->getMetaClient();
    auto future = mc->setTimezone(timezone_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void TimezoneExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (sentence_->getType() == TimezoneSentence::TimezoneType::kSetTimezone) {
        return;
    }
    resp = std::move(*resp_);
}
}   // namespace graph
}   // namespace nebula

