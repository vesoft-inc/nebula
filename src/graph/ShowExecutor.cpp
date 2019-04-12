/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/ShowExecutor.h"
#include "network/NetworkUtils.h"

namespace nebula {
namespace graph {

using nebula::network::NetworkUtils;

ShowExecutor::ShowExecutor(Sentence *sentence,
                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<ShowSentence*>(sentence);
}


Status ShowExecutor::prepare() {
    return Status::OK();
}


void ShowExecutor::execute() {
    auto showType = sentence_->showType();
    switch (showType) {
        case ShowSentence::ShowType::kShowHosts:
            showHostsExecute();
            break;
        case ShowSentence::ShowType::kUnknown:
            onError_(Status::Error("Show Sentence type unknown"));
            break;
        // intentionally no `default'
    }
}


void ShowExecutor::showHostsExecute() {
    auto future = ectx()->getMetaClient()->listHosts();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("remove hosts failed"));
            return;
        }

        auto retShowHosts = resp.value();
        std::vector<cpp2::RowValue> rows;
        std::vector<cpp2::ColumnValue> row;
        std::vector<std::string> header;
        resp_ = std::make_unique<cpp2::ExecutionResponse>();

        header.clear();
        header.push_back("Ip");
        header.push_back("Port");
        resp_->set_column_names(std::move(header));

        for (auto &host : retShowHosts) {
            row.clear();
            row.resize(2);
            row[0].set_str(NetworkUtils::ipFromHostAddr(host));
            row[1].set_str(folly::to<std::string>(NetworkUtils::portFromHostAddr(host)));
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


void ShowExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
