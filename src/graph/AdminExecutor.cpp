/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "http/HttpClient.h"
#include "graph/AdminExecutor.h"
#include "process/ProcessUtils.h"
#include "webservice/Common.h"

#include <folly/executors/Async.h>
#include <folly/futures/Future.h>

namespace nebula {
namespace graph {

AdminExecutor::AdminExecutor(Sentence *sentence,
        ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AdminSentence*>(sentence);
    header_["add_job"] = "Job Id";
    header_["show_jobs"] = "Job Id,Command,Status,Start Time,Stop Time";
    header_["show_job"] = "Job Id(TaskId),Command(Dest),Status,Start Time,Stop Time";
    header_["stop_job"] = "Job Id";
    header_["backup_job"] = "Job num backuped";
    header_["recover_job"] = "Job num recovered";
}

Status AdminExecutor::prepare() {
    return Status::OK();
}

bool AdminExecutor::opNeedsSpace(const std::string& op) {
    return op == "add_job";
}

void AdminExecutor::execute() {
    if (opNeedsSpace(sentence_->getType())) {
        auto status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            DCHECK(onError_);
            onError_(std::move(status));
            return;
        }
    }

    auto *mc = ectx()->getMetaClient();
    auto addresses = mc->getAddresses();
    auto metaHost = network::NetworkUtils::intToIPv4(addresses[0].first);
    auto spaceName = ectx()->rctx()->session()->spaceName();
    auto sType = sentence_->getType();
    std::string optionalParas("");
    for (auto i = 0U; i != sentence_->getParas().size(); ++i) {
        optionalParas.append("&para" + std::to_string(i) + "=" + sentence_->getParas()[i]);
    }

    auto func = [sType, metaHost, spaceName, optionalParas]() {
        static const char *tmp = "http://%s:%d/%s?op=%s&spaceName=%s&%s";
        auto url = folly::stringPrintf(tmp, metaHost.c_str(),
                FLAGS_ws_meta_http_port,
                "admin-dispatch",
                sType.c_str(),
                spaceName.c_str(),
                optionalParas.c_str());
        auto result = http::HttpClient::get(url);
        return result;
    };
    auto future = folly::async(func);

    auto *runner = ectx()->rctx()->runner();

    auto cb = [this, sType] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("Admin Failed"));
            return;
        }

        std::string str_header;
        if (header_.count(sType)) {
            str_header = header_[sType];
        }

        std::string str_body = resp.value();

        resp_ = std::make_unique<cpp2::ExecutionResponse>();

        if (!str_header.empty()) {
            std::vector<std::string> header;
            folly::split(",", str_header, header, true);
            resp_->set_column_names(std::move(header));
        }

        if (!str_body.empty()) {
            std::vector<std::string> bodies;
            folly::split("\n", str_body, bodies, true);
            std::vector<cpp2::RowValue> table(bodies.size());
            for (auto i = 0U; i != table.size(); ++i) {
                std::vector<std::string> str_cols;
                folly::split(",", bodies[i], str_cols, true);
                std::vector<cpp2::ColumnValue> row(str_cols.size());
                for (auto j = 0U; j != row.size(); ++j) {
                    row[j].set_str(str_cols[j]);
                }
                table[i].set_columns(std::move(row));
            }
            resp_->set_rows(std::move(table));
        }

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
        // onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void AdminExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
