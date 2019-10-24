/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "http/HttpClient.h"
#include "graph/CompactionExecutor.h"
#include "process/ProcessUtils.h"
#include "webservice/Common.h"

#include <folly/executors/Async.h>
#include <folly/futures/Future.h>

namespace nebula {
namespace graph {

CompactionExecutor::CompactionExecutor(Sentence *sentence,
                               ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CompactionSentence*>(sentence);
}

Status CompactionExecutor::prepare() {
    return Status::OK();
}

void CompactionExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }
    auto *mc = ectx()->getMetaClient();
    auto  addresses = mc->getAddresses();
    auto  metaHost = network::NetworkUtils::intToIPv4(addresses[0].first);
    auto spaceName = ectx()->rctx()->session()->spaceName();
    auto  spaceId = ectx()->rctx()->session()->space();

    auto func = [metaHost, spaceName, spaceId]() {
        static const char *tmp = "http://%s:%d/%s?spaceName=%s&spaceId=%d";
        auto url = folly::stringPrintf(tmp, metaHost.c_str(),
                                       FLAGS_ws_meta_http_port,
                                       "compact-dispatch",
                                       spaceName.c_str(), spaceId);
        auto result = http::HttpClient::get(url);
        return result.ok();
    };
    auto future = folly::async(func);

    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp) {
            DCHECK(onError_);
            onError_(Status::Error("Compaction Failed"));
            return;
        }
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
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

void CompactionExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
