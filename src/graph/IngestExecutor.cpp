/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "http/HttpClient.h"
#include "graph/IngestExecutor.h"
#include "process/ProcessUtils.h"
#include "webservice/Common.h"

#include <folly/executors/Async.h>
#include <folly/futures/Future.h>

namespace nebula {
namespace graph {

IngestExecutor::IngestExecutor(Sentence *sentence,
                               ExecutionContext *ectx) : Executor(ectx, "ingest") {
    sentence_ = static_cast<IngestSentence*>(sentence);
}

Status IngestExecutor::prepare() {
    return Status::OK();
}

void IngestExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    auto *mc = ectx()->getMetaClient();
    auto  addresses = mc->getAddresses();
    auto  metaHost = network::NetworkUtils::intToIPv4(addresses[0].first);
    auto  spaceId = ectx()->rctx()->session()->space();

    auto func = [metaHost, spaceId]() {
        static const char *tmp = "http://%s:%d/%s?space=%d";
        auto url = folly::stringPrintf(tmp, metaHost.c_str(),
                                       FLAGS_ws_meta_http_port,
                                       "ingest-dispatch", spaceId);
        auto result = http::HttpClient::get(url);
        if (result.ok() && result.value() == "SSTFile ingest successfully") {
            LOG(INFO) << "Ingest Successfully";
            return true;
        } else {
            LOG(ERROR) << "Ingest Failed";
            return false;
        }
    };
    auto future = folly::async(func);

    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp) {
            doError(Status::Error("Ingest Failed"));
            return;
        }
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Ingest exception: " << e.what();
        doError(Status::Error("Ingest exception: %s", e.what().c_str()));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void IngestExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
