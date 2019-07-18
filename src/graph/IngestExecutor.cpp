/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/IngestExecutor.h"
#include "process/ProcessUtils.h"

#include <folly/executors/Async.h>
#include <folly/futures/Future.h>

DEFINE_int32(meta_ingest_http_port, 11000, "Default meta daemon's http port");

namespace nebula {
namespace graph {

IngestExecutor::IngestExecutor(Sentence *sentence,
                               ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<IngestSentence*>(sentence);
}

Status IngestExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}

void IngestExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto  addresses = mc->getAddresses();
    auto  metaHost = network::NetworkUtils::intToIPv4(addresses[0].first);
    auto  spaceId = ectx()->rctx()->session()->space();
    auto *path  = sentence_->path();

    auto func = [metaHost, path, spaceId]() {
        std::string tmp = "/usr/bin/curl -G \"http://%s:%d/%s?path=%s&space=%d\"";
        auto command = folly::stringPrintf(tmp.c_str(), metaHost.c_str(),
                                           FLAGS_meta_ingest_http_port,
                                           "ingest-dispatch", path->c_str(), spaceId);
        LOG(INFO) << "Ingest Command: " << command;
        auto result = nebula::ProcessUtils::runCommand(command.c_str());
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
            DCHECK(onError_);
            onError_(Status::Error("Ingest Failed"));
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

void IngestExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
