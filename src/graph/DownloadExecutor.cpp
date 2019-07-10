/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DownloadExecutor.h"
#include "process/ProcessUtils.h"
#include "base/StatusOr.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <folly/executors/Async.h>
#include <folly/futures/Future.h>
#include <folly/executors/ThreadedExecutor.h>

DEFINE_int32(meta_http_port, 11000, "Default meta daemon's http port");

namespace nebula {
namespace graph {

DownloadExecutor::DownloadExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DownloadSentence*>(sentence);
}

Status DownloadExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}

void DownloadExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto  addresses = mc->getAddresses();
    auto  metaHost = network::NetworkUtils::intToIPv4(addresses[0].first);
    auto  spaceId = ectx()->rctx()->session()->space();
    auto *hdfsHost  = sentence_->host();
    auto  hdfsPort  = sentence_->port();
    auto *hdfsPath  = sentence_->path();
    auto *hdfsLocal = sentence_->localPath();

    auto func = [metaHost, hdfsHost, hdfsPort, hdfsPath, hdfsLocal, spaceId]() {
        auto tmp = "%s \"http://%s:%d/%s?host=%s&port=%d&path=%s&local=%s&space=%d\"";
        auto command = folly::stringPrintf(tmp, "/usr/bin/curl -G", metaHost.c_str(),
                                           FLAGS_meta_http_port, "download-dispatch",
                                           hdfsHost->c_str(), hdfsPort, hdfsPath->c_str(),
                                           hdfsLocal->c_str(), spaceId);
        LOG(INFO) << "Download Command: " << command;
        auto result = nebula::ProcessUtils::runCommand(command.c_str());
        if (result.ok() && result.value() == "SSTFile dispatch successfully") {
            LOG(INFO) << "Download Successfully";
            return true;
        } else {
            LOG(ERROR) << "Download Failed ";
            return false;
        }
    };
    auto future = folly::async(func);

    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp) {
            DCHECK(onError_);
            onError_(Status::Error("Download Failed"));
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

void DownloadExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula

