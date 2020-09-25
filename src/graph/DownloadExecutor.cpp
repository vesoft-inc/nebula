/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/StatusOr.h"
#include "http/HttpClient.h"
#include "graph/DownloadExecutor.h"
#include "process/ProcessUtils.h"
#include "webservice/Common.h"

#include <folly/executors/Async.h>
#include <folly/futures/Future.h>
#include <folly/executors/ThreadedExecutor.h>

namespace nebula {
namespace graph {

DownloadExecutor::DownloadExecutor(Sentence *sentence,
                                   ExecutionContext *ectx)
    : Executor(ectx, "download") {
    sentence_ = static_cast<DownloadSentence*>(sentence);
}

Status DownloadExecutor::prepare() {
    return Status::OK();
}

void DownloadExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    auto *mc = ectx()->getMetaClient();
    auto  addresses = mc->getAddresses();
    auto  metaHost = addresses[0].getAddressStr();
    auto  spaceId = ectx()->rctx()->session()->space();
    auto *hdfsHost  = sentence_->host();
    auto  hdfsPort  = sentence_->port();
    auto *hdfsPath  = sentence_->path();
    if (hdfsHost == nullptr || hdfsPort == 0 || hdfsPath == nullptr) {
        LOG(ERROR) << "URL Parse Failed";
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        doError(Status::Error("URL Parse Failed"));
        return;
    }

    std::string url;

    if (sentence_->edge()) {
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(
            spaceId, *sentence_->edge());
        if (!edgeStatus.ok()) {
            doError(Status::Error("edge not found."));
            return;
        }
        auto edgeType = edgeStatus.value();
        url = folly::stringPrintf(
            "http://%s:%d/download-dispatch?host=%s&port=%d&path=%s&space=%d&edge=%d",
            metaHost.c_str(), FLAGS_ws_meta_http_port,
            hdfsHost->c_str(), hdfsPort, hdfsPath->c_str(), spaceId, edgeType);
    } else if (sentence_->tag()) {
        auto tagStatus = ectx()->schemaManager()->toTagID(
            spaceId, *sentence_->tag());
        if (!tagStatus.ok()) {
            doError(Status::Error("tag not found."));
            return;
        }
        auto tagType = tagStatus.value();
        url = folly::stringPrintf(
            "http://%s:%d/download-dispatch?host=%s&port=%d&path=%s&space=%d&tag=%d",
            metaHost.c_str(), FLAGS_ws_meta_http_port,
            hdfsHost->c_str(), hdfsPort, hdfsPath->c_str(), spaceId, tagType);
    } else {
        url = folly::stringPrintf(
            "http://%s:%d/download-dispatch?host=%s&port=%d&path=%s&space=%d",
            metaHost.c_str(), FLAGS_ws_meta_http_port,
            hdfsHost->c_str(), hdfsPort, hdfsPath->c_str(), spaceId);
    }

    auto func = [url] {
        auto result = http::HttpClient::get(url);
        if (result.ok() && result.value() == "SSTFile dispatch successfully") {
            LOG(INFO) << "Download Successfully";
            return true;
        } else {
            LOG(ERROR) << "Download Failed: " << result.value();
            return false;
        }
    };
    auto future = folly::async(func);

    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp) {
            doError(Status::Error("Download Failed"));
            return;
        }
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Download exception: " << e.what();
        doError(Status::Error("Download exception: %s", e.what().c_str()));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DownloadExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula

