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
    auto  metaHost = addresses[0].getAddressStr();
    auto  spaceId = ectx()->rctx()->session()->space();

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
            "http://%s:%d/ingest-dispatch?space=%d&edge=%d",
            metaHost.c_str(), FLAGS_ws_meta_http_port, spaceId, edgeType);
    } else if (sentence_->tag()) {
        auto tagStatus = ectx()->schemaManager()->toTagID(
            spaceId, *sentence_->tag());
        if (!tagStatus.ok()) {
            doError(Status::Error("tag not found."));
            return;
        }
        auto tagType = tagStatus.value();
        url = folly::stringPrintf(
            "http://%s:%d/ingest-dispatch?space=%d&tag=%d",
            metaHost.c_str(), FLAGS_ws_meta_http_port, spaceId, tagType);
    } else {
        url = folly::stringPrintf(
            "http://%s:%d/ingest-dispatch?space=%d",
            metaHost.c_str(), FLAGS_ws_meta_http_port, spaceId);
    }

    auto func = [url] {
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
