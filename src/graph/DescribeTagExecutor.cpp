/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/DescribeTagExecutor.h"

namespace nebula {
namespace graph {

DescribeTagExecutor::DescribeTagExecutor(Sentence *sentence,
                                         ExecutionContext *ectx)
    : DescribeSchemaExecutor(ectx, "describe_tag") {
    sentence_ = static_cast<DescribeTagSentence*>(sentence);
}


Status DescribeTagExecutor::prepare() {
    return Status::OK();
}


void DescribeTagExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();

    // Get the lastest ver
    auto future = ectx()->getMetaClient()->getTagSchema(spaceId, *name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Describe tag `%s' failed.",
                        sentence_->name()->c_str()));
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(header_);
        resp_->set_rows(genRows(resp.value().columns));
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Describe tag `%s' exception: %s",
                sentence_->name()->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void DescribeTagExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
