/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/DescribeSpaceExecutor.h"
#include "meta/SchemaManager.h"

namespace nebula {
namespace graph {

DescribeSpaceExecutor::DescribeSpaceExecutor(Sentence *sentence,
                                             ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DescribeSpaceSentence*>(sentence);
}

Status DescribeSpaceExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}

void DescribeSpaceExecutor::execute() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = ectx()->getMetaClient()->getSpace(spaceId);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }
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

void DescribeSpaceExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
