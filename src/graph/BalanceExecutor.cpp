/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/BalanceExecutor.h"

namespace nebula {
namespace graph {

BalanceExecutor::BalanceExecutor(Sentence *sentence,
                                 ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<BalanceSentence*>(sentence);
}

Status BalanceExecutor::prepare() {
    return Status::OK();
}

void BalanceExecutor::execute() {
    auto showType = sentence_->subType();
    switch (showType) {
        case BalanceSentence::SubType::kLeader:
            balanceLeader();
            break;
        case BalanceSentence::SubType::kUnknown:
            onError_(Status::Error("Type unknown"));
            break;
    }
}

void BalanceExecutor::balanceLeader() {
    auto future = ectx()->getMetaClient()->balanceLeader();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }
        auto ret = std::move(resp).value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("Balance leader failed"));
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

}   // namespace graph
}   // namespace nebula
