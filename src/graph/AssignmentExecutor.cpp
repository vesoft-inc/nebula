/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/AssignmentExecutor.h"
#include "graph/TraverseExecutor.h"


namespace nebula {
namespace graph {


AssignmentExecutor::AssignmentExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AssignmentSentence*>(sentence);
}


Status AssignmentExecutor::prepare() {
    var_ = sentence_->var();
    executor_ = TraverseExecutor::makeTraverseExecutor(sentence_->sentence(), ectx());

    auto onError = [this] (Status s) {
        DCHECK(onError_);
        onError_(std::move(s));
    };
    auto onFinish = [this] (Executor::ProcessControl ctr) {
        DCHECK(onFinish_);
        onFinish_(ctr);
    };
    auto onResult = [this] (std::unique_ptr<InterimResult> result) {
        ectx()->variableHolder()->add(*var_, std::move(result));
    };
    executor_->setOnError(onError);
    executor_->setOnFinish(onFinish);
    executor_->setOnResult(onResult);

    auto status = executor_->prepare();
    if (!status.ok()) {
        FLOG_ERROR("Prepare executor `%s' failed: %s",
                    executor_->name(), status.toString().c_str());
        return status;
    }

    return Status::OK();
}


void AssignmentExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }
    executor_->execute();
}


}   // namespace graph
}   // namespace nebula
