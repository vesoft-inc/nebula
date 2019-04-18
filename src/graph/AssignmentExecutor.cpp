/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        return status;
    }

    var_ = sentence_->var();
    executor_ = TraverseExecutor::makeTraverseExecutor(sentence_->sentence(), ectx());
    status = executor_->prepare();
    if (!status.ok()) {
        FLOG_ERROR("Prepare executor `%s' failed: %s",
                    executor_->name(), status.toString().c_str());
        return status;
    }
    auto onError = [this] (Status s) {
        DCHECK(onError_);
        onError_(std::move(s));
    };
    auto onFinish = [this] () {
        DCHECK(onFinish_);
        onFinish_();
    };
    auto onResult = [this] (std::unique_ptr<InterimResult> result) {
        ectx()->variableHolder()->add(*var_, std::move(result));
    };
    executor_->setOnError(onError);
    executor_->setOnFinish(onFinish);
    executor_->setOnResult(onResult);

    return Status::OK();
}


void AssignmentExecutor::execute() {
    executor_->execute();
}


}   // namespace graph
}   // namespace nebula
