/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/PipeExecutor.h"

namespace nebula {
namespace graph {

PipeExecutor::PipeExecutor(Sentence *sentence,
                           ExecutionContext *ectx) : TraverseExecutor(ectx) {
    sentence_ = static_cast<PipedSentence*>(sentence);
}


Status PipeExecutor::prepare() {
    auto status = syntaxPreCheck();
    if (!status.ok()) {
        return status;
    }

    left_ = makeTraverseExecutor(sentence_->left());
    right_ = makeTraverseExecutor(sentence_->right());
    DCHECK(left_ != nullptr);
    DCHECK(right_ != nullptr);

    auto onError = [this] (Status s) {
        /**
         * TODO(dutor)
         * If we allow concurrent execution of both sides, we must wait for all their
         * internal on-fly async-requests' to arrive before call `onError_'.
         * For the sake of simplicity, we don't allow the pipeline way for now.
         */
        onError_(std::move(s));
    };

    // Setup dependencies
    {
        auto onFinish = [this] (Executor::ProcessControl ctr) {
            UNUSED(ctr);
            // Start executing `right_' when `left_' is finished.
            left_.reset();
            right_->execute();
        };
        left_->setOnFinish(onFinish);

        auto onResult = [this] (std::unique_ptr<InterimResult> result) {
            // Feed results from `left_' to `right_'
            // result should never be null, it should give the column names at least.
            DCHECK(result != nullptr);
            right_->feedResult(std::move(result));
        };
        left_->setOnResult(onResult);

        left_->setOnError(onError);
    }
    {
        auto onFinish = [this] (Executor::ProcessControl ctr) {
            // This executor is done when `right_' finishes.
            DCHECK(onFinish_);
            onFinish_(ctr);
        };
        right_->setOnFinish(onFinish);

        if (onResult_) {
            auto onResult = [this] (std::unique_ptr<InterimResult> result) {
                // This executor takes results of `right_' as results.
                // result should never be null, it should give the column names at least.
                DCHECK(result != nullptr);
                onResult_(std::move(result));
            };
            right_->setOnResult(onResult);
        } else {
            // `right_' is the right most part of the pipeline
        }

        right_->setOnError(onError);
    }

    status = left_->prepare();
    if (!status.ok()) {
        FLOG_ERROR("Prepare executor `%s' failed: %s",
                    left_->name(), status.toString().c_str());
        return status;
    }

    status = right_->prepare();
    if (!status.ok()) {
        FLOG_ERROR("Prepare executor `%s' failed: %s",
                    right_->name(), status.toString().c_str());
        return status;
    }


    return Status::OK();
}

Status PipeExecutor::syntaxPreCheck() {
    // Set op not support input,
    // because '$-' would be ambiguous in such a situation:
    // Go | (Go | Go $- UNION GO)
    if (sentence_->right()->kind() == Sentence::Kind::kSet) {
        return Status::SyntaxError("Set op not support input.");
    }

    if (sentence_->left()->kind() == Sentence::Kind::kFindPath) {
        return Status::SyntaxError("Can not reference the result of FindPath.");
    }

    return Status::OK();
}

void PipeExecutor::execute() {
    left_->execute();
}


void PipeExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    left_->feedResult(std::move(result));
}


void PipeExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    /**
     * `setupResponse()' could be invoked if and only if this executor
     * is the right most one, i.e. `onResult_' wasn't set.
     */
    DCHECK(!onResult_);
    right_->setupResponse(resp);
}

}   // namespace graph
}   // namespace nebula
