/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/CompoundExecutor.h"
#include "vgraphd/GoExecutor.h"
#include "vgraphd/PipeExecutor.h"
#include "vgraphd/UseExecutor.h"

namespace vesoft {
namespace vgraph {

CompoundExecutor::CompoundExecutor(CompoundSentence *compound,
                                   ExecutionContext *ectx) : Executor(ectx) {
    compound_ = compound;
}


Status CompoundExecutor::prepare() {
    for (auto i = 0U; i < compound_->sentences_.size(); i++) {
        auto *sentence = compound_->sentences_[i].get();
        auto executor = makeExecutor(sentence);
        DCHECK(executor != nullptr);
        auto status = executor->prepare();
        if (!status.ok()) {
            FLOG_ERROR("Prepare executor `%s' failed: %s",
                        executor->name(), status.toString().c_str());
            return status;
        }
        executors_.emplace_back(std::move(executor));
    }
    /**
     * For the time being, we execute sentences one by one. We may allow concurrent
     * or out of order execution in the future.
     */
    // For an executor except the last one, it executes the next one on finished.
    // If any fails, the whole execution would abort.
    auto onError = [this] (Status status) {
        DCHECK(onError_);
        onError_(std::move(status));
    };
    for (auto i = 0U; i < executors_.size() - 1; i++) {
        auto onFinish = [this, next = i + 1] () {
            executors_[next]->execute();
        };
        executors_[i]->setOnFinish(onFinish);
        executors_[i]->setOnError(onError);
    }
    // The whole execution is done upon the last executor finishes.
    auto onFinish = [this] () {
        DCHECK(onFinish_);
        onFinish_();
    };
    executors_.back()->setOnFinish(onFinish);
    executors_.back()->setOnError(onError);

    return Status::OK();
}


void CompoundExecutor::execute() {
    executors_.front()->execute();
}


void CompoundExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    executors_.back()->setupResponse(resp);
}

}   // namespace vgraph
}   // namespace vesoft
