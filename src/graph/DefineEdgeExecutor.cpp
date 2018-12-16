/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/DefineEdgeExecutor.h"

namespace nebula {
namespace graph {

DefineEdgeExecutor::DefineEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DefineEdgeSentence*>(sentence);
}


Status DefineEdgeExecutor::prepare() {
    return Status::OK();
}


void DefineEdgeExecutor::execute() {
    auto *sm = ectx()->schemaManager();
    auto *name = sentence_->name();
    auto *src = sentence_->src();
    auto *dst = sentence_->dst();
    auto specs = sentence_->columnSpecs();
    sm->addEdgeSchema(*name, *src, *dst, specs);
    DCHECK(onFinish_);
    onFinish_();
}

}   // namespace graph
}   // namespace nebula
