/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/DefineTagExecutor.h"

namespace nebula {
namespace graph {

DefineTagExecutor::DefineTagExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DefineTagSentence*>(sentence);
}


Status DefineTagExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}


void DefineTagExecutor::execute() {
    auto *sm = ectx()->schemaManager();
    auto *name = sentence_->name();
    auto specs = sentence_->columnSpecs();
    sm->addTagSchema(*name, specs);
    DCHECK(onFinish_);
    onFinish_();
}

}   // namespace graph
}   // namespace nebula
