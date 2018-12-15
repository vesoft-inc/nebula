/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/InsertVertexExecutor.h"

namespace vesoft {
namespace vgraph {

InsertVertexExecutor::InsertVertexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<InsertVertexSentence*>(sentence);
}


Status InsertVertexExecutor::prepare() {
    overwritable_ = sentence_->overwritable();
    id_ = sentence_->id();
    vertex_ = sentence_->vertex();
    properties_ = sentence_->properties();
    values_ = sentence_->values();
    // TODO(dutor) check on property names and types
    if (properties_.size() != values_.size()) {
        return Status::Error("Number of property names and values not match");
    }
    return Status::OK();
}


void InsertVertexExecutor::execute() {
    std::vector<VariantType> values;
    values.resize(values_.size());
    auto eval = [] (auto *expr) { return expr->eval(); };
    std::transform(values_.begin(), values_.end(), values.begin(), eval);

    auto future = ectx()->storage()->addTag(vertex_, id_, properties_, values);
    auto *runner = ectx()->rctx()->runner();
    std::move(future).via(runner).then([this] (Status status) {
        DCHECK(onFinish_);
        onFinish_();
    });
}

}   // namespace vgraph
}   // namespace vesoft
