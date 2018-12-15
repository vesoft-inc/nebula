/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/InsertEdgeExecutor.h"

namespace vesoft {
namespace vgraph {

InsertEdgeExecutor::InsertEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<InsertEdgeSentence*>(sentence);
}


Status InsertEdgeExecutor::prepare() {
    overwritable_ = sentence_->overwritable();
    srcid_ = sentence_->srcid();
    dstid_ = sentence_->dstid();
    edge_ = sentence_->edge();
    rank_ = sentence_->rank();
    properties_ = sentence_->properties();
    values_ = sentence_->values();
    // TODO(dutor) check on property names and types
    if (properties_.size() != values_.size()) {
        return Status::Error("Number of property names and values not match");
    }
    return Status::OK();
}


void InsertEdgeExecutor::execute() {
    std::vector<VariantType> values;
    values.resize(values_.size());
    auto eval = [] (auto *expr) { return expr->eval(); };
    std::transform(values_.begin(), values_.end(), values.begin(), eval);

    auto future = ectx()->storage()->addEdge(edge_, srcid_, dstid_, properties_, values);
    auto *runner = ectx()->rctx()->runner();
    std::move(future).via(runner).then([this] (Status status) {
        if (!status.ok()) {
            auto &resp = ectx()->rctx()->resp();
            resp.set_error_code(cpp2::ErrorCode::E_EXECUTION_ERROR);
            resp.set_error_msg(status.toString());
        }
        DCHECK(onFinish_);
        onFinish_();
    });
}

}   // namespace vgraph
}   // namespace vesoft
