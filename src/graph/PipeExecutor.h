/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_PIPEEXECUTOR_H_
#define GRAPH_PIPEEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {

class PipeExecutor final : public TraverseExecutor {
public:
    PipeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "PipeExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(std::unique_ptr<IntermResult> result) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

    ResultSchema* resultSchema() const override {
        return right_->resultSchema();
    }

    void setInputResultSchema(ResultSchema *schema) override {
        left_->setInputResultSchema(schema);
    }

private:
    PipedSentence                              *sentence_{nullptr};
    std::unique_ptr<TraverseExecutor>           left_;
    std::unique_ptr<TraverseExecutor>           right_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_PIPEEXECUTOR_H_
