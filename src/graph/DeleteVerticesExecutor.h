/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DELETEVERTEXEXECUTOR_H_
#define GRAPH_DELETEVERTEXEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {

class DeleteVerticesExecutor final : public TraverseExecutor {
public:
    DeleteVerticesExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DeleteVerticesExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    void deleteEdges(std::vector<storage::cpp2::EdgeKey>& edges);
    void deleteVertices();
    void onEmptyInputs();

private:
    DeleteVerticesSentence                     *sentence_{nullptr};
    std::unique_ptr<ExpressionContext>          expCtx_;
    std::vector<VertexID>                       vids_;
    GraphSpaceID                                space_{-1};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_DELETEVERTEXEXECUTOR_H_
