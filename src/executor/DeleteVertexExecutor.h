/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DELETEVERTEXEXECUTOR_H_
#define GRAPH_DELETEVERTEXEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DeleteVertexExecutor final : public Executor {
public:
    DeleteVertexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DeleteVertexExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    void deleteEdges(std::vector<storage::cpp2::EdgeKey>* edges);
    void deleteVertex();

private:
    DeleteVertexSentence                       *sentence_{nullptr};
    std::unique_ptr<ExpressionContext>          expCtx_;
    VertexID                                    vid_;
    GraphSpaceID                                spaceId_{-1};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_DELETEVERTEXEXECUTOR_H_
