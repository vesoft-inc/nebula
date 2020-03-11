/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_UPDATEEDGEEXECUTOR_H_
#define GRAPH_UPDATEEDGEEXECUTOR_H_

#include "base/Base.h"
#include "filter/Expressions.h"
#include "graph/Executor.h"
#include "meta/SchemaManager.h"
#include "storage/client/StorageClient.h"

namespace nebula {

namespace storage {

namespace cpp2 {
class UpdateResponse;
}   // namespace cpp2
}   // namespace storage

namespace graph {

class UpdateEdgeExecutor final : public Executor {
public:
    UpdateEdgeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "UpdateEdgeExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    Status prepareData();
    // To do some preparing works on the clauses
    Status prepareSet();

    Status prepareWhen();

    Status prepareYield();

    std::vector<std::string> getReturnColumns();

    void updateEdge(bool reversely);

    // All required data have arrived, finish the execution.
    void toResponse(storage::cpp2::UpdateResponse &&rpcResp);

private:
    UpdateEdgeSentence                         *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
    bool                                        insertable_{false};
    storage::cpp2::EdgeKey                      edge_;
    const std::string                          *edgeTypeName_{nullptr};
    std::vector<storage::cpp2::UpdateItem>      updateItems_;
    Expression                                 *filter_{nullptr};
    std::vector<YieldColumn*>                   yields_;
    std::unique_ptr<ExpressionContext>          expCtx_;
    GraphSpaceID                                spaceId_{-1};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_UPDATEEDGEEXECUTOR_H_
