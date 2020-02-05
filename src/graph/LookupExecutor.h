/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_GRAPH_LOOKUPEXECUTOR_H
#define NEBULA_GRAPH_LOOKUPEXECUTOR_H

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {
class LookupExecutor final : public TraverseExecutor {
public:
    LookupExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char *name() const override {
        return "LookupExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(std::unique_ptr<InterimResult> result) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    Status prepareClauses();

    Status prepareFrom();

    Status prepareWhere();

    Status prepareYield();

    Status optimize();

    Status traversalExpr(const Expression *expr);

    Status checkFilter();

    std::vector<IndexID>
    findValidIndex(const std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>& ids);

    Status findOptimalIndex(std::vector<IndexID> ids);

    Status chooseIndex();

    void stepEdgeOut();

    void stepVertexOut();

    void finishExecution(std::unique_ptr<RowSetWriter> rsWriter);

    void setOutputYields(SchemaWriter *outputSchema,
                         const std::shared_ptr<ResultSchemaProvider>& schema);

    using EdgeRpcResponse = storage::StorageRpcResponse<storage::cpp2::LookUpEdgeIndexResp>;
    void processEdgeResult(EdgeRpcResponse &&result);

    using VertexRpcResponse = storage::StorageRpcResponse<storage::cpp2::LookUpVertexIndexResp>;
    void processVertexResult(VertexRpcResponse &&result);

private:
    using FilterItem = std::pair<std::string, RelationalExpression::Operator>;
    LookupSentence                                 *sentence_{nullptr};
    GraphSpaceID                                   spaceId_;
    IndexID                                        index_;
    const std::string                              *from_{nullptr};
    std::unique_ptr<WhereWrapper>                  whereWrapper_;
    std::vector<YieldColumn*>                      yields_;
    std::unique_ptr<YieldClauseWrapper>            yieldClauseWrapper_;
    std::unique_ptr<ExpressionContext>             expCtx_;
    int32_t                                        tagOrEdge_;
    bool                                           isEdge_{false};
    bool                                           skipOptimize_{false};
    std::unique_ptr<InterimResult>                 inputs_;
    std::unique_ptr<cpp2::ExecutionResponse>       resp_;
    std::vector<std::string>                       returnCols_;
    std::vector<FilterItem>                        filters_;
};
}  // namespace graph
}  // namespace nebula
#endif  // NEBULA_GRAPH_LOOKUPEXECUTOR_H
