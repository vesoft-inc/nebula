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

using RpcResponse = storage::StorageRpcResponse<storage::cpp2::LookUpIndexResp>;

class LookupExecutor final : public TraverseExecutor {
    FRIEND_TEST(LookupTest, OptimizerTest);

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

    Status prepareIndexes();

    Status prepareWhere();

    Status prepareYield();

    Status checkAliasProperty(const AliasPropertyExpression* aExpr);

    Status optimize();

    Status traversalExpr(const Expression *expr);

    Status checkFilter();

    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> findValidIndex();

    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> findIndexForEqualScan(
        const std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>& indexes);

    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> findIndexForRangeScan(
        const std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>& indexes);

    Status findOptimalIndex();

    void lookUp();

    std::vector<std::string> getResultColumnNames() const;

    using Callback = std::function<Status(std::vector<VariantType>,
                                          const std::vector<nebula::cpp2::SupportedType>&)>;

    void finishExecution(RpcResponse &&resp);

    bool setupInterimResult(RpcResponse &&resp,
                            std::unique_ptr<InterimResult> &result);

    StatusOr<std::vector<cpp2::RowValue>> toThriftResponse(RpcResponse&& resp);

    bool processFinalEdgeResult(RpcResponse &rpcResp, const Callback& cb) const;

    bool processFinalVertexResult(RpcResponse &rpcResp, const Callback& cb) const;


private:
    using FilterItem = std::pair<std::string, RelationalExpression::Operator>;
    LookupSentence                                 *sentence_{nullptr};
    GraphSpaceID                                   spaceId_;
    IndexID                                        index_;
    const std::string                              *from_{nullptr};
    std::vector<YieldColumn*>                      yields_;
    std::unique_ptr<YieldClauseWrapper>            yieldClauseWrapper_;
    std::unique_ptr<ExpressionContext>             expCtx_;
    int32_t                                        tagOrEdge_;
    bool                                           isEdge_{false};
    std::unique_ptr<cpp2::ExecutionResponse>       resp_;
    std::vector<std::string>                       returnCols_;
    std::vector<FilterItem>                        filters_;
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> indexes_;
};
}  // namespace graph
}  // namespace nebula
#endif  // NEBULA_GRAPH_LOOKUPEXECUTOR_H
