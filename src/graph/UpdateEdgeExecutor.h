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
    // To do some preparing works on the clauses
    Status prepareSet();

    Status prepareWhere();

    Status prepareYield();

    Status prepareNeededProps();

    StatusOr<std::vector<storage::cpp2::PropDef>> getDstProps();

    StatusOr<std::vector<storage::cpp2::PropDef>> getReturnProps();

    using RpcUpdateResponse = storage::StorageRpcResponse<storage::cpp2::UpdateResponse>;

    bool fetchDstVertexProps();

    // Replace all DestPropertyExpression($$.TagName.PropName) with its value in exp
    bool applyDestPropExp(const Expression* exp);

    bool insertReverselyEdge();

    // All required data have arrived, finish the execution.
    void finishExecution(RpcUpdateResponse &&rpcResp);

    /**
     * A container to hold the mapping from vertex id to its properties, used for lookups
     * during the final evaluation process.
     */
    class VertexHolder final {
    public:
        OptVariantType get(const VertexID id, const int64_t index) const;
        void add(const storage::cpp2::QueryResponse &resp);
        const auto* schema() const {
            return schema_.get();
        }

    private:
        std::shared_ptr<ResultSchemaProvider>       schema_;
        std::unordered_map<VertexID, std::string>   data_;
    };

private:
    using SchemaPropIndex = std::unordered_map<std::pair<std::string, std::string>, int64_t>;
    using EdgeSchema = std::shared_ptr<const meta::SchemaProviderIf>;
    UpdateEdgeSentence                         *sentence_{nullptr};
    bool                                        insertable_{false};
    storage::cpp2::EdgeKey                      edge_;
    EdgeSchema                                  schema_;
    EdgeType                                    edgeType_{0};
    const std::string                          *edgeTypeName_{nullptr};
    std::vector<storage::cpp2::UpdateItem>      updateItems_;
    Expression                                 *filter_{nullptr};
    std::vector<YieldColumn*>                   yields_;
    std::unique_ptr<ExpressionContext>          expCtx_;
    std::unique_ptr<ExpressionContext>          yieldExpCtx_;
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
    std::unique_ptr<VertexHolder>               vertexHolder_;
    SchemaPropIndex                             schemaPropIndex_;
    SchemaPropIndex                             dstTagProps_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_UPDATEEDGEEXECUTOR_H_
