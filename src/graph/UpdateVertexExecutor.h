/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_UPDATEVERTEXEXECUTOR_H_
#define GRAPH_UPDATEVERTEXEXECUTOR_H_

#include "base/Base.h"
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

class UpdateVertexExecutor final : public Executor {
public:
    UpdateVertexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "UpdateVertexExecutor";
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

    StatusOr<std::vector<storage::cpp2::PropDef>> getReturnProps();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::UpdateResponse>;

    // All required data have arrived, finish the execution.
    void finishExecution(RpcResponse &&rpcResp);

private:
    using SchemaPropIndex = std::unordered_map<std::pair<std::string, std::string>, int64_t>;
    UpdateVertexSentence                       *sentence_{nullptr};
    bool                                        insertable_{false};
    VertexID                                    vertex_;
    std::vector<storage::cpp2::UpdateItem>      updateItems_;
    Expression                                 *filter_{nullptr};
    std::vector<YieldColumn*>                   yields_;
    std::unique_ptr<ExpressionContext>          expCtx_;
    std::unique_ptr<ExpressionContext>          yieldExpCtx_;
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
    SchemaPropIndex                             schemaPropIndex_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_UPDATEVERTEXEXECUTOR_H_
