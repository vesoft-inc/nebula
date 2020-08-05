/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_FETCHVERTICESEXECUTOR_H_
#define GRAPH_FETCHVERTICESEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/SchemaWriter.h"

namespace nebula {
namespace graph {
class FetchVerticesExecutor final : public TraverseExecutor {
public:
    FetchVerticesExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "FetchVerticesExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    Status prepareTags();

    Status prepareYield();

    Status prepareClauses();

    Status prepareVids();

    void fetchVertices();

    void onEmptyInputs();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::QueryResponse>;
    void processResult(RpcResponse &&result);

    void finishExecution(std::unique_ptr<RowSetWriter> rsWriter);

    enum FromType {
        kInstantExpr,
        kRef,
    };

private:
    GraphSpaceID                                spaceId_{-1};
    FromType                                    fromType_{kInstantExpr};
    YieldClause                                *yieldClause_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
    std::unique_ptr<ExpressionContext>          expCtx_;
    std::vector<YieldColumn*>                   yields_;
    YieldColumns                                yieldColsHolder_;
    bool                                        distinct_{false};
    FetchVerticesSentence                      *sentence_{nullptr};
    std::vector<std::string>                    colNames_;
    std::vector<nebula::cpp2::SupportedType>    colTypes_;
    std::vector<VertexID>                       vids_;
    std::vector<std::string>                    tagNames_;
    std::unordered_set<std::string>             tagNameSet_;
    std::vector<TagID>                          tagIds_;
    std::vector<storage::cpp2::PropDef>         props_;
    const InterimResult*                        inputsPtr_{nullptr};
    std::string*                                colname_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_FETCHVERTICESEXECUTOR_H_
