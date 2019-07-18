/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_FETCHEDGESEXECUTOR_H_
#define GRAPH_FETCHEDGESEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/SchemaWriter.h"

namespace nebula {
namespace graph {
class FetchEdgesExecutor final : public TraverseExecutor {
public:
    FetchEdgesExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "FetchEdgesExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(std::unique_ptr<InterimResult> result) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    Status prepareEdgeKeys();

    Status prepareYield();

    Status setupColumns();

    Status setupEdgeKeys();

    void onEmptyInputs();

    StatusOr<std::vector<storage::cpp2::PropDef>> getPropNames();

    void fetchEdges();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::EdgePropResponse>;
    void processResult(RpcResponse &&result);

    void getOutputSchema(
            meta::SchemaProviderIf *schema,
            const RowReader *reader,
            SchemaWriter *outputSchema) const;

    std::vector<std::string> getResultColumnNames() const;

    void finishExecution(std::unique_ptr<RowSetWriter> rsWriter);

private:
    FetchEdgesSentence                         *sentence_{nullptr};
    std::vector<storage::cpp2::EdgeKey>         edgeKeys_;
    std::vector<YieldColumn*>                   yields_;
    GraphSpaceID                                spaceId_;
    EdgeType                                    edgeType_;
    std::string                                *srcid_;
    std::string                                *dstid_;
    std::string                                *rank_;
    std::string                                 varname_;
    std::unique_ptr<ExpressionContext>          expCtx_;
    std::unique_ptr<InterimResult>              inputs_;
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};
}  // namespace graph
}  // namespace nebula
#endif
