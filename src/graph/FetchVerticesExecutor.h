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

    void feedResult(std::unique_ptr<InterimResult> result) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    Status prepareYield();

    Status setupVids();

    void onEmptyInputs();

    StatusOr<std::vector<storage::cpp2::PropDef>> getPropNames();

    void fetchVertices();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::QueryResponse>;
    void processResult(RpcResponse &&result);

private:
    FetchVerticesSentence                      *sentence_;
    std::unique_ptr<ExpressionContext>          expCtx_;
    std::unique_ptr<InterimResult>              inputs_;
    std::vector<VertexID>                       vids_;
    std::string                                *varname_;
    std::string                                *colname_;
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_FETCHVERTICESEXECUTOR_H_
