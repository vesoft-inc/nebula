/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_FETCHVERTICESEXECUTOR_H_
#define GRAPH_FETCHVERTICESEXECUTOR_H_

#include "base/Base.h"
#include "graph/FetchExecutor.h"
#include "storage/client/StorageClient.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/SchemaWriter.h"

namespace nebula {
namespace graph {
class FetchVerticesExecutor final : public FetchExecutor {
public:
    FetchVerticesExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "FetchVerticesExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    Status prepareClauses();

    Status prepareVids();

    Status setupVids();

    Status setupVidsFromRef();

    Status setupVidsFromExpr();

    std::vector<storage::cpp2::PropDef> getPropNames();

    void fetchVertices();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::QueryResponse>;
    void processResult(RpcResponse &&result);

private:
    FetchVerticesSentence                      *sentence_{nullptr};
    std::vector<VertexID>                       vids_;
    TagID                                       tagID_{INT_MIN};
    std::string                                *varname_{nullptr};
    std::string                                *colname_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_FETCHVERTICESEXECUTOR_H_
