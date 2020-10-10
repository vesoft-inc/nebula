/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_FETCHEDGESEXECUTOR_H_
#define GRAPH_FETCHEDGESEXECUTOR_H_

#include "base/Base.h"
#include "graph/FetchExecutor.h"
#include "storage/client/StorageClient.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/SchemaWriter.h"

namespace nebula {
namespace graph {
class FetchEdgesExecutor final : public FetchExecutor {
public:
    FetchEdgesExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "FetchEdgesExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    Status prepareClauses();

    Status prepareEdgeKeys();

    Status checkEdgeProps();

    Status setupEdgeKeys();

    Status setupEdgeKeysFromExpr();

    Status setupEdgeKeysFromRef();

    Status getPropNames(std::vector<storage::cpp2::PropDef> &props);

    void fetchEdges();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::EdgePropResponse>;
    void processResult(RpcResponse &&result);

    using EdgeKeyHashSet = std::unordered_set<
            storage::cpp2::EdgeKey,
            std::function<size_t(const storage::cpp2::EdgeKey& key)>>;

private:
    FetchEdgesSentence                                     *sentence_{nullptr};
    std::vector<storage::cpp2::EdgeKey>                     edgeKeys_;
    EdgeType                                                edgeType_{INT_MIN};
    std::string                                            *srcid_{nullptr};
    std::string                                            *dstid_{nullptr};
    std::string                                            *rank_{nullptr};
    std::string                                             varname_;
    std::function<size_t(const storage::cpp2::EdgeKey)>     hash_;
    std::string                                             edgeSrcName_;
    std::string                                             edgeDstName_;
    std::string                                             edgeRankName_;
};
}  // namespace graph
}  // namespace nebula
#endif
