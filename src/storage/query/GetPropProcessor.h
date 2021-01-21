/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_GETPROPPROCESSOR_H_
#define STORAGE_QUERY_GETPROPPROCESSOR_H_

#include "common/base/Base.h"
#include <gtest/gtest_prod.h>
#include "storage/query/QueryBaseProcessor.h"
#include "storage/exec/StoragePlan.h"

namespace nebula {
namespace storage {

class GetPropProcessor
    : public QueryBaseProcessor<cpp2::GetPropRequest, cpp2::GetPropResponse> {
public:
    static GetPropProcessor* instance(StorageEnv* env,
                                      stats::Stats* stats,
                                      folly::Executor* executor = nullptr,
                                      VertexCache* cache = nullptr) {
        return new GetPropProcessor(env, stats, executor, cache);
    }

    void process(const cpp2::GetPropRequest& req) override;

    void doProcess(const cpp2::GetPropRequest& req);

protected:
    GetPropProcessor(StorageEnv* env,
                     stats::Stats* stats,
                     folly::Executor* executor,
                     VertexCache* cache)
        : QueryBaseProcessor<cpp2::GetPropRequest,
                             cpp2::GetPropResponse>(env, stats, executor, cache) {}

    StoragePlan<VertexID> buildTagPlan(nebula::DataSet* result);

    StoragePlan<cpp2::EdgeKey> buildEdgePlan(nebula::DataSet* result);

    void onProcessFinished() override;

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetPropRequest& req) override;

    cpp2::ErrorCode checkRequest(const cpp2::GetPropRequest& req);

    cpp2::ErrorCode buildTagContext(const cpp2::GetPropRequest& req);

    cpp2::ErrorCode buildEdgeContext(const cpp2::GetPropRequest& req);

    void buildTagColName(const std::vector<cpp2::VertexProp>& tagProps);
    void buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps);

private:
    bool isEdge_ = false;                   // true for edge, false for tag
    std::unique_ptr<StorageExpressionContext> expCtx_;
    std::vector<std::unique_ptr<Expression>> yields_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETPROPPROCESSOR_H_
