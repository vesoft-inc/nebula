/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
#define STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_

#include "storage/query/QueryBaseProcessor.h"
#include "storage/exec/StoragePlan.h"
#include "storage/context/StorageExpressionContext.h"
#include "common/expression/Expression.h"
#include "common/interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace storage {

class UpdateEdgeProcessor
    : public QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse> {
public:
    static UpdateEdgeProcessor* instance(StorageEnv* env,
                                         stats::Stats* stats) {
        return new UpdateEdgeProcessor(env, stats);
    }

    void process(const cpp2::UpdateEdgeRequest& req) override;

private:
    UpdateEdgeProcessor(StorageEnv* env, stats::Stats* stats)
        : QueryBaseProcessor<cpp2::UpdateEdgeRequest,
                             cpp2::UpdateResponse>(env, stats) {}

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) override;

    StoragePlan<cpp2::EdgeKey> buildPlan(nebula::DataSet* result);

    // Get the schema of all versions of edgeType in the spaceId
    cpp2::ErrorCode buildEdgeSchema();

    // Build EdgeContext by parsing return props expressions,
    // filter expression, update props expression
    cpp2::ErrorCode buildEdgeContext(const cpp2::UpdateEdgeRequest& req);

    void onProcessFinished() override;

    std::vector<Expression*> getReturnPropsExp() {
        std::vector<Expression*> result;
        result.resize(returnPropsExp_.size());
        auto get = [] (auto &ptr) {return ptr.get(); };
        std::transform(returnPropsExp_.begin(), returnPropsExp_.end(), result.begin(), get);
        return result;
    }

private:
    bool                                                            insertable_{false};

    cpp2::EdgeKey                                                   edgeKey_;

    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>     indexes_;

    std::unique_ptr<StorageExpressionContext>                       expCtx_;

    // update <prop name, new value expression>
    std::vector<storage::cpp2::UpdatedProp>                         updatedProps_;

    // return props expression
    std::vector<std::unique_ptr<Expression>>                        returnPropsExp_;

    // condition expression
    std::unique_ptr<Expression>                                     filterExp_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
