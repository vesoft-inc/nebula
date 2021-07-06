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

extern ProcessorCounters kUpdateEdgeCounters;

class UpdateEdgeProcessor
    : public QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse> {
public:
    static UpdateEdgeProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kUpdateEdgeCounters,
            folly::Executor* executor = nullptr) {
        return new UpdateEdgeProcessor(env, counters, executor);
    }

    void process(const cpp2::UpdateEdgeRequest& req) override;

    void doProcess(const cpp2::UpdateEdgeRequest& req);

private:
    UpdateEdgeProcessor(StorageEnv* env,
                        const ProcessorCounters* counters,
                        folly::Executor* executor)
        : QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse>(env,
                                                                            counters,
                                                                            executor) {}

    nebula::cpp2::ErrorCode
    checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) override;

    StoragePlan<cpp2::EdgeKey> buildPlan(nebula::DataSet* result);

    // Get the schema of all versions of edgeType in the spaceId
    nebula::cpp2::ErrorCode buildEdgeSchema();

    // Build EdgeContext by parsing return props expressions,
    // filter expression, update props expression
    nebula::cpp2::ErrorCode buildEdgeContext(const cpp2::UpdateEdgeRequest& req);

    void onProcessFinished() override;

    std::vector<Expression*> getReturnPropsExp() {
        // std::vector<Expression*> result;
        // result.resize(returnPropsExp_.size());
        // auto get = [] (auto &ptr) {return ptr.get(); };
        // std::transform(returnPropsExp_.begin(), returnPropsExp_.end(), result.begin(), get);
        // return result;
        return returnPropsExp_;
    }

private:
    std::unique_ptr<RunTimeContext>                                      context_;
    bool                                                                 insertable_{false};

    cpp2::EdgeKey                                                        edgeKey_;

    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>          indexes_;

    std::unique_ptr<StorageExpressionContext>                            expCtx_;

    // update <prop name, new value expression>
    std::vector<storage::cpp2::UpdatedProp>                              updatedProps_;

    // return props expression
    std::vector<Expression*>                                             returnPropsExp_;

    // condition expression
    Expression*                                                          filterExp_{nullptr};

    // updatedProps_ dependent props in value expression
    std::vector<std::pair<std::string, std::unordered_set<std::string>>> depPropMap_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
