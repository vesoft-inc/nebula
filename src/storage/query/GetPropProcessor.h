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

extern ProcessorCounters kGetPropCounters;

class GetPropProcessor
    : public QueryBaseProcessor<cpp2::GetPropRequest, cpp2::GetPropResponse> {
public:
    static GetPropProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kGetPropCounters,
            folly::Executor* executor = nullptr) {
        return new GetPropProcessor(env, counters, executor);
    }

    void process(const cpp2::GetPropRequest& req) override;

    void doProcess(const cpp2::GetPropRequest& req);

protected:
    GetPropProcessor(StorageEnv* env,
                     const ProcessorCounters* counters,
                     folly::Executor* executor)
        : QueryBaseProcessor<cpp2::GetPropRequest, cpp2::GetPropResponse>(env,
                                                                          counters,
                                                                          executor) {}

private:
    StoragePlan<VertexID> buildTagPlan(RunTimeContext* context, nebula::DataSet* result);

    StoragePlan<cpp2::EdgeKey> buildEdgePlan(RunTimeContext* context, nebula::DataSet* result);

    void onProcessFinished() override;

    nebula::cpp2::ErrorCode
    checkAndBuildContexts(const cpp2::GetPropRequest& req) override;

    nebula::cpp2::ErrorCode checkRequest(const cpp2::GetPropRequest& req);

    nebula::cpp2::ErrorCode buildTagContext(const cpp2::GetPropRequest& req);

    nebula::cpp2::ErrorCode buildEdgeContext(const cpp2::GetPropRequest& req);

    void buildTagColName(const std::vector<cpp2::VertexProp>& tagProps);

    void buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps);

    void runInSingleThread(const cpp2::GetPropRequest& req);
    void runInMultipleThread(const cpp2::GetPropRequest& req);

    folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> runInExecutor(
        RunTimeContext* context,
        nebula::DataSet* result,
        PartitionID partId,
        const std::vector<nebula::Row>& rows);

private:
    std::vector<RunTimeContext>               contexts_;
    std::vector<nebula::DataSet>              results_;
    bool isEdge_ = false;                     // true for edge, false for tag
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETPROPPROCESSOR_H_
