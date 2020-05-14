/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_
#define STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "storage/query/QueryBaseProcessor.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/StorageDAG.h"

namespace nebula {
namespace storage {

class GetNeighborsProcessor
    : public QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::GetNeighborsResponse> {

public:
    static GetNeighborsProcessor* instance(StorageEnv* env,
                                           stats::Stats* stats,
                                           VertexCache* cache) {
        return new GetNeighborsProcessor(env, stats, cache);
    }

    void process(const cpp2::GetNeighborsRequest& req) override;

protected:
    GetNeighborsProcessor(StorageEnv* env,
                          stats::Stats* stats,
                          VertexCache* cache)
        : QueryBaseProcessor<cpp2::GetNeighborsRequest,
                             cpp2::GetNeighborsResponse>(env, stats, cache) {}

    StorageDAG buildDAG(PartitionID partId,
                        const VertexID& vId,
                        FilterNode* filter,
                        nebula::Row* row);

    void onProcessFinished() override;

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetNeighborsRequest& req) override;
    cpp2::ErrorCode getSpaceSchema();
    cpp2::ErrorCode buildTagContext(const cpp2::GetNeighborsRequest& req);
    cpp2::ErrorCode buildEdgeContext(const cpp2::GetNeighborsRequest& req);

    // collect tag props need to return
    cpp2::ErrorCode prepareVertexProps(const std::vector<std::string>& vertexProps,
                                       std::vector<ReturnProp>& returnProps);

    // collect edge props need to return
    cpp2::ErrorCode prepareEdgeProps(const std::vector<std::string>& edgeProps,
                                     std::vector<ReturnProp>& returnProps);

    // add PropContext of stat
    cpp2::ErrorCode handleEdgeStatProps(const std::vector<cpp2::StatProp>& statProps);
    cpp2::ErrorCode checkStatType(const meta::cpp2::PropertyType& fType, cpp2::StatType statType);
    PropContext buildPropContextWithStat(const std::string& name,
                                         size_t idx,
                                         const cpp2::StatType& statType,
                                         const meta::SchemaProviderIf::Field* field);

private:
    size_t statCount_ = 0;
    bool scanAllEdges_ = false;
    static constexpr size_t kStatReturnIndex_ = 1;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_
