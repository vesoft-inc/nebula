/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_QUERYBOUNDPROCESSOR_H_
#define STORAGE_QUERY_QUERYBOUNDPROCESSOR_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {


class QueryBoundProcessor
    : public QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryResponse> {
    FRIEND_TEST(QueryBoundTest,  GenBucketsTest);

public:
    static QueryBoundProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan,
                                         stats::Stats* stats,
                                         folly::Executor* executor,
                                         VertexCache* cache = nullptr) {
        return new QueryBoundProcessor(kvstore, schemaMan, stats, executor, cache);
    }

protected:
    explicit QueryBoundProcessor(kvstore::KVStore* kvstore,
                                 meta::SchemaManager* schemaMan,
                                 stats::Stats* stats,
                                 folly::Executor* executor,
                                 VertexCache* cache)
        : QueryBaseProcessor<cpp2::GetNeighborsRequest,
                             cpp2::QueryResponse>(kvstore, schemaMan, stats, executor, cache) {}

    kvstore::ResultCode processVertex(PartitionID partId, VertexID vId) override;

    void onProcessFinished(int32_t retNum) override;

private:
    std::vector<cpp2::VertexData> vertices_;

    kvstore::ResultCode processEdge(PartitionID partId, VertexID vId, FilterContext &fcontext,
                                    cpp2::VertexData& vdata);
    kvstore::ResultCode processEdgeImpl(const PartitionID partId, const VertexID vId,
                                        const EdgeType edgeType,
                                        const std::vector<PropContext>& props,
                                        FilterContext& fcontext, cpp2::VertexData& vdata);

protected:
    // Indicate the request only get vertex props.
    bool onlyVertexProps_ = false;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_QUERYBOUNDPROCESSOR_H_
