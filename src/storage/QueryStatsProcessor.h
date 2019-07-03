/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERYSTATSPROCESSOR_H_
#define STORAGE_QUERYSTATSPROCESSOR_H_

#include "base/Base.h"
#include "storage/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class QueryStatsProcessor
    : public QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryStatsResponse> {
public:
    static QueryStatsProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan,
                                         folly::Executor* executor,
                                         BoundType type = BoundType::OUT_BOUND) {
        return new QueryStatsProcessor(kvstore, schemaMan, executor, type);
    }

private:
    explicit QueryStatsProcessor(kvstore::KVStore* kvstore,
                                 meta::SchemaManager* schemaMan,
                                 folly::Executor* executor,
                                 BoundType type)
        : QueryBaseProcessor<cpp2::GetNeighborsRequest,
                             cpp2::QueryStatsResponse>(kvstore, schemaMan, executor, type) {}

    kvstore::ResultCode processVertex(PartitionID partID, VertexID vId) override;

    void onProcessFinished(int32_t retNum) override;

    void calcResult(std::vector<PropContext>&& props);

private:
    StatsCollector collector_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYSTATSPROCESSOR_H_
