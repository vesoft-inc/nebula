/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
                                         meta::SchemaManager* schemaMan) {
        return new QueryStatsProcessor(kvstore, schemaMan);
    }

private:
    QueryStatsProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
        : QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryStatsResponse>(kvstore, schemaMan) {}

    kvstore::ResultCode processVertex(PartitionID partID, VertexID vId,
                                      std::vector<TagContext>& tagContexts,
                                      EdgeContext& edgeContext) override;

    void onProcessed(std::vector<TagContext>& tagContexts,
                     EdgeContext& edgeContext,
                     int32_t retNum) override;

    kvstore::ResultCode collectVertexStats(PartitionID partId,
                                           VertexID vId, TagID tagId,
                                           SchemaProviderIf* tagSchema,
                                           std::vector<PropContext>& props);


    kvstore::ResultCode collectEdgesStats(PartitionID partId,
                                          VertexID vId, EdgeType edgeType,
                                          SchemaProviderIf* edgeSchema,
                                          std::vector<PropContext>& props);

    void calcResult(std::vector<PropContext>&& props);

private:
    StatsCollector collector_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYSTATSPROCESSOR_H_
