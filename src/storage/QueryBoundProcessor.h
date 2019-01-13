/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_QUERYBOUNDPROCESSOR_H_
#define STORAGE_QUERYBOUNDPROCESSOR_H_

#include "base/Base.h"
#include "storage/QueryBaseProcessor.h"

namespace nebula {
namespace storage {


class QueryBoundProcessor
    : public QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryResponse> {
public:
    static QueryBoundProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan) {
        return new QueryBoundProcessor(kvstore, schemaMan);
    }

protected:
    QueryBoundProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
        : QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryResponse>(kvstore, schemaMan) {}

    kvstore::ResultCode processVertex(PartitionID partID, VertexID vId,
                                      std::vector<TagContext>& tagContexts,
                                      EdgeContext& edgeContext) override;

    void onProcessed(std::vector<TagContext>& tagContexts,
                     EdgeContext& edgeContext,
                     int32_t retNum) override;

    kvstore::ResultCode collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            SchemaProviderIf* tagSchema,
                            std::vector<PropContext>& props,
                            RowWriter& writer);

    kvstore::ResultCode collectEdgeProps(
                                       PartitionID partId,
                                       VertexID vId,
                                       EdgeType edgeType,
                                       SchemaProviderIf* schema,
                                       std::vector<PropContext>& props,
                                       RowSetWriter& writer);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYBOUNDPROCESSOR_H_
