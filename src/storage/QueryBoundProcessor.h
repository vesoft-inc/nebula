/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_QUERYBOUNDPROCESSOR_H_
#define STORAGE_QUERYBOUNDPROCESSOR_H_

#include "base/Base.h"
#include "kvstore/include/KVStore.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class QueryBoundProcessor : public BaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryResponse> {
public:
    static QueryBoundProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan) {
        return new QueryBoundProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::GetNeighborsRequest& req) override;

protected:
    QueryBoundProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
        : BaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryResponse>(kvstore)
        , schemaMan_(schemaMan) {}

    /**
     * Scan props passed-in, and generate all Schema objects current request needed
     * */
    void prepareSchemata(const cpp2::GetNeighborsRequest& req,
                         SchemaProviderIf*& edgeSchema,
                         std::vector<std::pair<TagID, SchemaProviderIf*>>& tagSchemata,
                         cpp2::Schema& respEdge,
                         cpp2::Schema& respTag,
                         int32_t& schemaVer);

    /**
     * Collect vertex props as respTagSchema
     * */
    kvstore::ResultCode collectVertexProps(
                       PartitionID partId, VertexID vId,
                       std::vector<std::pair<TagID, SchemaProviderIf*>> tagSchemata,
                       SchemaProviderIf* respTagSchema,
                       cpp2::VertexResponse& vResp);

    /**
     * Collect edges props as respEdgeSchema.
     * */
    kvstore::ResultCode collectEdgesProps(PartitionID partId,
                                          VertexID vId,
                                          EdgeType edgeType,
                                          SchemaProviderIf* edgeSchema,
                                          SchemaProviderIf* respEdgeSchema,
                                          cpp2::VertexResponse& vResp);


private:
    meta::SchemaManager* schemaMan_ = nullptr;
    GraphSpaceID  spaceId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYBOUNDPROCESSOR_H_
