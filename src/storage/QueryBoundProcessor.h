/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_QUERYBOUNDPROCESSOR_H_
#define STORAGE_QUERYBOUNDPROCESSOR_H_

#include "base/Base.h"
#include <folly/SpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "kvstore/include/KVStore.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/BaseProcessor.h"
#include "meta/SchemaManager.h"
#include "dataman/RowSetWriter.h"

namespace nebula {
namespace storage {

class QueryBoundProcessor : public BaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryResponse> {
public:
    static QueryBoundProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan) {
        return new QueryBoundProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::GetNeighborsRequest& req) override;

private:
    QueryBoundProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
        : BaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryResponse>(kvstore)
        , schemaMan_(schemaMan) {}

    cpp2::ColumnDef columnDef(std::string name, cpp2::SupportedType type);

    /**
     * Scan props passed-in, and generate all Schema objects current request needed
     * */
    void prepareSchemata(const std::vector<cpp2::PropDef>& props,
                         EdgeType edgeType,
                         SchemaProviderIf*& edgeSchema,
                         std::vector<std::pair<TagID, SchemaProviderIf*>>& tagSchemata,
                         cpp2::Schema& respEdge,
                         cpp2::Schema& respTag,
                         int32_t& schemaVer);
    /**
     * It will parse inputRow as inputSchema, and output data into rowWriter.
     * */
    void output(SchemaProviderIf* inputSchema,
                folly::StringPiece inputRow,
                int32_t ver,
                SchemaProviderIf* outputSchema,
                RowWriter& writer);

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
