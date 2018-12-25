/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_QUERYEDGEPROPSROCESSOR_H_
#define STORAGE_QUERYEDGEPROPSROCESSOR_H_

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

class QueryEdgePropsProcessor
    : public BaseProcessor<cpp2::EdgePropRequest, cpp2::EdgePropResponse> {
public:
    static QueryEdgePropsProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan) {
        return new QueryEdgePropsProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::EdgePropRequest& req) override;

private:
    QueryEdgePropsProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
        : BaseProcessor<cpp2::EdgePropRequest, cpp2::EdgePropResponse>(kvstore)
        , schemaMan_(schemaMan) {}

    void prepareSchemata(const std::vector<cpp2::PropDef>& props,
                         EdgeType edgeType,
                         SchemaProviderIf*& edgeSchema,
                         cpp2::Schema& respEdge,
                         int32_t& schemaVer);

    kvstore::ResultCode collectEdgesProps(PartitionID partId,
                                          const cpp2::EdgeKey& edgeKey,
                                          SchemaProviderIf* edgeSchema,
                                          SchemaProviderIf* respEdgeSchema,
                                          RowSetWriter& rsWriter);

private:
    meta::SchemaManager* schemaMan_ = nullptr;
    GraphSpaceID  spaceId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYEDGEPROPSROCESSOR_H_
