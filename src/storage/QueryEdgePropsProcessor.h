/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_QUERYEDGEPROPSROCESSOR_H_
#define STORAGE_QUERYEDGEPROPSROCESSOR_H_

#include "storage/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class QueryEdgePropsProcessor
    : public QueryBaseProcessor<cpp2::EdgePropRequest, cpp2::EdgePropResponse> {
public:
    static QueryEdgePropsProcessor* instance(kvstore::KVStore* kvstore,
                                             meta::SchemaManager* schemaMan) {
        return new QueryEdgePropsProcessor(kvstore, schemaMan);
    }

    // It is one new method for QueryBaseProcessor.process.
    void process(const cpp2::EdgePropRequest& req);

private:
    QueryEdgePropsProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
        : QueryBaseProcessor<cpp2::EdgePropRequest, cpp2::EdgePropResponse>(kvstore, schemaMan) {}

    kvstore::ResultCode collectEdgesProps(PartitionID partId,
                                          const cpp2::EdgeKey& edgeKey,
                                          SchemaProviderIf* edgeSchema,
                                          std::vector<PropContext>& props,
                                          RowSetWriter& rsWriter);

    kvstore::ResultCode processVertex(PartitionID partID, VertexID vId,
                                      std::vector<TagContext>& tagContexts,
                                      EdgeContext& edgeContext) {
        UNUSED(partID);
        UNUSED(vId);
        UNUSED(tagContexts);
        UNUSED(edgeContext);
        LOG(FATAL) << "Unimplement!";
        return kvstore::ResultCode::SUCCESSED;
    }

    void onProcessed(std::vector<TagContext>& tagContexts,
                     EdgeContext& edgeContext, int32_t retNum) {
        UNUSED(tagContexts);
        UNUSED(edgeContext);
        UNUSED(retNum);
        LOG(FATAL) << "Unimplement!";
    }
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYEDGEPROPSROCESSOR_H_
