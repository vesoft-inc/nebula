/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_QUERYEDGEPROPSROCESSOR_H_
#define STORAGE_QUERY_QUERYEDGEPROPSROCESSOR_H_

#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class QueryEdgePropsProcessor
    : public QueryBaseProcessor<cpp2::EdgePropRequest, cpp2::EdgePropResponse> {
public:
    static QueryEdgePropsProcessor* instance(kvstore::KVStore* kvstore,
                                             meta::SchemaManager* schemaMan,
                                             stats::Stats* stats,
                                             folly::Executor* executor = nullptr) {
        return new QueryEdgePropsProcessor(kvstore, schemaMan, stats, executor);
    }

    // It is one new method for QueryBaseProcessor.process.
    void process(const cpp2::EdgePropRequest& req);

    void doProcess(const cpp2::EdgePropRequest& req);

private:
    explicit QueryEdgePropsProcessor(kvstore::KVStore* kvstore,
                                     meta::SchemaManager* schemaMan,
                                     stats::Stats* stats,
                                     folly::Executor* executor)
        : QueryBaseProcessor<cpp2::EdgePropRequest,
                             cpp2::EdgePropResponse>(kvstore, schemaMan, stats, executor) {}

    kvstore::ResultCode collectEdgesProps(PartitionID partId,
                                          const cpp2::EdgeKey& edgeKey,
                                          std::vector<PropContext>& props,
                                          RowSetWriter& rsWriter);

    kvstore::ResultCode processVertex(BucketIdx, PartitionID, VertexID) override {
        LOG(FATAL) << "Unimplement!";
        return kvstore::ResultCode::SUCCEEDED;
    }

    void onProcessFinished(int32_t retNum) override {
        UNUSED(retNum);
        LOG(FATAL) << "Unimplement!";
    }
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_QUERYEDGEPROPSROCESSOR_H_
