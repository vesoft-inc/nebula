/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_QUERYVERTEXPROPSPROCESSOR_H_
#define STORAGE_QUERY_QUERYVERTEXPROPSPROCESSOR_H_

#include "base/Base.h"
#include "storage/query/QueryBoundProcessor.h"

namespace nebula {
namespace storage {

class QueryVertexPropsProcessor : public QueryBoundProcessor {
public:
    static QueryVertexPropsProcessor* instance(kvstore::KVStore* kvstore,
                                               meta::SchemaManager* schemaMan,
                                               stats::Stats* stats,
                                               folly::Executor* executor,
                                               VertexCache* cache = nullptr) {
        return new QueryVertexPropsProcessor(kvstore, schemaMan, stats, executor, cache);
    }

    void process(const cpp2::VertexPropRequest& req);

    // Only use in fetch *
    // Check tagId in tagTTLInfo_, if any, get ttl information.
    // If not, add it first, then return ttl information
    folly::Optional<std::pair<std::string, int64_t>>
    getTagTTLInfo(TagID tagId, const meta::SchemaProviderIf* tagschema);

private:
    explicit QueryVertexPropsProcessor(kvstore::KVStore* kvstore,
                                       meta::SchemaManager* schemaMan,
                                       stats::Stats* stats,
                                       folly::Executor* executor,
                                       VertexCache* cache)
        : QueryBoundProcessor(kvstore, schemaMan, stats, executor, cache) {}

    kvstore::ResultCode collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            std::vector<cpp2::TagData> &tds);

    std::unordered_map<TagID, std::pair<std::string, int64_t>> tagTTLInfo_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_QUERYVERTEXPROPSPROCESSOR_H_
