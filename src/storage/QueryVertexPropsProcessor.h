/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERYVERTEXPROPSPROCESSOR_H_
#define STORAGE_QUERYVERTEXPROPSPROCESSOR_H_

#include "base/Base.h"
#include "storage/QueryBoundProcessor.h"

namespace nebula {
namespace storage {

class QueryVertexPropsProcessor : public QueryBoundProcessor {
public:
    static QueryVertexPropsProcessor* instance(kvstore::KVStore* kvstore,
                                               meta::SchemaManager* schemaMan,
                                               StorageStats* stats,
                                               folly::Executor* executor) {
        return new QueryVertexPropsProcessor(kvstore, schemaMan, stats, executor);
    }

    void process(const cpp2::VertexPropRequest& req);

private:
    explicit QueryVertexPropsProcessor(kvstore::KVStore* kvstore,
                                       meta::SchemaManager* schemaMan,
                                       StorageStats* stats,
                                       folly::Executor* executor)
        : QueryBoundProcessor(kvstore, schemaMan, stats, executor) {}
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYVERTEXPROPSPROCESSOR_H_
