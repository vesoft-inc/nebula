/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_LOOKUPINDEXPROCESSOR_H
#define STORAGE_LOOKUPINDEXPROCESSOR_H

#include "IndexExecutor.h"

namespace nebula {
namespace storage {

class LookUpIndexProcessor: public IndexExecutor<cpp2::LookUpIndexResp> {
public:
    static LookUpIndexProcessor* instance(kvstore::KVStore* kvstore,
                                          meta::SchemaManager* schemaMan,
                                          meta::IndexManager* indexMan,
                                          stats::Stats* stats,
                                          folly::Executor* executor,
                                          VertexCache* cache = nullptr) {
        return new LookUpIndexProcessor(kvstore, schemaMan, indexMan, stats, executor, cache);
    }

    void process(const cpp2::LookUpIndexRequest& req);

private:
    explicit LookUpIndexProcessor(kvstore::KVStore* kvstore,
                                  meta::SchemaManager* schemaMan,
                                  meta::IndexManager* indexMan,
                                  stats::Stats* stats,
                                  folly::Executor* executor,
                                  VertexCache* cache = nullptr)
        : IndexExecutor<cpp2::LookUpIndexResp>(kvstore, schemaMan,
                                               indexMan, stats, cache, executor) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_LOOKUPINDEXPROCESSOR_H
