/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_LOOKUPVERTEXINDEXPROCESSOR_H
#define STORAGE_LOOKUPVERTEXINDEXPROCESSOR_H

#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "IndexExecutor.h"

namespace nebula {
namespace storage {
class LookUpVertexIndexProcessor:
        public IndexExecutor<cpp2::LookUpVertexIndexResp> {
public:
    static LookUpVertexIndexProcessor* instance(kvstore::KVStore* kvstore,
                                                meta::SchemaManager* schemaMan,
                                                stats::Stats* stats,
                                                VertexCache* cache = nullptr) {
        return new LookUpVertexIndexProcessor(kvstore, schemaMan, stats, cache);
    }

    void process(const cpp2::LookUpIndexRequest& req);

private:
    explicit LookUpVertexIndexProcessor(kvstore::KVStore* kvstore,
                                        meta::SchemaManager* schemaMan,
                                        stats::Stats* stats,
                                        VertexCache* cache = nullptr)
        : IndexExecutor<cpp2::LookUpVertexIndexResp>
                (kvstore, schemaMan, stats, cache) {}
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_LOOKUPVERTEXINDEXPROCESSOR_H

