/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_SCANEDGEINDEXPROCESSOR_H
#define STORAGE_SCANEDGEINDEXPROCESSOR_H

#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "LookUpIndexBaseProcessor.h"

namespace nebula {
namespace storage {
class LookUpEdgeIndexProcessor
    : public LookUpIndexBaseProcessor<cpp2::LookUpIndexRequest, cpp2::LookUpEdgeIndexResp> {
public:
    static LookUpEdgeIndexProcessor* instance(kvstore::KVStore* kvstore,
                                            meta::SchemaManager* schemaMan,
                                            stats::Stats* stats,
                                            folly::Executor* executor,
                                            VertexCache* cache = nullptr) {
        return new LookUpEdgeIndexProcessor(kvstore, schemaMan, stats, executor, cache);
    }

    void process(const cpp2::LookUpIndexRequest& req);

private:
    explicit LookUpEdgeIndexProcessor(kvstore::KVStore* kvstore,
                                    meta::SchemaManager* schemaMan,
                                    stats::Stats* stats,
                                    folly::Executor* executor,
                                    VertexCache* cache = nullptr)
            : LookUpIndexBaseProcessor<cpp2::LookUpIndexRequest, cpp2::LookUpEdgeIndexResp>
                (kvstore, schemaMan, stats, executor, cache) {}
};
}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_SCANEDGEINDEXPROCESSOR_H

