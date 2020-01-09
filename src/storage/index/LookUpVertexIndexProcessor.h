/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_SCANVERTEXINDEXPROCESSOR_H
#define STORAGE_SCANVERTEXINDEXPROCESSOR_H

#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "LookUpIndexBaseProcessor.h"

namespace nebula {
namespace storage {
class LookUpVertexIndexProcessor
    : public LookUpIndexBaseProcessor<cpp2::LookUpIndexRequest, cpp2::LookUpVertexIndexResp> {
public:
    static LookUpVertexIndexProcessor* instance(kvstore::KVStore* kvstore,
                                              meta::SchemaManager* schemaMan,
                                              stats::Stats* stats,
                                              folly::Executor* executor,
                                              VertexCache* cache = nullptr) {
        return new LookUpVertexIndexProcessor(kvstore, schemaMan, stats, executor, cache);
    }

    void process(const cpp2::LookUpIndexRequest& req);

private:
    explicit LookUpVertexIndexProcessor(kvstore::KVStore* kvstore,
                                      meta::SchemaManager* schemaMan,
                                      stats::Stats* stats,
                                      folly::Executor* executor,
                                      VertexCache* cache = nullptr)
            : LookUpIndexBaseProcessor<cpp2::LookUpIndexRequest, cpp2::LookUpVertexIndexResp>
                (kvstore, schemaMan, stats, executor, cache) {}
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_SCANVERTEXINDEXPROCESSOR_H

