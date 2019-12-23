/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_SCANVERTEXINDEXPROCESSOR_H
#define STORAGE_SCANVERTEXINDEXPROCESSOR_H

#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "ScanIndexBaseProcessor.h"

namespace nebula {
namespace storage {
class ScanVertexIndexProcessor
        : public ScanIndexBaseProcessor<cpp2::IndexScanRequest, cpp2::ScanVertexResponse> {
public:
    static ScanVertexIndexProcessor* instance(kvstore::KVStore* kvstore,
                                              meta::SchemaManager* schemaMan,
                                              stats::Stats* stats,
                                              folly::Executor* executor,
                                              VertexCache* cache = nullptr) {
        return new ScanVertexIndexProcessor(kvstore, schemaMan, stats, executor, cache);
    }

    void process(const cpp2::IndexScanRequest& req);

private:
    explicit ScanVertexIndexProcessor(kvstore::KVStore* kvstore,
                                      meta::SchemaManager* schemaMan,
                                      stats::Stats* stats,
                                      folly::Executor* executor,
                                      VertexCache* cache = nullptr)
            : ScanIndexBaseProcessor<cpp2::IndexScanRequest, cpp2::ScanVertexResponse>
                (kvstore, schemaMan, stats, executor, cache) {}

    folly::Future<std::pair<PartitionID, kvstore::ResultCode>>
    asyncProcess(PartitionID part, IndexID indexId , TagID tagId);

    kvstore::ResultCode processVertices(PartitionID part, IndexID indexId, TagID tagId);
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_SCANVERTEXINDEXPROCESSOR_H

