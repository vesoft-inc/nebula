/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_SCANEDGEINDEXPROCESSOR_H
#define STORAGE_SCANEDGEINDEXPROCESSOR_H

#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "ScanIndexBaseProcessor.h"

namespace nebula {
namespace storage {
class ScanEdgeIndexProcessor
        : public ScanIndexBaseProcessor<cpp2::IndexScanRequest, cpp2::ScanEdgeResponse> {
public:
    static ScanEdgeIndexProcessor* instance(kvstore::KVStore* kvstore,
                                            meta::SchemaManager* schemaMan,
                                            stats::Stats* stats,
                                            folly::Executor* executor,
                                            VertexCache* cache = nullptr) {
        return new ScanEdgeIndexProcessor(kvstore, schemaMan, stats, executor, cache);
    }

    void process(const cpp2::IndexScanRequest& req);

private:
    explicit ScanEdgeIndexProcessor(kvstore::KVStore* kvstore,
                                    meta::SchemaManager* schemaMan,
                                    stats::Stats* stats,
                                    folly::Executor* executor,
                                    VertexCache* cache = nullptr)
            : ScanIndexBaseProcessor<cpp2::IndexScanRequest, cpp2::ScanEdgeResponse>
                (kvstore, schemaMan, stats, executor, cache) {}

    folly::Future<std::pair<PartitionID, kvstore::ResultCode>>
    asyncProcess(PartitionID part, IndexID indexId , EdgeType edgeType);

    kvstore::ResultCode processEdges(PartitionID part, IndexID indexId, EdgeType edgeType);
};
}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_SCANEDGEINDEXPROCESSOR_H

