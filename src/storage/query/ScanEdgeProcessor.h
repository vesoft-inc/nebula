/* Copyright (c) 2019 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_SCANEDGEPROCESSOR_H_
#define STORAGE_SCANEDGEPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class ScanEdgeProcessor
    : public BaseProcessor<cpp2::ScanEdgeResponse> {
public:
    static ScanEdgeProcessor* instance(kvstore::KVStore* kvstore,
                                       meta::SchemaManager* schemaMan,
                                       stats::Stats* stats) {
        return new ScanEdgeProcessor(kvstore, schemaMan, stats);
    }

    void process(const cpp2::ScanEdgeRequest& req);

private:
    explicit ScanEdgeProcessor(kvstore::KVStore* kvstore,
                               meta::SchemaManager* schemaMan,
                               stats::Stats* stats)
            : BaseProcessor<cpp2::ScanEdgeResponse>(kvstore, schemaMan, stats) {}

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::ScanEdgeRequest& req);

    std::unordered_map<EdgeType, std::vector<PropContext>> edgeContexts_;
    std::unordered_map<EdgeType, nebula::cpp2::Schema> edgeSchema_;
    bool returnAllColumns_{false};
    GraphSpaceID spaceId_;
    PartitionID partId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_SCANEDGEPROCESSOR_H_
