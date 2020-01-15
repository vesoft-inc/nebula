/* Copyright (c) 2019 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_SCANVERTEXPROCESSOR_H_
#define STORAGE_SCANVERTEXPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class ScanVertexProcessor
    : public BaseProcessor<cpp2::ScanVertexResponse> {
public:
    static ScanVertexProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan,
                                         stats::Stats* stats) {
        return new ScanVertexProcessor(kvstore, schemaMan, stats);
    }

    void process(const cpp2::ScanVertexRequest& req);

private:
    explicit ScanVertexProcessor(kvstore::KVStore* kvstore,
                                 meta::SchemaManager* schemaMan,
                                 stats::Stats* stats)
            : BaseProcessor<cpp2::ScanVertexResponse>(kvstore, schemaMan, stats) {}

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::ScanVertexRequest& req);

    std::unordered_map<TagID, std::vector<PropContext>> tagContexts_;
    std::unordered_map<TagID, nebula::cpp2::Schema> tagSchema_;
    bool returnAllColumns_{false};
    GraphSpaceID spaceId_;
    PartitionID partId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_SCANVERTEXPROCESSOR_H_
