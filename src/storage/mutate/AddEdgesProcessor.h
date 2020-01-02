/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
#define STORAGE_MUTATE_ADDEDGESPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

class AddEdgesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddEdgesProcessor* instance(kvstore::KVStore* kvstore,
                                       meta::SchemaManager* schemaMan,
                                       stats::Stats* stats) {
        return new AddEdgesProcessor(kvstore, schemaMan, stats);
    }

    void process(const cpp2::AddEdgesRequest& req);

private:
    explicit AddEdgesProcessor(kvstore::KVStore* kvstore,
                               meta::SchemaManager* schemaMan,
                               stats::Stats* stats)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats) {}

    std::string addEdges(int64_t version, PartitionID partId,
                         const std::vector<cpp2::Edge>& edges);

    std::string obsoleteIndex(PartitionID partId,
                              const folly::StringPiece& rawKey,
                              const nebula::cpp2::IndexItem& index);

    std::string newIndex(PartitionID partId,
                         const folly::StringPiece& rawKey,
                         const folly::StringPiece& prop,
                         const nebula::cpp2::IndexItem& index);

private:
    GraphSpaceID  spaceId_;
    std::vector<nebula::cpp2::IndexItem> indexes_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
