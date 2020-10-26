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
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class AddEdgesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddEdgesProcessor* instance(kvstore::KVStore* kvstore,
                                       meta::SchemaManager* schemaMan,
                                       meta::IndexManager* indexMan,
                                       stats::Stats* stats) {
        return new AddEdgesProcessor(kvstore, schemaMan, indexMan, stats);
    }

    void process(const cpp2::AddEdgesRequest& req);

private:
    explicit AddEdgesProcessor(kvstore::KVStore* kvstore,
                               meta::SchemaManager* schemaMan,
                               meta::IndexManager* indexMan,
                               stats::Stats* stats)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats)
            , indexMan_(indexMan) {}

    std::string addEdges(int64_t version, PartitionID partId,
                         const std::vector<cpp2::Edge>& edges);

    std::string findObsoleteIndex(PartitionID partId,
                                  const folly::StringPiece& rawKey);

    std::string indexKey(PartitionID partId,
                         RowReader* reader,
                         const folly::StringPiece& rawKey,
                         std::shared_ptr<nebula::cpp2::IndexItem> index);

private:
    GraphSpaceID                                          spaceId_;
    meta::IndexManager*                                   indexMan_{nullptr};
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> indexes_;
    bool                                                  ignoreExistedIndex_{false};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
