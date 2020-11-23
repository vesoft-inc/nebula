/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
#define STORAGE_MUTATE_ADDEDGESPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

class AddEdgesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddEdgesProcessor* instance(StorageEnv* env,
                                       stats::Stats* stats) {
        return new AddEdgesProcessor(env, stats);
    }

    void process(const cpp2::AddEdgesRequest& req);

private:
    AddEdgesProcessor(StorageEnv* env, stats::Stats* stats)
        : BaseProcessor<cpp2::ExecResponse>(env, stats) {}

    folly::Optional<std::string> addEdges(PartitionID partId,
                                          const std::vector<kvstore::KV>& edges);

    folly::Optional<std::string> findOldValue(PartitionID partId,
                                              const folly::StringPiece& rawKey);

    std::string indexKey(PartitionID partId,
                         RowReader* reader,
                         const folly::StringPiece& rawKey,
                         std::shared_ptr<nebula::meta::cpp2::IndexItem> index);

private:
    GraphSpaceID                                                spaceId_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
