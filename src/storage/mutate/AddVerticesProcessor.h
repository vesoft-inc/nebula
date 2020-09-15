/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_
#define STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_

#include "common/base/Base.h"
#include "common/base/ConcurrentLRUCache.h"
#include "storage/BaseProcessor.h"
#include "storage/CommonUtils.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

class AddVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddVerticesProcessor* instance(StorageEnv* env,
                                          stats::Stats* stats,
                                          VertexCache* cache = nullptr) {
        return new AddVerticesProcessor(env, stats, cache);
    }

    void process(const cpp2::AddVerticesRequest& req);

private:
    AddVerticesProcessor(StorageEnv* env, stats::Stats* stats, VertexCache* cache)
        : BaseProcessor<cpp2::ExecResponse>(env, stats)
        , vertexCache_(cache) {}

    folly::Optional<std::string>
    addVertices(PartitionID partId,
                const std::vector<kvstore::KV>& vertices);

    folly::Optional<std::string> findObsoleteIndex(PartitionID partId, VertexID vId, TagID tagId);

    std::string indexKey(PartitionID partId, VertexID vId, RowReader* reader,
                         std::shared_ptr<nebula::meta::cpp2::IndexItem> index);

private:
    GraphSpaceID                                                spaceId_;
    VertexCache*                                                vertexCache_{nullptr};
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_
