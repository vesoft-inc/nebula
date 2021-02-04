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

extern ProcessorCounters kAddVerticesCounters;

class AddVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddVerticesProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kAddVerticesCounters,
            VertexCache* cache = nullptr) {
        return new AddVerticesProcessor(env, counters, cache);
    }

    void process(const cpp2::AddVerticesRequest& req);

private:
    AddVerticesProcessor(StorageEnv* env,
                         const ProcessorCounters* counters,
                         VertexCache* cache)
        : BaseProcessor<cpp2::ExecResponse>(env, counters)
        , vertexCache_(cache) {}

    folly::Optional<std::string>
    addVertices(PartitionID partId,
                const std::vector<kvstore::KV>& vertices);

    folly::Optional<std::string> findOldValue(PartitionID partId, const VertexID& vId, TagID tagId);

    std::string indexKey(PartitionID partId, const VertexID& vId, RowReader* reader,
                         std::shared_ptr<nebula::meta::cpp2::IndexItem> index);

private:
    GraphSpaceID                                                spaceId_;
    VertexCache*                                                vertexCache_{nullptr};
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_
