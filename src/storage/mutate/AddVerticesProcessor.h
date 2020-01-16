/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_
#define STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_

#include "base/Base.h"
#include "base/ConcurrentLRUCache.h"
#include "storage/BaseProcessor.h"
#include "storage/CommonUtils.h"
#include "kvstore/LogEncoder.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class AddVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddVerticesProcessor* instance(kvstore::KVStore* kvstore,
                                          meta::SchemaManager* schemaMan,
                                          stats::Stats* stats,
                                          VertexCache* cache = nullptr) {
        return new AddVerticesProcessor(kvstore, schemaMan, stats, cache);
    }

    void process(const cpp2::AddVerticesRequest& req);

private:
    explicit AddVerticesProcessor(kvstore::KVStore* kvstore,
                                  meta::SchemaManager* schemaMan,
                                  stats::Stats* stats,
                                  VertexCache* cache)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats)
            , vertexCache_(cache) {}

    std::string addVertices(int64_t version, PartitionID partId,
                            const std::vector<cpp2::Vertex>& vertices);

    std::string findObsoleteIndex(PartitionID partId,
                                  VertexID vId,
                                  TagID tagId);

    std::string indexKey(PartitionID partId,
                         VertexID vId,
                         RowReader* reader,
                         const nebula::cpp2::IndexItem& index);

private:
    GraphSpaceID  spaceId_;
    VertexCache* vertexCache_ = nullptr;
    std::vector<nebula::cpp2::IndexItem> indexes_;
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_
