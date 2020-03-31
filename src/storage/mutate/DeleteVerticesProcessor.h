/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_
#define STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_

#include "base/Base.h"
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class DeleteVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static DeleteVerticesProcessor* instance(kvstore::KVStore* kvstore,
                                             meta::SchemaManager* schemaMan,
                                             meta::IndexManager* indexMan,
                                             stats::Stats* stats,
                                             VertexCache* cache = nullptr) {
        return new DeleteVerticesProcessor(kvstore, schemaMan, indexMan, stats, cache);
    }

    void process(const cpp2::DeleteVerticesRequest& req);

private:
    explicit DeleteVerticesProcessor(kvstore::KVStore* kvstore,
                                     meta::SchemaManager* schemaMan,
                                     meta::IndexManager* indexMan,
                                     stats::Stats* stats,
                                     VertexCache* cache)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats)
            , indexMan_(indexMan)
            , vertexCache_(cache) {}

    folly::Optional<std::string>
    deleteVertices(GraphSpaceID spaceId,
                   PartitionID partId,
                   const std::vector<VertexID>& vertices);

private:
    meta::IndexManager*                                   indexMan_{nullptr};
    VertexCache*                                          vertexCache_{nullptr};
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>> indexes_;
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_
