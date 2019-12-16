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

namespace nebula {
namespace storage {

class AddVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddVerticesProcessor* instance(kvstore::KVStore* kvstore,
                                          meta::SchemaManager* schemaMan,
                                          meta::IndexManager* indexMan,
                                          stats::Stats* stats,
                                          VertexCache* cache = nullptr) {
        return new AddVerticesProcessor(kvstore, schemaMan, indexMan, stats, cache);
    }

    void process(const cpp2::AddVerticesRequest& req);

private:
    explicit AddVerticesProcessor(kvstore::KVStore* kvstore,
                                  meta::SchemaManager* schemaMan,
                                  meta::IndexManager* indexMan,
                                  stats::Stats* stats,
                                  VertexCache* cache)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats)
            , indexMan_(indexMan)
            , vertexCache_(cache) {}

private:
    meta::IndexManager* indexMan_{nullptr};
    VertexCache* vertexCache_{nullptr};
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_
