/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_DELETEVERTEXPROCESSOR_H_
#define STORAGE_MUTATE_DELETEVERTEXPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class DeleteVertexProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static DeleteVertexProcessor* instance(kvstore::KVStore* kvstore,
                                           meta::SchemaManager* schemaMan,
                                           stats::Stats* stats,
                                           VertexCache* cache = nullptr) {
        return new DeleteVertexProcessor(kvstore, schemaMan, stats, cache);
    }

    void process(const cpp2::DeleteVertexRequest& req);

private:
    explicit DeleteVertexProcessor(kvstore::KVStore* kvstore,
                                   meta::SchemaManager* schemaMan,
                                   stats::Stats* stats,
                                   VertexCache* cache)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats)
            , vertexCache_(cache) {}

private:
    VertexCache* vertexCache_ = nullptr;
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETEVERTEXPROCESSOR_H_
