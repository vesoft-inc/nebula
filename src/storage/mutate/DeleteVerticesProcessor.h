/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_
#define STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_

#include "common/base/Base.h"
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

class DeleteVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static DeleteVerticesProcessor* instance(StorageEnv* env,
                                             stats::Stats* stats,
                                             VertexCache* cache = nullptr) {
        return new DeleteVerticesProcessor(env, stats, cache);
    }

    void process(const cpp2::DeleteVerticesRequest& req);

private:
    DeleteVerticesProcessor(StorageEnv* env, stats::Stats* stats, VertexCache* cache)
        : BaseProcessor<cpp2::ExecResponse>(env, stats)
        , vertexCache_(cache) {}

    folly::Optional<std::string>
    deleteVertices(PartitionID partId,
                   const std::vector<Value>& vertices);

private:
    GraphSpaceID                                                spaceId_;
    VertexCache*                                                vertexCache_{nullptr};
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_
