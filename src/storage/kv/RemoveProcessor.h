/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_KV_REMOVEPROCESSOR_H_
#define STORAGE_KV_REMOVEPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class RemoveProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static RemoveProcessor* instance(kvstore::KVStore* kvstore,
                                     meta::SchemaManager* schemaMan,
                                     stats::Stats* stats,
                                     folly::Executor* executor) {
        return new RemoveProcessor(kvstore, schemaMan, stats, executor);
    }

    void process(const cpp2::RemoveRequest& req);

private:
    explicit RemoveProcessor(kvstore::KVStore* kvstore,
                             meta::SchemaManager* schemaMan,
                             stats::Stats* stats,
                             folly::Executor* executor = nullptr)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats),
            executor_(executor) {}

    folly::Future<PartitionCode>
    asyncProcess(PartitionID partId, std::vector<std::string> keys);

private:
    GraphSpaceID  space_;
    folly::Executor  *executor_{nullptr};
    meta::MetaClient *client_{nullptr};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_REMOVEPROCESSOR_H_
