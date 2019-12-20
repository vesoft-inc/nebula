/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_KV_SCANPROCESSOR_H
#define STORAGE_KV_SCANPROCESSOR_H

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class ScanProcessor : public BaseProcessor<cpp2::GeneralResponse> {
public:
    static ScanProcessor* instance(kvstore::KVStore* kvstore,
                                   meta::SchemaManager* schemaMan,
                                   stats::Stats* stats,
                                   folly::Executor* executor) {
        return new ScanProcessor(kvstore, schemaMan, stats, executor);
    }

     void process(const cpp2::ScanRequest& req);

private:
    explicit ScanProcessor(kvstore::KVStore* kvstore,
                           meta::SchemaManager* schemaMan,
                           stats::Stats* stats,
                           folly::Executor* executor = nullptr)
            : BaseProcessor<cpp2::GeneralResponse>(kvstore, schemaMan, stats),
            executor_(executor) {}

    folly::Future<PartitionCode>
    asyncProcess(PartitionID part, std::string start, std::string end, std::string seek,
                 int32_t limit);

private:
    GraphSpaceID  space_;
    folly::Executor *executor_{nullptr};
    std::unordered_map<std::string, std::string> pairs_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_SCANPROCESSOR_H

