/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_PREFIXPROCESSOR_H
#define STORAGE_PREFIXPROCESSOR_H

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class PrefixProcessor : public BaseProcessor<cpp2::GeneralResponse> {
public:
    static PrefixProcessor* instance(kvstore::KVStore* kvstore,
                                     meta::SchemaManager* schemaMan,
                                     stats::Stats* stats,
                                     folly::Executor* executor) {
        return new PrefixProcessor(kvstore, schemaMan, stats, executor);
    }

     void process(const cpp2::PrefixRequest& req);

private:
    explicit PrefixProcessor(kvstore::KVStore* kvstore,
                             meta::SchemaManager* schemaMan,
                             stats::Stats* stats,
                             folly::Executor* executor = nullptr)
            : BaseProcessor<cpp2::GeneralResponse>(kvstore, schemaMan, stats),
            executor_(executor) {}

    folly::Future<PartitionCode>
    asyncProcess(PartitionID part, std::string prefix);

private:
    GraphSpaceID  space_;
    folly::Executor *executor_{nullptr};
    std::unordered_map<std::string, std::string> pairs_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_PREFIXPROCESSOR_H

