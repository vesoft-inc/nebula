/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_REMOVERANGEPROCESSOR_H_
#define STORAGE_REMOVERANGEPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class RemoveRangeProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static RemoveRangeProcessor* instance(kvstore::KVStore* kvstore,
                                          meta::SchemaManager* schemaMan,
                                          stats::Stats* stats,
                                          folly::Executor* executor) {
        return new RemoveRangeProcessor(kvstore, schemaMan, stats, executor);
    }

    void process(const cpp2::RemoveRangeRequest& req);

private:
    explicit RemoveRangeProcessor(kvstore::KVStore* kvstore,
                                  meta::SchemaManager* schemaMan,
                                  stats::Stats* stats,
                                  folly::Executor* executor = nullptr)
            : BaseProcessor<cpp2::ExecResponse>(kvstore, schemaMan, stats),
            executor_(executor) {}

    folly::Future<PartitionCode>
    asyncProcess(PartitionID partId, std::string start, std::string end);

private:
    folly::Executor *executor_ = nullptr;
    GraphSpaceID  space_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_REMOVERANGEPROCESSOR_H_
