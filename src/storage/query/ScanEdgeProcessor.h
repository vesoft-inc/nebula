/* Copyright (c) 2020 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_SCANEDGEPROCESSOR_H_
#define STORAGE_QUERY_SCANEDGEPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class ScanEdgeProcessor
    : public QueryBaseProcessor<cpp2::ScanEdgeRequest, cpp2::ScanEdgeResponse> {
public:
    static ScanEdgeProcessor* instance(StorageEnv* env,
                                       stats::Stats* stats) {
        return new ScanEdgeProcessor(env, stats);
    }

    void process(const cpp2::ScanEdgeRequest& req) override;

private:
    ScanEdgeProcessor(StorageEnv* env, stats::Stats* stats)
        : QueryBaseProcessor<cpp2::ScanEdgeRequest, cpp2::ScanEdgeResponse>(env, stats) {
    }

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::ScanEdgeRequest& req) override;

    void onProcessFinished() override;

    bool returnNoProps_{false};
    PartitionID partId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_SCANEDGEPROCESSOR_H_
