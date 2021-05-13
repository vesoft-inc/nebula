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

extern ProcessorCounters kScanEdgeCounters;

class ScanEdgeProcessor
    : public QueryBaseProcessor<cpp2::ScanEdgeRequest, cpp2::ScanEdgeResponse> {
public:
    static ScanEdgeProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kScanEdgeCounters,
            folly::Executor* executor = nullptr) {
        return new ScanEdgeProcessor(env, counters, executor);
    }

    void process(const cpp2::ScanEdgeRequest& req) override;

    void doProcess(const cpp2::ScanEdgeRequest& req);

private:
    ScanEdgeProcessor(StorageEnv* env,
                      const ProcessorCounters* counters,
                      folly::Executor* executor)
        : QueryBaseProcessor<cpp2::ScanEdgeRequest,
                             cpp2::ScanEdgeResponse>(env,
                                                     counters,
                                                     executor) {
    }

    nebula::cpp2::ErrorCode
    checkAndBuildContexts(const cpp2::ScanEdgeRequest& req) override;

    void buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps);

    void onProcessFinished() override;

    PartitionID partId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_SCANEDGEPROCESSOR_H_
