/* Copyright (c) 2020 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_SCANVERTEXPROCESSOR_H_
#define STORAGE_QUERY_SCANVERTEXPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kScanVertexCounters;

class ScanVertexProcessor
    : public QueryBaseProcessor<cpp2::ScanVertexRequest, cpp2::ScanVertexResponse> {
public:
    static ScanVertexProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kScanVertexCounters,
            folly::Executor* executor = nullptr) {
        return new ScanVertexProcessor(env, counters, executor);
    }

    void process(const cpp2::ScanVertexRequest& req) override;

    void doProcess(const cpp2::ScanVertexRequest& req);

private:
    ScanVertexProcessor(StorageEnv* env,
                        const ProcessorCounters* counters,
                        folly::Executor* executor)
        : QueryBaseProcessor<cpp2::ScanVertexRequest,
                             cpp2::ScanVertexResponse>(env,
                                                       counters,
                                                       executor) {
    }

    nebula::cpp2::ErrorCode
    checkAndBuildContexts(const cpp2::ScanVertexRequest& req) override;

    void buildTagColName(const std::vector<cpp2::VertexProp>& tagProps);

    void onProcessFinished() override;

private:
    PartitionID partId_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_QUERY_SCANVERTEXPROCESSOR_H_
