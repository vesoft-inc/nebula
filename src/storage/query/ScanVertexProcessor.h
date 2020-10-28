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

class ScanVertexProcessor
    : public QueryBaseProcessor<cpp2::ScanVertexRequest, cpp2::ScanVertexResponse> {
public:
    static ScanVertexProcessor* instance(StorageEnv* env,
                                         stats::Stats* stats) {
        return new ScanVertexProcessor(env, stats);
    }

    void process(const cpp2::ScanVertexRequest& req) override;

private:
    ScanVertexProcessor(StorageEnv* env, stats::Stats* stats)
        : QueryBaseProcessor<cpp2::ScanVertexRequest, cpp2::ScanVertexResponse>(env, stats) {
    }

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::ScanVertexRequest& req) override;

    void onProcessFinished() override;

    bool returnNoProps_{false};
    PartitionID partId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_SCANVERTEXPROCESSOR_H_
