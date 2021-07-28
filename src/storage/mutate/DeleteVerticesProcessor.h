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

extern ProcessorCounters kDelVerticesCounters;

class DeleteVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static DeleteVerticesProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kDelVerticesCounters) {
        return new DeleteVerticesProcessor(env, counters);
    }

    void process(const cpp2::DeleteVerticesRequest& req);

private:
    DeleteVerticesProcessor(StorageEnv* env,
                            const ProcessorCounters* counters)
        : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

    ErrorOr<nebula::cpp2::ErrorCode, std::string>
    deleteVertices(PartitionID partId,
                   const std::vector<Value>& vertices,
                   std::vector<VMLI>& target);

private:
    GraphSpaceID                                                spaceId_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
};


}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_
