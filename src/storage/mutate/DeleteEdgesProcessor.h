/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_
#define STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kDelEdgesCounters;

class DeleteEdgesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static DeleteEdgesProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kDelEdgesCounters) {
        return new DeleteEdgesProcessor(env, counters);
    }

    void process(const cpp2::DeleteEdgesRequest& req);

private:
    explicit DeleteEdgesProcessor(StorageEnv* env, const ProcessorCounters* counters)
            : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

    ErrorOr<nebula::cpp2::ErrorCode, std::string>
    deleteEdges(PartitionID partId, const std::vector<cpp2::EdgeKey>& edges);

private:
    GraphSpaceID                                                spaceId_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_
