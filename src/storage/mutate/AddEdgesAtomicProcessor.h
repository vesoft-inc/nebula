/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_ADDEDGESATOMICPROCESSOR_H_
#define STORAGE_MUTATE_ADDEDGESATOMICPROCESSOR_H_

#include "common/base/Base.h"
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kAddEdgesAtomicCounters;

class AddEdgesAtomicProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddEdgesAtomicProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kAddEdgesAtomicCounters) {
        return new AddEdgesAtomicProcessor(env, counters);
    }

    void process(const cpp2::AddEdgesRequest& req);

    void processByChain(const cpp2::AddEdgesRequest& req);

private:
    AddEdgesAtomicProcessor(StorageEnv* env, const ProcessorCounters* counters)
        : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

    nebula::cpp2::ErrorCode
    encodeSingleEdgeProps(const cpp2::NewEdge& e, std::string& encodedVal);

    GraphSpaceID                                                spaceId_;
    int64_t                                                     vIdLen_;
    std::vector<std::string>                                    propNames_;
    std::unique_ptr<AddEdgesProcessor>                          processor_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
};

}   // namespace storage
}   // namespace nebula
#endif   // STORAGE_MUTATE_ADDEDGESATOMICPROCESSOR_H_
