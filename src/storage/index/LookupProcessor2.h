/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "common/base/Base.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/BaseProcessor.h"
namespace nebula {
namespace storage {
extern ProcessorCounters kLookupCounters;
using IndexFilterItem =
    std::unordered_map<int32_t, std::pair<std::unique_ptr<StorageExpressionContext>, Expression*>>;

class LookupProcessor : public BaseProcessor<cpp2::LookupIndexResp> {
 public:
  static LookupProcessor* instance(StorageEnv* env,
                                   const ProcessorCounters* counters = &kLookupCounters,
                                   folly::Executor* executor = nullptr);
  void process(const cpp2::LookupIndexRequest& req);

 private:
  LookupProcessor(StorageEnv* env, const ProcessorCounters* counters, folly::Executor* executor);
  void onProcessFinished();

  void runInSingleThread(const cpp2::LookupIndexRequest& req);
  void runInMultipleThread(const cpp2::LookupIndexRequest& req);
  folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> runInExecutor(
      IndexFilterItem* filterItem, nebula::DataSet* result, PartitionID partId);

  void doProcess(const cpp2::LookupIndexRequest& req);
  folly::Executor* executor_{nullptr};
};
}  // namespace storage

}  // namespace nebula
