/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once
#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/BaseProcessor.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
extern ProcessorCounters kLookupCounters;

class LookupProcessor : public BaseProcessor<cpp2::LookupIndexResp> {
 public:
  static LookupProcessor* instance(StorageEnv* env,
                                   const ProcessorCounters* counters = &kLookupCounters,
                                   folly::Executor* executor = nullptr) {
    return new LookupProcessor(env, counters, executor);
  }
  void process(const cpp2::LookupIndexRequest& req);

 private:
  LookupProcessor(StorageEnv* env, const ProcessorCounters* counters, folly::Executor* executor)
      : BaseProcessor<cpp2::LookupIndexResp>(env, counters), executor_(executor) {}
  void doProcess(const cpp2::LookupIndexRequest& req);
  void onProcessFinished() {
    BaseProcessor<cpp2::LookupIndexResp>::resp_.data_ref() = std::move(resultDataSet_);
  }
  void profilePlan(IndexNode* plan);
  void runInSingleThread(const std::vector<PartitionID>& parts, std::unique_ptr<IndexNode> plan);
  void runInMultipleThread(const std::vector<PartitionID>& parts, std::unique_ptr<IndexNode> plan);
  ::nebula::cpp2::ErrorCode prepare(const cpp2::LookupIndexRequest& req);
  std::unique_ptr<IndexNode> buildPlan(const cpp2::LookupIndexRequest& req);
  std::unique_ptr<IndexNode> buildOneContext(const cpp2::IndexQueryContext& ctx);
  std::vector<std::unique_ptr<IndexNode>> reproducePlan(IndexNode* root, size_t count);
  folly::Executor* executor_{nullptr};
  std::unique_ptr<PlanContext> planContext_;
  std::unique_ptr<RuntimeContext> context_;
  nebula::DataSet resultDataSet_;
  std::vector<nebula::DataSet> partResults_;
};
}  // namespace storage
}  // namespace nebula
