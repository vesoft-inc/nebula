/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_INDEX_LOOKUPANNPROCESSOR_H
#define STORAGE_INDEX_LOOKUPANNPROCESSOR_H

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/BaseProcessor.h"
#include "storage/exec/IndexNode.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kLookupAnnCounters;

/**
 * @brief ANN (Approximate Nearest Neighbor) index lookup processor.
 *        Supports multi-tag vector similarity search with filtering and deduplication.
 */
class LookupAnnProcessor : public BaseProcessor<cpp2::LookupIndexResp> {
 public:
  static LookupAnnProcessor* instance(StorageEnv* env,
                                      const ProcessorCounters* counters = &kLookupAnnCounters,
                                      folly::Executor* executor = nullptr) {
    return new LookupAnnProcessor(env, counters, executor);
  }

  void process(const cpp2::LookupAnnIndexRequest& req);

 private:
  LookupAnnProcessor(StorageEnv* env, const ProcessorCounters* counters, folly::Executor* executor)
      : BaseProcessor<cpp2::LookupIndexResp>(env, counters), executor_(executor) {}

  void doProcess(const cpp2::LookupAnnIndexRequest& req);
  void onProcessFinished() {
    BaseProcessor<cpp2::LookupIndexResp>::resp_.data_ref() = std::move(resultDataSet_);
    BaseProcessor<cpp2::LookupIndexResp>::resp_.stat_data_ref() = std::move(statsDataSet_);
  }

  ::nebula::cpp2::ErrorCode prepare(const cpp2::LookupAnnIndexRequest& req);

  // Build execution plan for ANN index scan
  ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<IndexNode>> buildAnnPlan(
      const cpp2::LookupAnnIndexRequest& req);

  ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<IndexNode>> buildOneAnnContext(
      const cpp2::IndexQueryContext& ctx);

  // Single threaded execution for single partition
  void runInSingleThread(const std::vector<PartitionID>& parts, std::unique_ptr<IndexNode> plan);

  // Multi-threaded execution for multiple partitions
  void runInMultipleThread(const std::vector<PartitionID>& parts, std::unique_ptr<IndexNode> plan);

  // Merge results from different tags/contexts
  void mergeResults();

  std::vector<std::unique_ptr<IndexNode>> reproducePlan(IndexNode* root, size_t count);

  folly::Executor* executor_{nullptr};
  std::unique_ptr<PlanContext> planContext_;
  std::unique_ptr<RuntimeContext> context_;

  nebula::DataSet resultDataSet_;  // Final output datasets
  nebula::DataSet statsDataSet_;
  std::map<PartitionID, nebula::DataSet> partResults_;  // Per-partition results
  int64_t param_;
  Value queryVector_;
  int32_t globalLimit_{-1};  // Global limit  results
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_INDEX_LOOKUPANNPROCESSOR_H
