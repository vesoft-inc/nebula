/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_QUERY_SCANEDGEPROCESSOR_H_
#define STORAGE_QUERY_SCANEDGEPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/exec/ScanNode.h"
#include "storage/exec/StoragePlan.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kScanEdgeCounters;

/**
 * @brief Processor class to scan edges.
 *
 */
class ScanEdgeProcessor : public QueryBaseProcessor<cpp2::ScanEdgeRequest, cpp2::ScanResponse> {
 public:
  /**
   * @brief Generate new processor instance for edge scan.
   *
   * @param env Related environment variables for storage.
   * @param counters Statistic counter pointer for edge scan.
   * @param executor Expected executor for this processor, running directly if nullptr.
   * @return ScanEdgeProcessor* ScanEdgeProcessor instance.
   */
  static ScanEdgeProcessor* instance(StorageEnv* env,
                                     const ProcessorCounters* counters = &kScanEdgeCounters,
                                     folly::Executor* executor = nullptr) {
    return new ScanEdgeProcessor(env, counters, executor);
  }

  /**
   * @brief Entry point of edge scan.
   *
   * @param req Request for edge scan.
   */
  void process(const cpp2::ScanEdgeRequest& req) override;

  /**
   * @brief Logic part of edge scan.
   *
   * @param req Request for edge scan.
   */
  void doProcess(const cpp2::ScanEdgeRequest& req);

 private:
  ScanEdgeProcessor(StorageEnv* env, const ProcessorCounters* counters, folly::Executor* executor)
      : QueryBaseProcessor<cpp2::ScanEdgeRequest, cpp2::ScanResponse>(env, counters, executor) {}

  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::ScanEdgeRequest& req) override;

  void buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps);

  StoragePlan<Cursor> buildPlan(RuntimeContext* context,
                                nebula::DataSet* result,
                                std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors,
                                StorageExpressionContext* expCtx);

  folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> runInExecutor(
      RuntimeContext* context,
      nebula::DataSet* result,
      std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors,
      PartitionID partId,
      Cursor cursor,
      StorageExpressionContext* expCtx);

  void runInSingleThread(const cpp2::ScanEdgeRequest& req);

  void runInMultipleThread(const cpp2::ScanEdgeRequest& req);

  void onProcessFinished() override;

  std::vector<RuntimeContext> contexts_;
  std::vector<StorageExpressionContext> expCtxs_;
  std::vector<nebula::DataSet> results_;
  std::vector<std::unordered_map<PartitionID, cpp2::ScanCursor>> cursorsOfPart_;

  std::unordered_map<PartitionID, cpp2::ScanCursor> cursors_;
  int64_t limit_{-1};
  bool enableReadFollower_{false};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_SCANEDGEPROCESSOR_H_
