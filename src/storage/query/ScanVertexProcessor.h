/* Copyright (c) 2020 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_QUERY_SCANVERTEXPROCESSOR_H_
#define STORAGE_QUERY_SCANVERTEXPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/exec/ScanNode.h"
#include "storage/exec/StoragePlan.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kScanVertexCounters;

/**
 * @brief Processort to scan vertex.
 *
 */
class ScanVertexProcessor : public QueryBaseProcessor<cpp2::ScanVertexRequest, cpp2::ScanResponse> {
 public:
  /**
   * @brief Generate new processor instance for vertex scan.
   *
   * @param env Related environment variables for storage.
   * @param counters Statistic counter pointer for vertex scan.
   * @param executor Expected executor for this processor, running directly if nullptr.
   * @return ScanVertexProcessor* ScanVertexProcessor instance.
   */
  static ScanVertexProcessor* instance(StorageEnv* env,
                                       const ProcessorCounters* counters = &kScanVertexCounters,
                                       folly::Executor* executor = nullptr) {
    return new ScanVertexProcessor(env, counters, executor);
  }

  /**
   * @brief Entry point of vertex scan.
   *
   * @param req Request for vertex scan.
   */
  void process(const cpp2::ScanVertexRequest& req) override;

  /**
   * @brief Logic part of vertex scan.
   *
   * @param req Request for vertex scan.
   */
  void doProcess(const cpp2::ScanVertexRequest& req);

 private:
  ScanVertexProcessor(StorageEnv* env, const ProcessorCounters* counters, folly::Executor* executor)
      : QueryBaseProcessor<cpp2::ScanVertexRequest, cpp2::ScanResponse>(env, counters, executor) {}

  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::ScanVertexRequest& req) override;

  void buildTagColName(const std::vector<cpp2::VertexProp>& tagProps);

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

  void runInSingleThread(const cpp2::ScanVertexRequest& req);

  void runInMultipleThread(const cpp2::ScanVertexRequest& req);

  void onProcessFinished() override;

 private:
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

#endif  // STORAGE_QUERY_SCANVERTEXPROCESSOR_H_
