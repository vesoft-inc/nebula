/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_
#define STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_

#include <gtest/gtest_prod.h>

#include "common/base/Base.h"
#include "storage/exec/StoragePlan.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kGetNeighborsCounters;

/**
 * @brief Processor to get neighbors.
 *
 */
class GetNeighborsProcessor
    : public QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::GetNeighborsResponse> {
  FRIEND_TEST(ScanEdgePropBench, EdgeTypePrefixScanVsVertexPrefixScan);

 public:
  /**
   * @brief Consturct instance of GetNeighborsProcessor
   *
   * @param env Related environment variables for storage.
   * @param counters Statistic counter pointer for getting neighbors.
   * @param executor Expected executor for this processor, running directly if nullptr.
   * @return GetNeighborsProcessor* Consturcted instance.
   */
  static GetNeighborsProcessor* instance(StorageEnv* env,
                                         const ProcessorCounters* counters = &kGetNeighborsCounters,
                                         folly::Executor* executor = nullptr) {
    return new GetNeighborsProcessor(env, counters, executor);
  }

  void process(const cpp2::GetNeighborsRequest& req) override;

 protected:
  GetNeighborsProcessor(StorageEnv* env,
                        const ProcessorCounters* counters,
                        folly::Executor* executor)
      : QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::GetNeighborsResponse>(
            env, counters, executor) {}

  StoragePlan<VertexID> buildPlan(RuntimeContext* context,
                                  StorageExpressionContext* expCtx,
                                  nebula::DataSet* result,
                                  int64_t limit = 0,
                                  bool random = false);

  void onProcessFinished() override;

  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetNeighborsRequest& req) override;

 private:
  void doProcess(const cpp2::GetNeighborsRequest& req);

  nebula::cpp2::ErrorCode buildTagContext(const cpp2::TraverseSpec& req);
  nebula::cpp2::ErrorCode buildEdgeContext(const cpp2::TraverseSpec& req);

  // build tag/edge col name in response when prop specified
  void buildTagColName(const std::vector<cpp2::VertexProp>& tagProps);
  void buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps);

  // add PropContext of stat
  nebula::cpp2::ErrorCode handleEdgeStatProps(const std::vector<cpp2::StatProp>& statProps);

  void runInSingleThread(const cpp2::GetNeighborsRequest& req, int64_t limit, bool random);
  void runInMultipleThread(const cpp2::GetNeighborsRequest& req, int64_t limit, bool random);

  folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> runInExecutor(
      RuntimeContext* context,
      StorageExpressionContext* expCtx,
      nebula::DataSet* result,
      PartitionID partId,
      const std::vector<nebula::Value>& vids,
      int64_t limit,
      bool random);
  void profilePlan(StoragePlan<VertexID>& plan);

 private:
  std::vector<RuntimeContext> contexts_;
  std::vector<StorageExpressionContext> expCtxs_;
  std::vector<nebula::DataSet> results_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_
