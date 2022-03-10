/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_QUERY_GETPROPPROCESSOR_H_
#define STORAGE_QUERY_GETPROPPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/exec/StoragePlan.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kGetPropCounters;

/**
 * @brief Processor to get properties.
 *
 */
class GetPropProcessor : public QueryBaseProcessor<cpp2::GetPropRequest, cpp2::GetPropResponse> {
 public:
  /**
   * @brief Consturct instance of GetPropProcessor
   *
   * @param env Related environment variables for storage.
   * @param counters Statistic counter pointer for getting properties.
   * @param executor Expected executor for this processor, running directly if nullptr.
   * @return GetPropProcessor* Consturcted instance.
   */
  static GetPropProcessor* instance(StorageEnv* env,
                                    const ProcessorCounters* counters = &kGetPropCounters,
                                    folly::Executor* executor = nullptr) {
    return new GetPropProcessor(env, counters, executor);
  }

  /**
   * @brief Entry point of getting properties.
   *
   * @param req Reuqest for getting properties.
   */
  void process(const cpp2::GetPropRequest& req) override;

  /**
   * @brief Logic part of getting properties.
   *
   * @param req Reuqest for getting properties.
   */
  void doProcess(const cpp2::GetPropRequest& req);

 protected:
  GetPropProcessor(StorageEnv* env, const ProcessorCounters* counters, folly::Executor* executor)
      : QueryBaseProcessor<cpp2::GetPropRequest, cpp2::GetPropResponse>(env, counters, executor) {}

 private:
  StoragePlan<VertexID> buildTagPlan(RuntimeContext* context, nebula::DataSet* result);

  StoragePlan<cpp2::EdgeKey> buildEdgePlan(RuntimeContext* context, nebula::DataSet* result);

  void onProcessFinished() override;

  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetPropRequest& req) override;

  nebula::cpp2::ErrorCode checkRequest(const cpp2::GetPropRequest& req);

  nebula::cpp2::ErrorCode buildTagContext(const cpp2::GetPropRequest& req);

  nebula::cpp2::ErrorCode buildEdgeContext(const cpp2::GetPropRequest& req);

  void buildTagColName(const std::vector<cpp2::VertexProp>& tagProps);

  void buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps);

  void runInSingleThread(const cpp2::GetPropRequest& req);
  void runInMultipleThread(const cpp2::GetPropRequest& req);

  folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> runInExecutor(
      RuntimeContext* context,
      nebula::DataSet* result,
      PartitionID partId,
      const std::vector<nebula::Row>& rows);

 private:
  std::vector<RuntimeContext> contexts_;
  std::vector<nebula::DataSet> results_;
  bool isEdge_ = false;  // true for edge, false for tag
  std::size_t limit_{std::numeric_limits<std::size_t>::max()};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETPROPPROCESSOR_H_
