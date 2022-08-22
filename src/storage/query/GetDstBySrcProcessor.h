/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_QUERY_GETDSTBYSRCPROCESSOR_H_
#define STORAGE_QUERY_GETDSTBYSRCPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/exec/StoragePlan.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kGetDstBySrcCounters;

class GetDstBySrcProcessor
    : public QueryBaseProcessor<cpp2::GetDstBySrcRequest, cpp2::GetDstBySrcResponse> {
 public:
  static GetDstBySrcProcessor* instance(StorageEnv* env,
                                        const ProcessorCounters* counters = &kGetDstBySrcCounters,
                                        folly::Executor* executor = nullptr) {
    return new GetDstBySrcProcessor(env, counters, executor);
  }

  void process(const cpp2::GetDstBySrcRequest& req) override;

 protected:
  GetDstBySrcProcessor(StorageEnv* env,
                       const ProcessorCounters* counters,
                       folly::Executor* executor)
      : QueryBaseProcessor<cpp2::GetDstBySrcRequest, cpp2::GetDstBySrcResponse>(
            env, counters, executor) {}

  void onProcessFinished() override;

  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetDstBySrcRequest& req) override;

 private:
  void doProcess(const cpp2::GetDstBySrcRequest& req);

  nebula::cpp2::ErrorCode buildEdgeContext(const cpp2::GetDstBySrcRequest& req);

  void runInSingleThread(const cpp2::GetDstBySrcRequest& req);

  void runInMultipleThread(const cpp2::GetDstBySrcRequest& req);

  folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> runInExecutor(
      RuntimeContext* context,
      nebula::List* result,
      PartitionID partId,
      const std::vector<Value>& srcIds);

  StoragePlan<VertexID> buildPlan(RuntimeContext* context, nebula::List* result);

 private:
  std::vector<RuntimeContext> contexts_;
  // The process result of each part if run concurrently, then merge into resultDataSet_ at last
  std::vector<nebula::List> partResults_;
  nebula::List flatResult_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETDSTBYSRCPROCESSOR_H_
