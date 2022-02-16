/* Copyright (c) 2020 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_QUERY_SCANEDGEPROCESSOR_H_
#define STORAGE_QUERY_SCANEDGEPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Promise.h>  // for PromiseExcepti...
#include <stdint.h>                 // for int64_t

#include <unordered_map>  // for unordered_map
#include <utility>        // for move, pair
#include <vector>         // for vector

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"                  // for DataSet
#include "common/thrift/ThriftTypes.h"                 // for PartitionID
#include "interface/gen-cpp2/common_types.h"           // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"          // for ScanResponse
#include "storage/CommonUtils.h"                       // for RuntimeContext
#include "storage/context/StorageExpressionContext.h"  // for StorageExpress...
#include "storage/exec/ScanNode.h"                     // for Cursor
#include "storage/exec/StoragePlan.h"                  // for StoragePlan
#include "storage/query/QueryBaseProcessor.h"          // for QueryBaseProce...

namespace folly {
class Executor;

class Executor;
}  // namespace folly

namespace nebula {
namespace storage {

extern ProcessorCounters kScanEdgeCounters;

class ScanEdgeProcessor : public QueryBaseProcessor<cpp2::ScanEdgeRequest, cpp2::ScanResponse> {
 public:
  static ScanEdgeProcessor* instance(StorageEnv* env,
                                     const ProcessorCounters* counters = &kScanEdgeCounters,
                                     folly::Executor* executor = nullptr) {
    return new ScanEdgeProcessor(env, counters, executor);
  }

  void process(const cpp2::ScanEdgeRequest& req) override;

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
