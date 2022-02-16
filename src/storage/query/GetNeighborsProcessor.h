/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_
#define STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Promise.h>  // for PromiseExcepti...
#include <gtest/gtest_prod.h>       // for FRIEND_TEST
#include <stdint.h>                 // for int64_t

#include <utility>  // for move, pair
#include <vector>   // for vector

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"                  // for DataSet, Row
#include "common/thrift/ThriftTypes.h"                 // for PartitionID
#include "interface/gen-cpp2/common_types.h"           // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"          // for GetNeighborsRe...
#include "storage/CommonUtils.h"                       // for RuntimeContext
#include "storage/context/StorageExpressionContext.h"  // for StorageExpress...
#include "storage/exec/StoragePlan.h"                  // for StoragePlan
#include "storage/query/QueryBaseProcessor.h"          // for QueryBaseProce...

namespace folly {
class Executor;

class Executor;
}  // namespace folly

namespace nebula {
namespace storage {

extern ProcessorCounters kGetNeighborsCounters;

class GetNeighborsProcessor
    : public QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::GetNeighborsResponse> {
  FRIEND_TEST(ScanEdgePropBench, EdgeTypePrefixScanVsVertexPrefixScan);

 public:
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
      const std::vector<nebula::Row>& rows,
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
