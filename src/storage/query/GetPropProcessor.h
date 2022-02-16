/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_QUERY_GETPROPPROCESSOR_H_
#define STORAGE_QUERY_GETPROPPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Promise.h>  // for PromiseException::Prom...
#include <gtest/gtest_prod.h>

#include <utility>  // for move, pair
#include <vector>   // for vector

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"          // for DataSet, Row
#include "common/thrift/ThriftTypes.h"         // for PartitionID, VertexID
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"  // for GetPropResponse, GetPr...
#include "storage/CommonUtils.h"               // for RuntimeContext, Storag...
#include "storage/exec/StoragePlan.h"          // for StoragePlan
#include "storage/query/QueryBaseProcessor.h"  // for QueryBaseProcessor

namespace folly {
class Executor;

class Executor;
}  // namespace folly

namespace nebula {
namespace storage {

extern ProcessorCounters kGetPropCounters;

class GetPropProcessor : public QueryBaseProcessor<cpp2::GetPropRequest, cpp2::GetPropResponse> {
 public:
  static GetPropProcessor* instance(StorageEnv* env,
                                    const ProcessorCounters* counters = &kGetPropCounters,
                                    folly::Executor* executor = nullptr) {
    return new GetPropProcessor(env, counters, executor);
  }

  void process(const cpp2::GetPropRequest& req) override;

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
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETPROPPROCESSOR_H_
