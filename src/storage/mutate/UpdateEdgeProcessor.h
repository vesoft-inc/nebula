/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
#define STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_

#include <folly/Function.h>         // for Function
#include <folly/Optional.h>         // for Optional
#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseExcepti...

#include <list>           // for list
#include <memory>         // for shared_ptr
#include <string>         // for string, basic_...
#include <unordered_set>  // for unordered_set
#include <utility>        // for pair, move
#include <vector>         // for vector

#include "common/expression/Expression.h"
#include "common/time/Duration.h"                      // for Duration
#include "interface/gen-cpp2/common_types.h"           // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"          // for UpdateResponse
#include "storage/CommonUtils.h"                       // for RuntimeContext
#include "storage/context/StorageExpressionContext.h"  // for StorageExpress...
#include "storage/exec/RelNode.h"                      // for RelNode
#include "storage/exec/StoragePlan.h"                  // for StoragePlan
#include "storage/query/QueryBaseProcessor.h"          // for QueryBaseProce...

namespace nebula {
class Expression;
namespace meta {
namespace cpp2 {
class IndexItem;
}  // namespace cpp2
}  // namespace meta
struct DataSet;
}  // namespace nebula

namespace folly {
class Executor;

class Executor;
}  // namespace folly

namespace nebula {
class Expression;
namespace meta {
namespace cpp2 {
class IndexItem;
}  // namespace cpp2
}  // namespace meta
struct DataSet;

namespace storage {

extern ProcessorCounters kUpdateEdgeCounters;

class UpdateEdgeProcessor
    : public QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse> {
 public:
  static UpdateEdgeProcessor* instance(StorageEnv* env,
                                       const ProcessorCounters* counters = &kUpdateEdgeCounters,
                                       folly::Executor* executor = nullptr) {
    return new UpdateEdgeProcessor(env, counters, executor);
  }

  void process(const cpp2::UpdateEdgeRequest& req) override;

  void doProcess(const cpp2::UpdateEdgeRequest& req);

  using ContextAdjuster = folly::Function<void(EdgeContext& ctx)>;
  void adjustContext(ContextAdjuster fn);

 private:
  UpdateEdgeProcessor(StorageEnv* env, const ProcessorCounters* counters, folly::Executor* executor)
      : QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse>(env, counters, executor) {
  }

  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) override;

  StoragePlan<cpp2::EdgeKey> buildPlan(nebula::DataSet* result);

  // Get the schema of all versions of edgeType in the spaceId
  nebula::cpp2::ErrorCode buildEdgeSchema();

  // Build EdgeContext by parsing return props expressions,
  // filter expression, update props expression
  nebula::cpp2::ErrorCode buildEdgeContext(const cpp2::UpdateEdgeRequest& req);

  void onProcessFinished() override;

  std::vector<Expression*> getReturnPropsExp() {
    return returnPropsExp_;
  }
  void profilePlan(StoragePlan<cpp2::EdgeKey>& plan) {
    auto& nodes = plan.getNodes();
    for (auto& node : nodes) {
      profileDetail(node->name_, node->duration_.elapsedInUSec());
    }
  }

 private:
  std::unique_ptr<RuntimeContext> context_;
  bool insertable_{false};

  cpp2::EdgeKey edgeKey_;

  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;

  std::unique_ptr<StorageExpressionContext> expCtx_;

  // update <prop name, new value expression>
  std::vector<storage::cpp2::UpdatedProp> updatedProps_;

  folly::Optional<std::vector<std::string>> returnProps_;
  folly::Optional<std::string> condition_;

  // return props expression
  std::vector<Expression*> returnPropsExp_;

  // condition expression
  Expression* filterExp_{nullptr};

  // updatedProps_ dependent props in value expression
  std::vector<std::pair<std::string, std::unordered_set<std::string>>> depPropMap_;

  std::list<ContextAdjuster> ctxAdjuster_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
