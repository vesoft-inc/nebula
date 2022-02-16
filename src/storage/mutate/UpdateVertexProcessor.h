/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_MUTATE_UPDATEVERTEXROCESSOR_H_
#define STORAGE_MUTATE_UPDATEVERTEXROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseExcepti...

#include <memory>         // for shared_ptr
#include <string>         // for string, basic_...
#include <unordered_set>  // for unordered_set
#include <utility>        // for pair, move
#include <vector>         // for vector

#include "common/expression/Expression.h"
#include "common/thrift/ThriftTypes.h"                 // for VertexID, TagID
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

extern ProcessorCounters kUpdateVertexCounters;

class UpdateVertexProcessor
    : public QueryBaseProcessor<cpp2::UpdateVertexRequest, cpp2::UpdateResponse> {
 public:
  static UpdateVertexProcessor* instance(StorageEnv* env,
                                         const ProcessorCounters* counters = &kUpdateVertexCounters,
                                         folly::Executor* executor = nullptr) {
    return new UpdateVertexProcessor(env, counters, executor);
  }

  void process(const cpp2::UpdateVertexRequest& req) override;

  void doProcess(const cpp2::UpdateVertexRequest& req);

 private:
  UpdateVertexProcessor(StorageEnv* env,
                        const ProcessorCounters* counters,
                        folly::Executor* executor)
      : QueryBaseProcessor<cpp2::UpdateVertexRequest, cpp2::UpdateResponse>(
            env, counters, executor) {}

  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::UpdateVertexRequest& req) override;

  StoragePlan<VertexID> buildPlan(nebula::DataSet* result);

  // Get the schema of all versions of all tags in the spaceId
  nebula::cpp2::ErrorCode buildTagSchema();

  // Build TagContext by parsing return props expressions,
  // filter expression, update props expression
  nebula::cpp2::ErrorCode buildTagContext(const cpp2::UpdateVertexRequest& req);

  void onProcessFinished() override;

  std::vector<Expression*> getReturnPropsExp() {
    // std::vector<Expression*> result;
    // result.resize(returnPropsExp_.size());
    // auto get = [] (auto &ptr) {return ptr.get(); };
    // std::transform(returnPropsExp_.begin(), returnPropsExp_.end(),
    // result.begin(), get); return result;
    return returnPropsExp_;
  }
  void profilePlan(StoragePlan<VertexID>& plan) {
    auto& nodes = plan.getNodes();
    for (auto& node : nodes) {
      profileDetail(node->name_, node->duration_.elapsedInUSec());
    }
  }

 private:
  std::unique_ptr<RuntimeContext> context_;
  bool insertable_{false};

  // update tagId
  TagID tagId_;

  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;

  std::unique_ptr<StorageExpressionContext> expCtx_;

  // update <prop name, new value expression>
  std::vector<storage::cpp2::UpdatedProp> updatedProps_;

  // return props expression
  std::vector<Expression*> returnPropsExp_;

  // condition expression
  Expression* filterExp_{nullptr};

  // updatedProps_ dependent props in value expression
  std::vector<std::pair<std::string, std::unordered_set<std::string>>> depPropMap_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_MUTATE_UPDATEVERTEXROCESSOR_H_
