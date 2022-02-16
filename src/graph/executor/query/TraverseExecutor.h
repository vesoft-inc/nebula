/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
#define EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future
#include <stddef.h>                // for size_t
#include <stdint.h>                // for int64_t

#include <list>           // for list
#include <unordered_map>  // for unordered_map
#include <vector>         // for allocator, vector

#include "clients/storage/StorageClient.h"
#include "clients/storage/StorageClientBase.h"  // for StorageRpcResponse
#include "common/base/Status.h"                 // for Status
#include "common/base/StatusOr.h"
#include "common/datatypes/DataSet.h"              // for Row, DataSet
#include "common/datatypes/Edge.h"                 // for Edge
#include "common/datatypes/Geography.h"            // for Geography
#include "common/datatypes/List.h"                 // for List
#include "common/datatypes/Map.h"                  // for Map
#include "common/datatypes/Path.h"                 // for Path
#include "common/datatypes/Set.h"                  // for Set
#include "common/datatypes/Value.h"                // for Value
#include "common/datatypes/Vertex.h"               // for Vertex
#include "graph/executor/StorageAccessExecutor.h"  // for StorageAccessExecutor
#include "graph/planner/plan/Query.h"              // for Traverse
#include "interface/gen-cpp2/storage_types.h"      // for GetNeighborsResponse
#include "parser/MatchSentence.h"                  // for MatchStepRange

namespace nebula {
namespace graph {
class GetNeighborsIter;
class PlanNode;
class QueryContext;

class GetNeighborsIter;
class PlanNode;
class QueryContext;

using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;

class TraverseExecutor final : public StorageAccessExecutor {
 public:
  TraverseExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("Traverse", node, qctx) {
    traverse_ = asNode<Traverse>(node);
  }

  folly::Future<Status> execute() override;

  Status close() override;

 private:
  using Dst = Value;
  using Paths = std::vector<Row>;
  Status buildRequestDataSet();

  folly::Future<Status> traverse();

  void addStats(RpcResponse& resps, int64_t getNbrTimeInUSec);

  folly::Future<Status> getNeighbors();

  folly::Future<Status> handleResponse(RpcResponse&& resps);

  Status buildInterimPath(GetNeighborsIter* iter);

  Status buildResult();

  bool isFinalStep() const {
    return (range_ == nullptr && currentStep_ == 1) ||
           (range_ != nullptr && (currentStep_ == range_->max() || range_->max() == 0));
  }

  bool zeroStep() const {
    return range_ != nullptr && range_->min() == 0;
  }

  bool hasSameEdge(const Row& prevPath, const Edge& currentEdge);

  void releasePrevPaths(size_t cnt);

  void buildPath(std::unordered_map<Value, std::vector<Row>>& currentPaths,
                 const Value& dst,
                 Row&& path);

  Status handleZeroStep(const std::unordered_map<Value, Paths>& prev,
                        List&& vertices,
                        std::unordered_map<Value, Paths>& zeroSteps,
                        size_t& count);

 private:
  DataSet reqDs_;
  const Traverse* traverse_{nullptr};
  MatchStepRange* range_{nullptr};
  size_t currentStep_{0};
  std::list<std::unordered_map<Value, Paths>> paths_;
  size_t cnt_{0};
};

}  // namespace graph
}  // namespace nebula

#endif  // EXECUTOR_QUERY_TRAVERSEEXECUTOR_H_
