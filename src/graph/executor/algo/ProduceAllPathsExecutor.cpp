/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "graph/executor/algo/ProduceAllPathsExecutor.h"

#include <algorithm>  // for max
#include <memory>     // for unique_ptr, allocator_tr...
#include <string>     // for string, basic_string, hash
#include <utility>    // for move, pair

#include "common/base/Logging.h"             // for COMPACT_GOOGLE_LOG_INFO
#include "common/base/Status.h"              // for Status
#include "common/datatypes/DataSet.h"        // for Row, DataSet
#include "common/datatypes/Edge.h"           // for Edge
#include "common/datatypes/List.h"           // for List
#include "common/datatypes/Path.h"           // for Step, Path, operator<<
#include "common/datatypes/Vertex.h"         // for Tag, Vertex
#include "common/time/ScopedTimer.h"         // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Iterator.h"          // for Iterator
#include "graph/context/Result.h"            // for ResultBuilder, Result
#include "graph/planner/plan/Algo.h"         // for ProduceAllPaths
#include "graph/planner/plan/PlanNode.h"     // for PlanNode

namespace nebula {
namespace graph {
folly::Future<Status> ProduceAllPathsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* allPaths = asNode<ProduceAllPaths>(node());
  noLoop_ = allPaths->noLoop();
  auto iter = ectx_->getResult(allPaths->inputVar()).iter();
  DCHECK(!!iter);

  DataSet ds;
  ds.colNames = node()->colNames();
  Interims interims;

  if (!iter->isGetNeighborsIter()) {
    return Status::Error("Only accept GetNeighborsIter.");
  }
  for (; iter->valid(); iter->next()) {
    auto edgeVal = iter->getEdge();
    if (!edgeVal.isEdge()) {
      continue;
    }
    auto& edge = edgeVal.getEdge();
    auto histPaths = historyPaths_.find(edge.src);
    if (histPaths == historyPaths_.end()) {
      createPaths(edge, interims);
    } else {
      buildPaths(histPaths->second, edge, interims);
    }
  }

  for (auto& interim : interims) {
    Row row;
    auto& dst = interim.first;
    auto& paths = interim.second;
    row.values.emplace_back(std::move(dst));
    row.values.emplace_back(List(std::move(paths)));
    ds.rows.emplace_back(std::move(row));

    const auto& dstInDs = ds.rows.back().values.front();
    for (auto& path : ds.rows.back().values.back().getList().values) {
      historyPaths_[dstInDs].emplace_back(&path.getPath());
    }
  }
  count_++;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

void ProduceAllPathsExecutor::createPaths(const Edge& edge, Interims& interims) {
  Path path;
  path.src = Vertex(edge.src, {});
  path.steps.emplace_back(Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));
  if (noLoop_ && path.hasDuplicateVertices()) {
    return;
  }
  VLOG(1) << "Create path: " << path;
  interims[edge.dst].emplace_back(std::move(path));
}

void ProduceAllPathsExecutor::buildPaths(const std::vector<const Path*>& history,
                                         const Edge& edge,
                                         Interims& interims) {
  for (auto* histPath : history) {
    if (histPath->steps.size() < count_) {
      continue;
    } else {
      Path path = *histPath;
      path.steps.emplace_back(Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));
      if (path.hasDuplicateEdges()) {
        continue;
      }
      if (noLoop_ && path.hasDuplicateVertices()) {
        continue;
      }
      VLOG(1) << "Build path: " << path;
      interims[edge.dst].emplace_back(std::move(path));
    }
  }
}

}  // namespace graph
}  // namespace nebula
