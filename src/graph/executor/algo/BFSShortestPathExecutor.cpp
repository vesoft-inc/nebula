/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/algo/BFSShortestPathExecutor.h"

#include <algorithm>      // for max
#include <ostream>        // for operator<<, basic_ostream
#include <string>         // for string, operator<<, basi...
#include <unordered_map>  // for operator!=, unordered_mu...
#include <utility>        // for move, pair
#include <vector>         // for vector

#include "common/base/Logging.h"             // for COMPACT_GOOGLE_LOG_INFO
#include "common/base/Status.h"              // for Status
#include "common/datatypes/DataSet.h"        // for Row, DataSet
#include "common/datatypes/Edge.h"           // for Edge, operator<<
#include "common/time/ScopedTimer.h"         // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Iterator.h"          // for Iterator
#include "graph/context/Result.h"            // for ResultBuilder, Result
#include "graph/planner/plan/Algo.h"         // for BFSShortestPath
#include "graph/planner/plan/PlanNode.h"     // for PlanNode

namespace nebula {
namespace graph {
folly::Future<Status> BFSShortestPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* bfs = asNode<BFSShortestPath>(node());
  auto iter = ectx_->getResult(bfs->inputVar()).iter();
  VLOG(1) << "current: " << node()->outputVar();
  VLOG(1) << "input: " << bfs->inputVar();
  DCHECK(!!iter);

  DataSet ds;
  ds.colNames = node()->colNames();
  std::unordered_multimap<Value, Value> interim;

  for (; iter->valid(); iter->next()) {
    auto edgeVal = iter->getEdge();
    if (!edgeVal.isEdge()) {
      continue;
    }
    auto& edge = edgeVal.getEdge();
    auto visited = visited_.find(edge.dst) != visited_.end();
    if (visited) {
      continue;
    }

    // save the starts.
    visited_.emplace(edge.src);
    VLOG(1) << "dst: " << edge.dst << " edge: " << edge;
    interim.emplace(edge.dst, std::move(edgeVal));
  }
  for (auto& kv : interim) {
    auto dst = std::move(kv.first);
    auto edge = std::move(kv.second);
    Row row;
    row.values.emplace_back(dst);
    row.values.emplace_back(std::move(edge));
    ds.rows.emplace_back(std::move(row));
    visited_.emplace(dst);
  }
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}
}  // namespace graph
}  // namespace nebula
