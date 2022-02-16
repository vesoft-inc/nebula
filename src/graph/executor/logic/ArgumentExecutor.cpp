/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/logic/ArgumentExecutor.h"

#include <algorithm>      // for max
#include <memory>         // for unique_ptr, allocator
#include <string>         // for string, basic_string
#include <unordered_set>  // for unordered_set
#include <utility>        // for move, pair
#include <vector>         // for vector

#include "common/base/Logging.h"             // for COMPACT_GOOGLE_LOG_FATAL
#include "common/base/Status.h"              // for Status
#include "common/datatypes/DataSet.h"        // for Row, DataSet
#include "common/datatypes/Value.h"          // for Value, hash, operator==
#include "common/datatypes/Vertex.h"         // for Vertex
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Iterator.h"          // for Iterator
#include "graph/context/Result.h"            // for ResultBuilder, Result
#include "graph/planner/plan/Logic.h"        // for Argument

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;

class PlanNode;
class QueryContext;

ArgumentExecutor::ArgumentExecutor(const PlanNode *node, QueryContext *qctx)
    : Executor("ArgumentExecutor", node, qctx) {}

folly::Future<Status> ArgumentExecutor::execute() {
  auto *argNode = asNode<Argument>(node());
  auto &alias = argNode->getAlias();
  auto iter = ectx_->getResult(argNode->inputVar()).iter();
  DCHECK(iter != nullptr);

  DataSet ds;
  ds.colNames = argNode->colNames();
  ds.rows.reserve(iter->size());
  std::unordered_set<Value> unique;
  for (; iter->valid(); iter->next()) {
    auto val = iter->getColumn(alias);
    if (!val.isVertex()) {
      continue;
    }
    if (unique.emplace(val.getVertex().vid).second) {
      Row row;
      row.values.emplace_back(std::move(val));
      ds.rows.emplace_back(std::move(row));
    }
  }
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}
}  // namespace graph
}  // namespace nebula
