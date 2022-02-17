/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/UnwindExecutor.h"

#include <algorithm>  // for max
#include <memory>     // for unique_ptr, __shar...
#include <string>     // for string, basic_string
#include <utility>    // for move

#include "common/base/Logging.h"                   // for COMPACT_GOOGLE_LOG...
#include "common/base/Status.h"                    // for Status
#include "common/datatypes/DataSet.h"              // for Row, DataSet, oper...
#include "common/datatypes/List.h"                 // for List
#include "common/expression/Expression.h"          // for Expression
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder, Result
#include "graph/planner/plan/Query.h"              // for Unwind

namespace nebula {
namespace graph {

folly::Future<Status> UnwindExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *unwind = asNode<Unwind>(node());
  auto &inputRes = ectx_->getResult(unwind->inputVar());
  auto iter = inputRes.iter();
  bool emptyInput = inputRes.valuePtr()->type() == Value::Type::DATASET ? false : true;
  QueryExpressionContext ctx(ectx_);
  auto *unwindExpr = unwind->unwindExpr();

  DataSet ds;
  ds.colNames = unwind->colNames();
  for (; iter->valid(); iter->next()) {
    const Value &list = unwindExpr->eval(ctx(iter.get()));
    std::vector<Value> vals = extractList(list);
    for (auto &v : vals) {
      Row row;
      if (!emptyInput) {
        row = *(iter->row());
      }
      row.values.emplace_back(std::move(v));
      ds.rows.emplace_back(std::move(row));
    }
  }
  VLOG(1) << "Unwind result is: " << ds;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

std::vector<Value> UnwindExecutor::extractList(const Value &val) {
  std::vector<Value> ret;
  if (val.isList()) {
    auto &list = val.getList();
    ret = list.values;
  } else {
    if (!(val.isNull() || val.empty())) {
      ret.push_back(val);
    }
  }

  return ret;
}

}  // namespace graph
}  // namespace nebula
