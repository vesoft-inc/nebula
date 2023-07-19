// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/UnwindExecutor.h"

#include "graph/planner/plan/Query.h"

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
      if (!unwind->fromPipe() && !emptyInput) {
        row = *(iter->row());
      }
      row.values.emplace_back(std::move(v));
      ds.rows.emplace_back(std::move(row));
    }
  }
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
