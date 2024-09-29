// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/LimitExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/stats/GraphStats.h"
namespace nebula {
namespace graph {

folly::Future<Status> LimitExecutor::execute() {
  auto start_ts = std::chrono::steady_clock::now();
  auto& spaceName = qctx()->rctx() ? qctx()->rctx()->session()->spaceName() : "";
  SCOPED_TIMER(&execTime_);

  auto* limit = asNode<Limit>(node());
  auto inputVar = limit->inputVar();
  // Use the userCount of the operator's inputVar at runtime to determine whether concurrent
  // read-write conflicts exist, and if so, copy the data
  bool canMoveData = movable(inputVar);
  Result result = ectx_->getResult(inputVar);
  auto* iter = result.iterRef();
  // Always reuse getNeighbors's dataset to avoid some go statement execution plan related issues
  if (iter->isGetNeighborsIter()) {
    canMoveData = true;
  }
  ResultBuilder builder;
  auto offset = static_cast<std::size_t>(limit->offset());
  QueryExpressionContext qec(ectx_);
  auto count = static_cast<std::size_t>(limit->count(qec));
  if (LIKELY(canMoveData)) {
    builder.value(result.valuePtr());
    iter->select(offset, count);
    builder.iter(std::move(result).iter());
    auto diff = std::chrono::steady_clock::now() - start_ts;
    stats::StatsManager::addValue(
        kLimitExecutorsLatencyUs,
        std::chrono::duration_cast<std::chrono::milliseconds>(diff).count());
    if (FLAGS_enable_space_level_metrics && spaceName != "") {
      stats::StatsManager::addValue(
          stats::StatsManager::histoWithLabels(kLimitExecutorsLatencyUs, {{"space", spaceName}}));
    }
    return finish(builder.build());
  } else {
    DataSet ds;
    ds.colNames = result.getColNames();
    for (std::size_t i = 0; iter->valid(); ++i, iter->next()) {
      if (i >= offset && i <= (offset + count - 1)) {
        Row row;
        row = *iter->row();
        ds.rows.emplace_back(std::move(row));
      }
    }
    return finish(builder.value(Value(std::move(ds))).iter(Iterator::Kind::kProp).build());
  }
}

}  // namespace graph
}  // namespace nebula
