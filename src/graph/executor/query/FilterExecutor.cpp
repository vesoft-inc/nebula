// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/FilterExecutor.h"

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> FilterExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *filter = asNode<Filter>(node());
  auto iter = ectx_->getResult(filter->inputVar()).iter();
  if (iter == nullptr || iter->isDefaultIter()) {
    auto status = Status::Error("iterator is nullptr or DefaultIter");
    LOG(ERROR) << status;
    return status;
  }

  DataSet ds;
  ds.colNames = iter->valuePtr()->getDataSet().colNames;
  ds.rows.reserve(iter->size());

  auto scatter = [this](size_t begin, size_t end, Iterator *tmpIter) mutable -> StatusOr<DataSet> {
    return handleJob(begin, end, tmpIter);
  };

  auto gather = [this, result = std::move(ds)](auto &&results) mutable -> Status {
    for (auto &r : results) {
      auto &&rows = std::move(r).value();
      result.rows.insert(result.rows.end(),
                         std::make_move_iterator(rows.begin()),
                         std::make_move_iterator(rows.end()));
    }
    finish(ResultBuilder().value(Value(std::move(result))).build());
    return Status::OK();
  };

  return runMultiJobs(std::move(scatter), std::move(gather), iter.get());
}

DataSet FilterExecutor::handleJob(size_t begin, size_t end, Iterator *iter) {
  // Iterates to the begin pos
  size_t tmp = 0;
  for (; iter->valid() && tmp < begin; ++tmp) {
    iter->next();
  }

  auto *filter = asNode<Filter>(node());
  QueryExpressionContext ctx(ectx_);
  auto condition = filter->condition()->clone();
  DataSet ds;
  for (; iter->valid() && tmp++ < end; iter->next()) {
    auto val = condition->eval(ctx(iter));
    if (val.isBadNull() || (!val.empty() && !val.isImplicitBool() && !val.isNull())) {
      // TODO:
      continue;
    }
    if (!(val.empty() || val.isNull() || (val.isImplicitBool() && !val.implicitBool()))) {
      auto row = *iter->row();
      ds.rows.emplace_back(std::move(row));
    }
  }
  return ds;
}

}  // namespace graph
}  // namespace nebula
