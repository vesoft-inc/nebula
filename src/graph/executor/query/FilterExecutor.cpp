// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/FilterExecutor.h"

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> FilterExecutor::execute() {
  time::Duration dur;
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

  size_t totalSize = iter->size();
  size_t batchSize = getBatchSize(totalSize);
  // Start multiple jobs for handling the results
  std::vector<folly::Future<StatusOr<DataSet>>> futures;
  size_t begin = 0, end = 0, dispathedCnt = 0;
  while (dispathedCnt < totalSize) {
    end = begin + batchSize > totalSize ? totalSize : begin + batchSize;
    auto f = folly::makeFuture<Status>(Status::OK())
                 .via(runner())
                 .thenValue([this, begin, end, tmpIter = iter->copy()](auto &&r) mutable {
                   UNUSED(r);
                   return handleJob(begin, end, tmpIter.get());
                 });
    futures.emplace_back(std::move(f));
    begin = end;
    dispathedCnt += batchSize;
  }

  LOG(ERROR) << "prepare: " << dur.elapsedInUSec();
  return folly::collect(futures).via(runner()).thenValue(
      [this, result = std::move(ds)](auto &&results) mutable {
        time::Duration dur1;
        for (auto &r : results) {
          auto &&rows = std::move(r).value();
          result.rows.insert(result.rows.end(),
                             std::make_move_iterator(rows.begin()),
                             std::make_move_iterator(rows.end()));
        }
        finish(ResultBuilder().value(Value(std::move(result))).build());
        LOG(ERROR) << "gather: " << dur1.elapsedInUSec();
        return Status::OK();
      });
}

DataSet FilterExecutor::handleJob(size_t begin, size_t end, Iterator *iter) {
  time::Duration dur;
  // Iterates to the begin pos
  size_t tmp = 0;
  while (iter->valid() && ++tmp < begin) {
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
