// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/FilterExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"

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

  if (FLAGS_max_job_size == 1 || iter->isGetNeighborsIter()) {
    // TODO :GetNeighborsIterator is not an thread safe implementation.
    return handleSingleJobFilter();
  } else {
    DataSet ds;
    ds.colNames = iter->valuePtr()->getDataSet().colNames;
    ds.rows.reserve(iter->size());
    auto scatter = [this](
                       size_t begin, size_t end, Iterator *tmpIter) mutable -> StatusOr<DataSet> {
      return handleJob(begin, end, tmpIter);
    };

    auto gather =
        [this, result = std::move(ds), kind = iter->kind()](auto &&results) mutable -> Status {
      for (auto &r : results) {
        if (!r.ok()) {
          return r.status();
        }
        auto &&rows = std::move(r).value();
        result.rows.insert(result.rows.end(),
                           std::make_move_iterator(rows.begin()),
                           std::make_move_iterator(rows.end()));
      }
      if (kind == Iterator::Kind::kSequential) {
        finish(ResultBuilder().value(Value(std::move(result))).build());
      } else if (kind == Iterator::Kind::kProp) {
        finish(ResultBuilder().value(Value(std::move(result))).iter(Iterator::Kind::kProp).build());
      } else {
        LOG(ERROR) << "Default and Getneigbors Iter not support multi job filter.";
      }
      return Status::OK();
    };

    return runMultiJobs(std::move(scatter), std::move(gather), iter.get());
  }
}

StatusOr<DataSet> FilterExecutor::handleJob(size_t begin, size_t end, Iterator *iter) {
  auto *filter = asNode<Filter>(node());
  QueryExpressionContext ctx(ectx_);
  auto condition = filter->condition()->clone();
  DataSet ds;
  for (; iter->valid() && begin++ < end; iter->next()) {
    auto val = condition->eval(ctx(iter));
    if (val.isBadNull() || (!val.empty() && !val.isImplicitBool() && !val.isNull())) {
      return Status::Error("Wrong type result, the type should be NULL, EMPTY, BOOL");
    }
    if (!(val.empty() || val.isNull() || (val.isImplicitBool() && !val.implicitBool()))) {
      // TODO: Maybe we can move.
      auto row = *iter->row();
      ds.rows.emplace_back(std::move(row));
    }
  }
  return ds;
}

Status FilterExecutor::handleSingleJobFilter() {
  auto *filter = asNode<Filter>(node());
  auto inputVar = filter->inputVar();
  // Use the userCount of the operator's inputVar at runtime to determine whether concurrent
  // read-write conflicts exist, and if so, copy the data
  bool canMoveData = movable(inputVar);
  Result result = ectx_->getResult(inputVar);
  auto *iter = result.iterRef();
  // Always reuse getNeighbors's dataset to avoid some go statement execution plan related issues
  if (iter->isGetNeighborsIter()) {
    canMoveData = true;
  }
  ResultBuilder builder;
  QueryExpressionContext ctx(ectx_);
  auto condition = filter->condition();
  if (LIKELY(canMoveData)) {
    builder.value(result.valuePtr());
    while (iter->valid()) {
      auto val = condition->eval(ctx(iter));
      if (val.isBadNull() || (!val.empty() && !val.isImplicitBool() && !val.isNull())) {
        return Status::Error("Wrong type result, the type should be NULL, EMPTY, BOOL");
      }
      if (val.empty() || val.isNull() || (val.isImplicitBool() && !val.implicitBool())) {
        if (UNLIKELY(filter->needStableFilter())) {
          iter->erase();
        } else {
          iter->unstableErase();
        }
      } else {
        iter->next();
      }
    }

    iter->reset();
    builder.iter(std::move(result).iter());
    return finish(builder.build());
  } else {
    DataSet ds;
    ds.colNames = result.getColNames();
    ds.rows.reserve(iter->size());
    for (; iter->valid(); iter->next()) {
      auto val = condition->eval(ctx(iter));
      if (val.isBadNull() || (!val.empty() && !val.isImplicitBool() && !val.isNull())) {
        return Status::Error("Wrong type result, the type should be NULL, EMPTY, BOOL");
      }
      if (val.isImplicitBool() && val.implicitBool()) {
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
