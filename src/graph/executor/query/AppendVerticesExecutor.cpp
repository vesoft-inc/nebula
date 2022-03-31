// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/AppendVerticesExecutor.h"

#include <iterator>

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {
folly::Future<Status> AppendVerticesExecutor::execute() {
  return appendVertices();
}

DataSet AppendVerticesExecutor::buildRequestDataSet(const AppendVertices *av) {
  if (av == nullptr) {
    return nebula::DataSet({kVid});
  }
  auto valueIter = ectx_->getResult(av->inputVar()).iter();
  return buildRequestDataSetByVidType(valueIter.get(), av->src(), av->dedup());
}

folly::Future<Status> AppendVerticesExecutor::appendVertices() {
  SCOPED_TIMER(&execTime_);

  auto *av = asNode<AppendVertices>(node());
  StorageClient *storageClient = qctx()->getStorageClient();

  DataSet vertices = buildRequestDataSet(av);
  if (vertices.rows.empty()) {
    return finish(ResultBuilder().value(Value(DataSet(av->colNames()))).build());
  }

  StorageClient::CommonRequestParam param(av->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());

  time::Duration getPropsTime;
  return DCHECK_NOTNULL(storageClient)
      ->getProps(param,
                 std::move(vertices),
                 av->props(),
                 nullptr,
                 av->exprs(),
                 av->dedup(),
                 av->orderBy(),
                 av->limit(qctx()),
                 av->filter())
      .via(runner())
      .ensure([this, getPropsTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc", folly::sformat("{}(us)", getPropsTime.elapsedInUSec()));
      })
      .thenValue([this](StorageRpcResponse<GetPropResponse> &&rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp));
      });
}

folly::Future<Status> AppendVerticesExecutor::handleResp(
    storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp) {
  auto result = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  auto *av = asNode<AppendVertices>(node());

  auto inputIter = qctx()->ectx()->getResult(av->inputVar()).iter();
  result_.colNames = av->colNames();
  result_.rows.reserve(inputIter->size());

  nebula::DataSet v;
  for (auto &resp : rpcResp.responses()) {
    if (resp.props_ref().has_value()) {
      auto &&respV = std::move(*resp.props_ref());
      v.colNames = respV.colNames;
      v.rows.insert(v.rows.end(),
                    std::make_move_iterator(respV.begin()),
                    std::make_move_iterator(respV.end()));
    }
  }
  auto propIter = PropIter(std::make_shared<Value>(std::move(v)));

  folly::Future<Status> prepareF = folly::makeFuture<Status>(Status::OK());
  if (!av->trackPrevPath()) {
    size_t totalSize = propIter.size();
    size_t batchSize = getBatchSize(totalSize);
    // Start multiple jobs for handling the results
    std::vector<folly::Future<StatusOr<DataSet>>> futures;
    size_t begin = 0, end = 0, dispathedCnt = 0;
    while (dispathedCnt < totalSize) {
      end = begin + batchSize > totalSize ? totalSize : begin + batchSize;
      auto f = folly::makeFuture<Status>(Status::OK())
                   .via(runner())
                   .thenValue([this, begin, end, tmpIter = propIter.copy()](auto &&r) mutable {
                     UNUSED(r);
                     return buildVerticesResult(begin, end, tmpIter.get());
                   });
      futures.emplace_back(std::move(f));
      begin = end;
      dispathedCnt += batchSize;
    }

    prepareF = folly::collect(futures).via(runner()).thenValue([this](auto &&results) {
      for (auto &r : results) {
        auto &&rows = std::move(r).value();
        result_.rows.insert(result_.rows.end(),
                            std::make_move_iterator(rows.begin()),
                            std::make_move_iterator(rows.end()));
      }
      return finish(ResultBuilder().value(Value(std::move(result_))).build());
    });
  } else {
    size_t totalSize = propIter.size();
    size_t batchSize = getBatchSize(totalSize);
    // Start multiple jobs for handling the results
    std::vector<folly::Future<StatusOr<folly::Unit>>> futures;
    size_t begin = 0, end = 0, dispathedCnt = 0;
    while (dispathedCnt < totalSize) {
      end = begin + batchSize > totalSize ? totalSize : begin + batchSize;
      auto f = folly::makeFuture<Status>(Status::OK())
                   .via(runner())
                   .thenValue([this, begin, end, tmpIter = propIter.copy()](auto &&r) mutable {
                     UNUSED(r);
                     return buildMap(begin, end, tmpIter.get());
                   });
      futures.emplace_back(std::move(f));
      begin = end;
      dispathedCnt += batchSize;
    }

    prepareF = folly::collect(futures).via(runner()).thenValue([](auto &&results) {
      UNUSED(results);
      return Status::OK();
    });
  }

  return std::move(prepareF).via(runner()).thenValue([this, inputIterNew = std::move(inputIter)](
                                                         auto &&prepareResult) {
    UNUSED(prepareResult);
    size_t totalSize = inputIterNew->size();
    size_t batchSize = getBatchSize(totalSize);
    // Start multiple jobs for handling the results
    std::vector<folly::Future<StatusOr<DataSet>>> futures;
    size_t begin = 0, end = 0, dispathedCnt = 0;
    while (dispathedCnt < totalSize) {
      end = begin + batchSize > totalSize ? totalSize : begin + batchSize;
      auto f = folly::makeFuture<Status>(Status::OK())
                   .via(runner())
                   .thenValue([this, begin, end, tmpIter = inputIterNew->copy()](auto &&r) mutable {
                     UNUSED(r);
                     return handleJob(begin, end, tmpIter.get());
                   });
      futures.emplace_back(std::move(f));
      begin = end;
      dispathedCnt += batchSize;
    }

    return folly::collect(futures).via(runner()).thenValue([this](auto &&results) {
      for (auto &r : results) {
        auto &&rows = std::move(r).value();
        result_.rows.insert(result_.rows.end(),
                            std::make_move_iterator(rows.begin()),
                            std::make_move_iterator(rows.end()));
      }
      return finish(ResultBuilder().value(Value(std::move(result_))).build());
    });
  });
}

DataSet AppendVerticesExecutor::buildVerticesResult(size_t begin, size_t end, Iterator *iter) {
  // Iterates to the begin pos
  size_t tmp = 0;
  for (; iter->valid() && tmp < begin; ++tmp) {
    iter->next();
  }

  auto *av = asNode<AppendVertices>(node());
  auto vFilter = av->vFilter() ? av->vFilter()->clone() : nullptr;
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(end - begin);
  QueryExpressionContext ctx(qctx()->ectx());
  for (; iter->valid() && tmp++ < end; iter->next()) {
    if (vFilter != nullptr) {
      auto &vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    Row row;
    row.values.emplace_back(iter->getVertex());
    ds.rows.emplace_back(std::move(row));
  }

  return ds;
}

void AppendVerticesExecutor::buildMap(size_t begin, size_t end, Iterator *iter) {
  // Iterates to the begin pos
  size_t tmp = 0;
  for (; iter->valid() && tmp < begin; ++tmp) {
    iter->next();
  }

  auto *av = asNode<AppendVertices>(node());
  auto vFilter = av->vFilter() ? av->vFilter()->clone() : nullptr;
  QueryExpressionContext ctx(qctx()->ectx());
  for (; iter->valid() && tmp++ < end; iter->next()) {
    if (vFilter != nullptr) {
      auto &vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    dsts_.emplace(iter->getColumn(kVid), iter->getVertex());
  }
}

DataSet AppendVerticesExecutor::handleJob(size_t begin, size_t end, Iterator *iter) {
  // Iterates to the begin pos
  size_t tmp = 0;
  for (; iter->valid() && tmp < begin; ++tmp) {
    iter->next();
  }

  auto *av = asNode<AppendVertices>(node());
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(end - begin);
  auto src = av->src()->clone();
  QueryExpressionContext ctx(qctx()->ectx());
  for (; iter->valid() && tmp++ < end; iter->next()) {
    auto dstFound = dsts_.find(src->eval(ctx(iter)));
    if (dstFound == dsts_.end()) {
      continue;
    }
    Row row = *iter->row();
    row.values.emplace_back(dstFound->second);
    ds.rows.emplace_back(std::move(row));
  }

  return ds;
}

}  // namespace graph
}  // namespace nebula
