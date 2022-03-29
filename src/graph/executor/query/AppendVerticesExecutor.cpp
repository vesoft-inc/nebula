// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/AppendVerticesExecutor.h"

#include <iterator>

#include "common/time/Duration.h"

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
  time::Duration dur;

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

  LOG(ERROR) << "appendVertices: " << dur.elapsedInUSec();
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
  time::Duration dur;
  auto result = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  auto *av = asNode<AppendVertices>(node());
  auto *vFilter = av->vFilter();
  QueryExpressionContext ctx(qctx()->ectx());

  auto inputIter = qctx()->ectx()->getResult(av->inputVar()).iter();
  result_.colNames = av->colNames();
  result_.rows.reserve(inputIter->size());

  LOG(ERROR) << "point1: " << dur.elapsedInUSec();

  for (auto &resp : rpcResp.responses()) {
    if (resp.props_ref().has_value()) {
      auto iter = PropIter(std::make_shared<Value>(std::move(*resp.props_ref())));
      for (; iter.valid(); iter.next()) {
        if (vFilter != nullptr) {
          auto &vFilterVal = vFilter->eval(ctx(&iter));
          if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
            continue;
          }
        }
        if (!av->trackPrevPath()) {  // eg. MATCH (v:Person) RETURN v
          Row row;
          row.values.emplace_back(iter.getVertex());
          result_.rows.emplace_back(std::move(row));
        } else {
          dsts_.emplace(iter.getColumn(kVid), iter.getVertex());
        }
      }
    }
  }
  LOG(ERROR) << "point2: " << dur.elapsedInUSec();

  if (!av->trackPrevPath()) {
    return finish(ResultBuilder().value(Value(std::move(result))).build());
  }

  size_t totalSize = inputIter->size();
  size_t batchSize = getBatchSize(totalSize);
  // Start multiple jobs for handling the results
  std::vector<folly::Future<StatusOr<DataSet>>> futures;
  size_t begin = 0, end = 0, dispathedCnt = 0;
  while (dispathedCnt < totalSize) {
    end = begin + batchSize > totalSize ? totalSize : begin + batchSize;
    auto f = folly::makeFuture<Status>(Status::OK())
                 .via(runner())
                 .thenValue([this, begin, end, tmpIter = inputIter->copy()](auto &&r) mutable {
                   UNUSED(r);
                   return handleJob(begin, end, tmpIter.get());
                 });
    futures.emplace_back(std::move(f));
    begin = end;
    dispathedCnt += batchSize;
  }

  LOG(ERROR) << "point3: " << dur.elapsedInUSec();
  return folly::collect(futures).via(runner()).thenValue([this](auto &&results) {
    time::Duration dur1;
    for (auto &r : results) {
      auto &&rows = std::move(r).value();
      result_.rows.insert(result_.rows.end(),
                          std::make_move_iterator(rows.begin()),
                          std::make_move_iterator(rows.end()));
    }
    LOG(ERROR) << "gather: " << dur1.elapsedInUSec();
    return finish(ResultBuilder().value(Value(std::move(result_))).build());
  });
}

DataSet AppendVerticesExecutor::handleJob(size_t begin, size_t end, Iterator *iter) {
  time::Duration dur;
  // Iterates to the begin pos
  size_t tmp = 0;
  while (iter->valid() && ++tmp < begin) {
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

  LOG(ERROR) << "handleJob: " << dur.elapsedInUSec() << begin << " " << end;
  return ds;
}

}  // namespace graph
}  // namespace nebula
