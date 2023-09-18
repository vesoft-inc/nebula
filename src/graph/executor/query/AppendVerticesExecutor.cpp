// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/AppendVerticesExecutor.h"

#include <iterator>

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

DECLARE_bool(optimize_appendvertices);

namespace nebula {
namespace graph {

folly::Future<Status> AppendVerticesExecutor::execute() {
  return appendVertices();
}

StatusOr<DataSet> AppendVerticesExecutor::buildRequestDataSet(const AppendVertices *av) {
  if (av == nullptr) {
    return nebula::DataSet({kVid});
  }
  auto valueIter = ectx_->getResult(av->inputVar()).iter();
  return buildRequestDataSetByVidType(valueIter.get(), av->src(), av->dedup(), true);
}

folly::Future<Status> AppendVerticesExecutor::appendVertices() {
  SCOPED_TIMER(&execTime_);
  auto *av = asNode<AppendVertices>(node());
  if (FLAGS_optimize_appendvertices && av != nullptr && av->noNeedFetchProp()) {
    return handleNullProp(av);
  }

  StorageClient *storageClient = qctx()->getStorageClient();
  auto res = buildRequestDataSet(av);
  NG_RETURN_IF_ERROR(res);
  auto vertices = std::move(res).value();
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
      .thenValue([this, getPropsTime](StorageRpcResponse<GetPropResponse> &&rpcResp) {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        addState("total_rpc", getPropsTime);
        addStats(rpcResp);
        if (FLAGS_max_job_size <= 1) {
          return folly::makeFuture<Status>(handleResp(std::move(rpcResp)));
        } else {
          return handleRespMultiJobs(std::move(rpcResp));
        }
      });
}

Status AppendVerticesExecutor::handleNullProp(const AppendVertices *av) {
  auto iter = ectx_->getResult(av->inputVar()).iter();
  auto *src = av->src();

  auto size = iter->size();
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(size);

  QueryExpressionContext ctx(ectx_);
  bool canBeMoved = movable(av->inputVars().front());

  for (; iter->valid(); iter->next()) {
    const auto &vid = src->eval(ctx(iter.get()));
    if (vid.empty()) {
      continue;
    }
    Vertex vertex;
    vertex.vid = vid;
    if (!av->trackPrevPath()) {
      Row row;
      row.values.emplace_back(std::move(vertex));
      ds.rows.emplace_back(std::move(row));
    } else {
      Row row;
      row = canBeMoved ? iter->moveRow() : *iter->row();
      row.values.emplace_back(std::move(vertex));
      ds.rows.emplace_back(std::move(row));
    }
  }
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

Status AppendVerticesExecutor::handleResp(
    storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp) {
  auto result = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  auto state = std::move(result).value();
  std::unordered_map<Value, Value> map;
  auto *av = asNode<AppendVertices>(node());
  auto *vFilter = av->vFilter();
  QueryExpressionContext ctx(qctx()->ectx());

  auto inputIter = qctx()->ectx()->getResult(av->inputVar()).iter();
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(inputIter->size());

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
          ds.rows.emplace_back(std::move(row));
        } else {
          map.emplace(iter.getColumn(kVid), iter.getVertex());
        }
      }
    }
  }

  if (!av->trackPrevPath()) {
    return finish(ResultBuilder().value(Value(std::move(ds))).state(state).build());
  }

  auto *src = av->src();
  bool mv = movable(av->inputVars().front());
  for (; inputIter->valid(); inputIter->next()) {
    auto dstFound = map.find(src->eval(ctx(inputIter.get())));
    if (dstFound != map.end()) {
      Row row = mv ? inputIter->moveRow() : *inputIter->row();
      row.values.emplace_back(dstFound->second);
      ds.rows.emplace_back(std::move(row));
    }
  }
  return finish(ResultBuilder().value(Value(std::move(ds))).state(state).build());
}

folly::Future<Status> AppendVerticesExecutor::handleRespMultiJobs(
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
      std::move(respV.begin(), respV.end(), std::back_inserter(v.rows));
    }
  }
  auto propIter = PropIter(std::make_shared<Value>(std::move(v)));

  if (!av->trackPrevPath()) {
    auto scatter = [this](
                       size_t begin, size_t end, Iterator *tmpIter) mutable -> StatusOr<DataSet> {
      return buildVerticesResult(begin, end, tmpIter);
    };

    auto gather = [this](std::vector<folly::Try<StatusOr<DataSet>>> &&results) -> Status {
      memory::MemoryCheckGuard guard;
      for (auto &respVal : results) {
        if (respVal.hasException()) {
          auto ex = respVal.exception().get_exception<std::bad_alloc>();
          if (ex) {
            throw std::bad_alloc();
          } else {
            throw std::runtime_error(respVal.exception().what().c_str());
          }
        }
        auto res = std::move(respVal).value();
        auto &&rows = std::move(res).value();
        std::move(rows.begin(), rows.end(), std::back_inserter(result_.rows));
      }
      return finish(ResultBuilder().value(Value(std::move(result_))).build());
    };

    return runMultiJobs(std::move(scatter), std::move(gather), &propIter);
  } else {
    auto scatter = [this](size_t begin, size_t end, Iterator *tmpIter) mutable -> folly::Unit {
      buildMap(begin, end, tmpIter);
      return folly::unit;
    };

    auto gather =
        [this, inputIterNew = std::move(inputIter)](
            std::vector<folly::Try<folly::Unit>> &&prepareResult) -> folly::Future<Status> {
      memory::MemoryCheckGuard guard1;
      for (auto &respVal : prepareResult) {
        if (respVal.hasException()) {
          auto ex = respVal.exception().get_exception<std::bad_alloc>();
          if (ex) {
            throw std::bad_alloc();
          } else {
            throw std::runtime_error(respVal.exception().what().c_str());
          }
        }
      }

      auto scatterInput =
          [this](size_t begin, size_t end, Iterator *tmpIter) mutable -> StatusOr<DataSet> {
        return handleJob(begin, end, tmpIter);
      };

      auto gatherFinal = [this](std::vector<folly::Try<StatusOr<DataSet>>> &&results) -> Status {
        memory::MemoryCheckGuard guard2;
        for (auto &respVal : results) {
          if (respVal.hasException()) {
            auto ex = respVal.exception().get_exception<std::bad_alloc>();
            if (ex) {
              throw std::bad_alloc();
            } else {
              throw std::runtime_error(respVal.exception().what().c_str());
            }
          }
          auto res = std::move(respVal).value();
          auto &&rows = std::move(res).value();
          std::move(rows.begin(), rows.end(), std::back_inserter(result_.rows));
        }
        return finish(ResultBuilder().value(Value(std::move(result_))).build());
      };

      return runMultiJobs(std::move(scatterInput), std::move(gatherFinal), inputIterNew.get());
    };

    return runMultiJobs(std::move(scatter), std::move(gather), &propIter);
  }
}

DataSet AppendVerticesExecutor::buildVerticesResult(size_t begin, size_t end, Iterator *iter) {
  auto *av = asNode<AppendVertices>(node());
  auto vFilter = av->vFilter() ? av->vFilter()->clone() : nullptr;
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(end - begin);
  QueryExpressionContext ctx(qctx()->ectx());
  for (; iter->valid() && begin++ < end; iter->next()) {
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
  auto *av = asNode<AppendVertices>(node());
  auto vFilter = av->vFilter() ? av->vFilter()->clone() : nullptr;
  QueryExpressionContext ctx(qctx()->ectx());
  for (; iter->valid() && begin++ < end; iter->next()) {
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
  auto *av = asNode<AppendVertices>(node());
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(end - begin);
  auto src = av->src()->clone();
  QueryExpressionContext ctx(qctx()->ectx());
  for (; iter->valid() && begin++ < end; iter->next()) {
    auto dstFound = dsts_.find(src->eval(ctx(iter)));
    if (dstFound != dsts_.end()) {
      Row row = *iter->row();
      row.values.emplace_back(dstFound->second);
      ds.rows.emplace_back(std::move(row));
    }
  }

  return ds;
}

}  // namespace graph
}  // namespace nebula
