/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/AppendVerticesExecutor.h"

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

Status AppendVerticesExecutor::handleResp(
    storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp) {
  auto result = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  auto state = std::move(result).value();
  auto *av = asNode<AppendVertices>(node());
  auto *vFilter = av->vFilter();
  QueryExpressionContext ctx(qctx()->ectx());

  // Ok, merge DataSets to one
  nebula::DataSet v;
  for (auto &resp : rpcResp.responses()) {
    if (resp.props_ref().has_value()) {
      if (UNLIKELY(!v.append(std::move(*resp.props_ref())))) {
        // it's impossible according to the interface
        LOG(WARNING) << "Heterogeneous props dataset";
        state = Result::State::kPartialSuccess;
      }
    } else {
      state = Result::State::kPartialSuccess;
    }
  }
  auto iter = PropIter(std::make_shared<Value>(std::move(v)));

  auto inputIter = qctx()->ectx()->getResult(av->inputVar()).iter();
  DataSet ret;
  ret.colNames = av->colNames();
  ret.rows.reserve(inputIter->size());
  if (!av->trackPrevPath()) {  // eg. MATCH (v:Person) RETURN v
    for (; iter.valid(); iter.next()) {
      if (vFilter != nullptr) {
        auto &vFilterVal = vFilter->eval(ctx(&iter));
        if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
          continue;
        }
      }
      Row row;
      row.values.emplace_back(iter.getVertex());
      ret.rows.emplace_back(std::move(row));
    }
    return finish(ResultBuilder().value(Value(std::move(ret))).state(state).build());
  }

  std::unordered_map<Value, Value> map;
  for (; iter.valid(); iter.next()) {
    if (vFilter != nullptr) {
      auto &vFilterVal = vFilter->eval(ctx(&iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    map.emplace(iter.getColumn(kVid), iter.getVertex());
  }

  auto *src = av->src();
  for (; inputIter->valid(); inputIter->next()) {
    auto dstFound = map.find(src->eval(ctx(inputIter.get())));
    if (dstFound == map.end()) {
      continue;
    }
    Row row = *inputIter->row();
    row.values.emplace_back(dstFound->second);
    ret.rows.emplace_back(std::move(row));
  }
  return finish(ResultBuilder().value(Value(std::move(ret))).state(state).build());
}

}  // namespace graph
}  // namespace nebula
