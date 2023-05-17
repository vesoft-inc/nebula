// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/StorageAccessExecutor.h"

#include <folly/Format.h>

#include "graph/context/Iterator.h"
#include "graph/context/QueryExpressionContext.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/Utils.h"
#include "interface/gen-cpp2/meta_types.h"

using apache::thrift::optional_field_ref;

namespace nebula {
namespace graph {

namespace internal {

template <typename VidType>
struct Vid;

template <>
struct Vid<int64_t> {
  static int64_t value(const Value &v) {
    return v.getInt();
  }
};

template <>
struct Vid<std::string> {
  static std::string value(const Value &v) {
    return v.getStr();
  }
};

template <typename VidType>
StatusOr<DataSet> buildRequestDataSet(const SpaceInfo &space,
                                      QueryExpressionContext &exprCtx,
                                      Iterator *iter,
                                      Expression *expr,
                                      bool dedup,
                                      bool isCypher) {
  DCHECK(iter && expr) << "iter=" << iter << ", expr=" << expr;
  nebula::DataSet vertices({kVid});
  auto s = iter->size();
  vertices.rows.reserve(s);

  std::unordered_set<VidType> uniqueSet;
  uniqueSet.reserve(s);

  const auto &vidType = *(space.spaceDesc.vid_type_ref());

  for (; iter->valid(); iter->next()) {
    auto vid = expr->eval(exprCtx(iter));
    if (vid.empty()) {
      continue;
    }
    if (!SchemaUtil::isValidVid(vid, vidType)) {
      if (isCypher) {
        continue;
      }
      std::stringstream ss;
      ss << "`" << vid.toString() << "', the srcs should be type of "
         << apache::thrift::util::enumNameSafe(vidType.get_type()) << ", but was`" << vid.type()
         << "'";
      return Status::Error(ss.str());
    }
    if (dedup && !uniqueSet.emplace(Vid<VidType>::value(vid)).second) {
      continue;
    }
    vertices.emplace_back(Row({std::move(vid)}));
  }
  return vertices;
}

template <typename VidType>
StatusOr<std::vector<Value>> buildRequestList(const SpaceInfo &space,
                                              QueryExpressionContext &exprCtx,
                                              Iterator *iter,
                                              Expression *expr,
                                              bool dedup,
                                              bool isCypher) {
  DCHECK(iter && expr) << "iter=" << iter << ", expr=" << expr;
  std::vector<Value> vids;
  auto iterSize = iter->size();
  vids.reserve(iterSize);

  std::unordered_set<VidType> uniqueSet;
  uniqueSet.reserve(iterSize);

  const auto &metaVidType = *(space.spaceDesc.vid_type_ref());
  auto vidType = SchemaUtil::propTypeToValueType(metaVidType.get_type());

  for (; iter->valid(); iter->next()) {
    auto vid = expr->eval(exprCtx(iter));
    if (vid.empty()) {
      continue;
    }
    if (vid.type() != vidType) {
      if (isCypher) {
        continue;
      }
      std::stringstream ss;
      ss << "`" << vid.toString() << "', the srcs should be type of " << vidType << ", but was`"
         << vid.type() << "'";
      return Status::Error(ss.str());
    }
    if (dedup && !uniqueSet.emplace(Vid<VidType>::value(vid)).second) {
      continue;
    }
    vids.emplace_back(std::move(vid));
  }
  return vids;
}

}  // namespace internal

bool StorageAccessExecutor::isIntVidType(const SpaceInfo &space) const {
  return (*space.spaceDesc.vid_type_ref()).type == nebula::cpp2::PropertyType::INT64;
}

StatusOr<DataSet> StorageAccessExecutor::buildRequestDataSetByVidType(Iterator *iter,
                                                                      Expression *expr,
                                                                      bool dedup,
                                                                      bool isCypher) {
  const auto &space = qctx()->rctx()->session()->space();
  QueryExpressionContext exprCtx(qctx()->ectx());

  if (isIntVidType(space)) {
    return internal::buildRequestDataSet<int64_t>(space, exprCtx, iter, expr, dedup, isCypher);
  }
  return internal::buildRequestDataSet<std::string>(space, exprCtx, iter, expr, dedup, isCypher);
}

StatusOr<std::vector<Value>> StorageAccessExecutor::buildRequestListByVidType(Iterator *iter,
                                                                              Expression *expr,
                                                                              bool dedup,
                                                                              bool isCypher) {
  const auto &space = qctx()->rctx()->session()->space();
  QueryExpressionContext exprCtx(qctx()->ectx());

  if (isIntVidType(space)) {
    return internal::buildRequestList<int64_t>(space, exprCtx, iter, expr, dedup, isCypher);
  }
  return internal::buildRequestList<std::string>(space, exprCtx, iter, expr, dedup, isCypher);
}

bool StorageAccessExecutor::hasSameEdge(const std::vector<Value> &edgeList, const Edge &edge) {
  for (auto &leftEdge : edgeList) {
    if (!leftEdge.isEdge()) {
      continue;
    }
    if (edge.keyEqual(leftEdge.getEdge())) {
      return true;
    }
  }
  return false;
}

folly::Future<std::vector<Value>> StorageAccessExecutor::getProps(
    const std::vector<Value> &vids, const std::vector<VertexProp> *vertexPropPtr) {
  nebula::DataSet vertices({kVid});
  vertices.rows.reserve(vids.size());
  for (auto &vid : vids) {
    vertices.emplace_back(Row({vid}));
  }
  StorageClient *storageClient = qctx_->getStorageClient();
  StorageClient::CommonRequestParam param(qctx_->rctx()->session()->space().id,
                                          qctx_->rctx()->session()->id(),
                                          qctx_->plan()->id(),
                                          qctx_->plan()->isProfileEnabled());
  return DCHECK_NOTNULL(storageClient)
      ->getProps(
          param, std::move(vertices), vertexPropPtr, nullptr, nullptr, false, {}, -1, nullptr)
      .via(runner())
      .thenValue([this](PropRpcResponse &&resp) {
        memory::MemoryCheckGuard guard;
        addStats(resp);
        return handlePropResp(std::move(resp));
      });
}

std::vector<Value> StorageAccessExecutor::handlePropResp(PropRpcResponse &&resps) {
  std::vector<Value> vertices;
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  if (!result.ok()) {
    LOG(WARNING) << "GetProp partial fail";
    return vertices;
  }
  nebula::DataSet v;
  for (auto &resp : resps.responses()) {
    if (resp.props_ref().has_value()) {
      if (UNLIKELY(!v.append(std::move(*resp.props_ref())))) {
        // it's impossible according to the interface
        LOG(WARNING) << "Heterogeneous props dataset";
      }
    } else {
      LOG(WARNING) << "GetProp partial success";
    }
  }
  auto val = std::make_shared<Value>(std::move(v));
  auto iter = std::make_unique<PropIter>(val);
  vertices.reserve(iter->size());
  for (; iter->valid(); iter->next()) {
    vertices.emplace_back(iter->getVertex());
  }
  return vertices;
}

void StorageAccessExecutor::addGetNeighborStats(RpcResponse &resp, size_t stepNum, bool reverse) {
  folly::dynamic stats = folly::dynamic::array();
  auto &hostLatency = resp.hostLatency();
  for (size_t i = 0; i < hostLatency.size(); ++i) {
    size_t size = 0u;
    auto &result = resp.responses()[i];
    if (result.vertices_ref().has_value()) {
      size = (*result.vertices_ref()).size();
    }
    auto info = util::collectRespProfileData(result.result, hostLatency[i], size);
    stats.push_back(std::move(info));
  }

  auto key = folly::sformat("{}step[{}]", reverse ? "reverse " : "", stepNum);
  std::lock_guard<folly::SpinLock> lk(statsLock_);
  otherStats_.emplace(key, folly::toPrettyJson(stats));
}

}  // namespace graph
}  // namespace nebula
