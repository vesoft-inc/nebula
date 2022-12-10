// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/FulltextIndexScanExecutor.h"

#include "common/datatypes/DataSet.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/FTIndexUtils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula::graph {

folly::Future<Status> FulltextIndexScanExecutor::execute() {
  auto esAdapterResult = FTIndexUtils::getESAdapter(qctx_->getMetaClient());
  if (!esAdapterResult.ok()) {
    return esAdapterResult.status();
  }
  esAdapter_ = std::move(esAdapterResult).value();
  auto* ftIndexScan = asNode<FulltextIndexScan>(node());
  auto esQueryResult = accessFulltextIndex(ftIndexScan->index(), ftIndexScan->searchExpression());
  if (!esQueryResult.ok()) {
    LOG(ERROR) << esQueryResult.status().message();
    return esQueryResult.status();
  }
  auto esResultValue = std::move(esQueryResult).value();
  const auto& space = qctx()->rctx()->session()->space();
  if (!isIntVidType(space)) {
    if (ftIndexScan->isEdge()) {
      DataSet edges({kSrc, kType, kRank, kDst});
      for (auto& item : esResultValue.items) {
        edges.emplace_back(Row({item.src, ftIndexScan->schemaId(), item.rank, item.dst}));
      }
      return getEdges(ftIndexScan, std::move(edges));
    } else {
      DataSet vertices({kVid});
      for (auto& item : esResultValue.items) {
        vertices.emplace_back(Row({item.vid}));
      }
      return getTags(ftIndexScan, std::move(vertices));
    }
  } else {
    if (ftIndexScan->isEdge()) {
      DataSet edges({kSrc, kType, kRank, kDst});
      for (auto& item : esResultValue.items) {
        std::string srcStr = item.src;
        std::string dstStr = item.src;
        int64_t src = *reinterpret_cast<int64_t*>(srcStr.data());
        int64_t dst = *reinterpret_cast<int64_t*>(dstStr.data());
        edges.emplace_back(Row({src, ftIndexScan->schemaId(), item.rank, dst}));
      }
      return getEdges(ftIndexScan, std::move(edges));
    } else {
      DataSet vertices({kVid});
      for (auto& item : esResultValue.items) {
        std::string vidStr = item.vid;
        int64_t vid = *reinterpret_cast<int64_t*>(vidStr.data());
        vertices.emplace_back(Row({vid}));
      }
      return getTags(ftIndexScan, std::move(vertices));
    }
  }
}

folly::Future<Status> FulltextIndexScanExecutor::getTags(const FulltextIndexScan* scan,
                                                         DataSet&& vertices) {
  SCOPED_TIMER(&execTime_);
  StorageClient* storageClient = qctx()->getStorageClient();
  if (vertices.rows.empty()) {
    return finish(ResultBuilder()
                      .value(Value(DataSet(scan->colNames())))
                      .iter(Iterator::Kind::kProp)
                      .build());
  }
  time::Duration getPropsTime;
  StorageClient::CommonRequestParam param(scan->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());

  return DCHECK_NOTNULL(storageClient)
      ->getProps(param,
                 std::move(vertices),
                 scan->vertexProps(),
                 nullptr,
                 nullptr,
                 scan->dedup(),
                 scan->orderBy(),
                 scan->limit(qctx()),
                 scan->filter())
      .via(runner())
      .ensure([this, getPropsTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc", folly::sformat("{}(us)", getPropsTime.elapsedInUSec()));
      })
      .thenValue([this, scan](StorageRpcResponse<GetPropResponse>&& rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp), scan->colNames());
      });
}
folly::Future<Status> FulltextIndexScanExecutor::getEdges(const FulltextIndexScan* scan,
                                                          DataSet&& edges) {
  SCOPED_TIMER(&execTime_);
  StorageClient* client = qctx()->getStorageClient();
  time::Duration getPropsTime;
  StorageClient::CommonRequestParam param(scan->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return DCHECK_NOTNULL(client)
      ->getProps(param,
                 std::move(edges),
                 nullptr,
                 scan->edgeProps(),
                 nullptr,
                 scan->dedup(),
                 scan->orderBy(),
                 scan->limit(qctx()),
                 scan->filter())
      .via(runner())
      .ensure([this, getPropsTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc", folly::sformat("{}(us)", getPropsTime.elapsedInUSec()));
      })
      .thenValue([this, scan](StorageRpcResponse<GetPropResponse>&& rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp), scan->colNames());
      });
}

StatusOr<plugin::ESQueryResult> FulltextIndexScanExecutor::accessFulltextIndex(
    const std::string& index, TextSearchExpression* tsExpr) {
  std::function<StatusOr<nebula::plugin::ESQueryResult>()> execFunc;
  plugin::ESAdapter& esAdapter = esAdapter_;
  switch (tsExpr->kind()) {
    case Expression::Kind::kTSFuzzy: {
      std::string pattern = tsExpr->arg()->val();
      int fuzziness = tsExpr->arg()->fuzziness();
      int64_t size = tsExpr->arg()->limit();
      int64_t timeout = tsExpr->arg()->timeout();
      execFunc = [&index, pattern, &esAdapter, fuzziness, size, timeout]() {
        return esAdapter.fuzzy(
            index, pattern, fuzziness < 0 ? "AUTO" : std::to_string(fuzziness), size, timeout);
      };
      break;
    }
    case Expression::Kind::kTSPrefix: {
      std::string pattern = tsExpr->arg()->val();
      int64_t size = tsExpr->arg()->limit();
      int64_t timeout = tsExpr->arg()->timeout();
      execFunc = [&index, pattern, &esAdapter, size, timeout]() {
        return esAdapter.prefix(index, pattern, size, timeout);
      };
      break;
    }
    case Expression::Kind::kTSRegexp: {
      std::string pattern = tsExpr->arg()->val();
      int64_t size = tsExpr->arg()->limit();
      int64_t timeout = tsExpr->arg()->timeout();
      execFunc = [&index, pattern, &esAdapter, size, timeout]() {
        return esAdapter.regexp(index, pattern, size, timeout);
      };
      break;
    }
    case Expression::Kind::kTSWildcard: {
      std::string pattern = tsExpr->arg()->val();
      int64_t size = tsExpr->arg()->limit();
      int64_t timeout = tsExpr->arg()->timeout();
      execFunc = [&index, pattern, &esAdapter, size, timeout]() {
        return esAdapter.wildcard(index, pattern, size, timeout);
      };
      break;
    }
    default: {
      return Status::SemanticError("text search expression error");
    }
  }

  // auto retryCnt = FLAGS_ft_request_retry_times > 0 ? FLAGS_ft_request_retry_times : 1;
  // StatusOr<nebula::plugin::ESQueryResult> result;
  // while (retryCnt-- > 0) {
  //   result = execFunc();
  //   if (!result.ok()) {
  //     continue;
  //   }
  //   break;
  // }

  return execFunc();
}

}  // namespace nebula::graph
