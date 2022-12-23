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
      DataSet edges({kSrc, kRank, kDst});
      for (auto& item : esResultValue.items) {
        edges.emplace_back(Row({item.src, item.rank, item.dst}));
      }
      finish(ResultBuilder().value(Value(std::move(edges))).iter(Iterator::Kind::kProp).build());
    } else {
      DataSet vertices({kVid});
      for (auto& item : esResultValue.items) {
        vertices.emplace_back(Row({item.vid}));
      }
      finish(ResultBuilder().value(Value(std::move(vertices))).iter(Iterator::Kind::kProp).build());
    }
  } else {
    if (ftIndexScan->isEdge()) {
      DataSet edges({kSrc, kRank, kDst});
      for (auto& item : esResultValue.items) {
        std::string srcStr = item.src;
        std::string dstStr = item.dst;
        int64_t src = *reinterpret_cast<int64_t*>(srcStr.data());
        int64_t dst = *reinterpret_cast<int64_t*>(dstStr.data());
        edges.emplace_back(Row({src, item.rank, dst}));
      }
      finish(ResultBuilder().value(Value(std::move(edges))).iter(Iterator::Kind::kProp).build());
    } else {
      DataSet vertices({kVid});
      for (auto& item : esResultValue.items) {
        std::string vidStr = item.vid;
        int64_t vid = *reinterpret_cast<int64_t*>(vidStr.data());
        vertices.emplace_back(Row({vid}));
      }
      finish(ResultBuilder().value(Value(std::move(vertices))).iter(Iterator::Kind::kProp).build());
    }
  }
  return Status::OK();
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
