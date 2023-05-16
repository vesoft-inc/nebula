// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/IndexScanExecutor.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/OptimizerUtils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::LookupIndexResp;

using IndexQueryContextList = nebula::graph::OptimizerUtils::IndexQueryContextList;

namespace nebula {
namespace graph {

folly::Future<Status> IndexScanExecutor::execute() {
  return indexScan();
}

folly::Future<Status> IndexScanExecutor::indexScan() {
  StorageClient *storageClient = qctx_->getStorageClient();
  auto *lookup = asNode<IndexScan>(node());
  auto objPool = qctx()->objPool();

  IndexQueryContextList ictxs;
  if (lookup->lazyIndexHint()) {
    auto filterStr = lookup->queryContext().front().get_filter();
    Expression *filter = Expression::decode(qctx()->objPool(), filterStr);
    if (filter->kind() != Expression::Kind::kRelEQ && filter->kind() != Expression::Kind::kRelIn) {
      return Status::Error("The kind of filter expression is invalid: %s",
                           filter->toString().c_str());
    }
    auto relFilter = static_cast<const RelationalExpression *>(filter);
    auto right = DCHECK_NOTNULL(relFilter->right());
    if (right->kind() != Expression::Kind::kLabel) {
      return Status::Error("The kind of expression is not label expression: %s",
                           right->toString().c_str());
    }
    const auto &colName = static_cast<const LabelExpression *>(right)->name();
    const auto &result = ectx_->getResult(lookup->inputVar());
    std::vector<Expression *> ops;
    std::unordered_set<Value> unique;
    for (auto iter = result.iterRef(); iter->valid(); iter->next()) {
      const auto &val = iter->getColumn(colName);
      if (!unique.emplace(val).second) continue;
      auto constExpr = ConstantExpression::make(objPool, val);
      auto leftExpr = relFilter->left()->clone();
      auto newRelExpr = RelationalExpression::makeEQ(objPool, leftExpr, constExpr);
      ops.push_back(newRelExpr);
    }

    if (ops.empty()) {
      return finish(ResultBuilder().value(Value(List())).iter(Iterator::Kind::kProp).build());
    }

    if (ops.size() == 1u) {
      NG_RETURN_IF_ERROR(OptimizerUtils::createIndexQueryCtx(ops[0], qctx(), lookup, ictxs));
    } else {
      auto logExpr = LogicalExpression::makeOr(objPool);
      logExpr->setOperands(std::move(ops));
      NG_RETURN_IF_ERROR(OptimizerUtils::createIndexQueryCtx(logExpr, qctx(), lookup, ictxs));
    }
  } else {
    ictxs = lookup->queryContext();
  }
  auto iter = std::find_if(
      ictxs.begin(), ictxs.end(), [](auto &ictx) { return !ictx.index_id_ref().is_set(); });
  if (ictxs.empty() || iter != ictxs.end()) {
    return Status::Error("There is no index to use at runtime");
  }

  StorageClient::CommonRequestParam param(lookup->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return storageClient
      ->lookupIndex(param,
                    ictxs,
                    lookup->isEdge(),
                    lookup->schemaId(),
                    lookup->returnColumns(),
                    lookup->orderBy(),
                    lookup->limit(qctx_))
      .via(runner())
      .thenValue([this](StorageRpcResponse<LookupIndexResp> &&rpcResp) {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        addStats(rpcResp);
        return handleResp(std::move(rpcResp));
      });
}

// TODO(shylock) merge the handler with GetProp
template <typename Resp>
Status IndexScanExecutor::handleResp(storage::StorageRpcResponse<Resp> &&rpcResp) {
  auto completeness = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  if (!completeness.ok()) {
    return std::move(completeness).status();
  }
  auto state = std::move(completeness).value();
  nebula::DataSet v;
  for (auto &resp : rpcResp.responses()) {
    if (resp.data_ref().has_value()) {
      nebula::DataSet &data = *resp.data_ref();
      // TODO: convert the column name to alias.
      if (v.colNames.empty()) {
        v.colNames = data.colNames;
      }
      v.rows.insert(v.rows.end(), data.rows.begin(), data.rows.end());
    } else {
      state = Result::State::kPartialSuccess;
    }
  }
  if (!node()->colNames().empty()) {
    DCHECK_EQ(node()->colNames().size(), v.colNames.size());
    v.colNames = node()->colNames();
  }
  return finish(
      ResultBuilder().value(std::move(v)).iter(Iterator::Kind::kProp).state(state).build());
}

}  // namespace graph
}  // namespace nebula
