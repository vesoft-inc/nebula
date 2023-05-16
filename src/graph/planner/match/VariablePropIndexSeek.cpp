/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/VariablePropIndexSeek.h"

#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::meta::cpp2::IndexItem;
using nebula::storage::cpp2::IndexQueryContext;

namespace nebula {
namespace graph {

bool VariablePropIndexSeek::matchNode(NodeContext* nodeCtx) {
  const auto& nodeInfo = *DCHECK_NOTNULL(nodeCtx->info);
  const auto& labels = nodeInfo.labels;
  if (labels.size() != 1 || labels.size() != nodeInfo.tids.size()) {
    // TODO multiple tag index seek need the IndexScan support
    VLOG(2) << "Multiple tag index seek is not supported now.";
    return false;
  }

  const std::string& label = labels.front();

  if (nodeInfo.alias.empty() || nodeInfo.anonymous) {
    return false;
  }

  auto whereClause = nodeCtx->bindWhereClause;
  if (!whereClause || !whereClause->filter) return false;

  auto filter = whereClause->filter;
  std::string refVarName, propName;
  std::shared_ptr<IndexItem> idxItem;
  Expression* indexFilter = nullptr;
  if (filter->kind() == Expression::Kind::kLogicalAnd) {
    auto logExpr = static_cast<const LogicalExpression*>(filter);
    bool found = false;
    for (auto op : logExpr->operands()) {
      if (getIndexItem(nodeCtx, op, label, nodeInfo.alias, &refVarName, &propName, &idxItem)) {
        // TODO(yee): Only select the first index as candidate filter expression and not support
        // the combined index
        indexFilter = TagPropertyExpression::make(nodeCtx->qctx->objPool(), label, propName);
        found = true;
        break;
      }
    }
    if (!found) return false;
  } else {
    if (!getIndexItem(nodeCtx, filter, label, nodeInfo.alias, &refVarName, &propName, &idxItem)) {
      return false;
    }
  }

  if (!nodeCtx->aliasesAvailable->count(refVarName)) {
    return false;
  }

  nodeCtx->refVarName = refVarName;

  nodeCtx->scanInfo.filter = indexFilter;
  nodeCtx->scanInfo.schemaIds = nodeInfo.tids;
  nodeCtx->scanInfo.schemaNames = nodeInfo.labels;

  return true;
}

StatusOr<SubPlan> VariablePropIndexSeek::transformNode(NodeContext* nodeCtx) {
  SubPlan plan;

  const auto& schemaIds = nodeCtx->scanInfo.schemaIds;
  DCHECK_EQ(schemaIds.size(), 1u) << "Not supported multiple tag properties seek.";

  auto qctx = nodeCtx->qctx;
  auto argument = Argument::make(qctx, nodeCtx->refVarName);
  argument->setColNames({nodeCtx->refVarName});
  IndexQueryContext iqctx;
  iqctx.filter_ref() = Expression::encode(*nodeCtx->scanInfo.filter);
  auto scan = IndexScan::make(
      qctx, argument, nodeCtx->spaceId, {std::move(iqctx)}, {kVid}, false, schemaIds.back());
  scan->setColNames({kVid});
  plan.tail = argument;
  plan.root = scan;

  // initialize start expression in project node
  auto* pool = nodeCtx->qctx->objPool();
  nodeCtx->initialExpr = VariablePropertyExpression::make(pool, "", kVid);
  return plan;
}

bool VariablePropIndexSeek::getIndexItem(const NodeContext* nodeCtx,
                                         const Expression* filter,
                                         const std::string& label,
                                         const std::string& alias,
                                         std::string* refVarName,
                                         std::string* propName,
                                         std::shared_ptr<IndexItem>* idxItem) {
  if (filter->kind() != Expression::Kind::kRelEQ && filter->kind() != Expression::Kind::kRelIn) {
    return false;
  }
  auto relInExpr = static_cast<const RelationalExpression*>(filter);
  auto right = relInExpr->right();
  if (right->kind() != Expression::Kind::kLabel) {
    // TODO(yee): swap the right and left side exprs when the left expr is label expr
    return false;
  }

  if (!MatchSolver::extractTagPropName(relInExpr->left(), label, alias, propName)) {
    return false;
  }
  // TODO(yee): workaround for index selection
  auto metaClient = nodeCtx->qctx->getMetaClient();
  auto status = metaClient->getTagIndexesFromCache(nodeCtx->spaceId);
  if (!status.ok()) return false;
  std::vector<std::shared_ptr<IndexItem>> idxItemList;
  for (auto itemPtr : status.value()) {
    auto schemaId = itemPtr->get_schema_id();
    if (schemaId.get_tag_id() == nodeCtx->info->tids.back()) {
      const auto& fields = itemPtr->get_fields();
      if (!fields.empty() && fields.front().get_name() == *propName) {
        idxItemList.push_back(itemPtr);
      }
    }
  }
  if (idxItemList.empty()) return false;
  std::sort(idxItemList.begin(), idxItemList.end(), [](auto rhs, auto lhs) {
    return rhs->get_fields().size() < lhs->get_fields().size();
  });
  *refVarName = static_cast<const LabelExpression*>(right)->name();
  *idxItem = idxItemList.front();
  return true;
}

}  // namespace graph
}  // namespace nebula
