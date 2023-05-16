/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/util/OptimizerUtils.h"

#include "common/base/Status.h"
#include "common/datatypes/Value.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/IndexUtil.h"

using nebula::graph::ExpressionUtils;
using nebula::meta::cpp2::ColumnDef;
using nebula::meta::cpp2::IndexItem;
using nebula::storage::cpp2::IndexColumnHint;
using nebula::storage::cpp2::IndexQueryContext;

using FilterItem = nebula::graph::OptimizerUtils::FilterItem;
using IndexItemPtr = nebula::graph::OptimizerUtils::IndexItemPtr;
using IindexQueryContextList = nebula::graph::OptimizerUtils::IndexQueryContextList;
using ExprKind = nebula::Expression::Kind;

namespace nebula {
namespace graph {
namespace {

// IndexScore is used to find the optimal index. The larger the score, the
// better the index. When it is a score sequence, the length of the sequence
// should also be considered, such as: {2, 1, 0} > {2, 1} > {2, 0, 1} > {2, 0} >
// {2} > {1, 2} > {1, 1} > {1}
enum class IndexScore : uint8_t {
  kNotEqual = 0,
  kRange = 1,
  kPrefix = 2,
};

struct ScoredColumnHint {
  storage::cpp2::IndexColumnHint hint;
  IndexScore score;
};

struct IndexResult {
  const meta::cpp2::IndexItem* index;
  // expressions not used in all `ScoredColumnHint'
  std::vector<const Expression*> unusedExprs;
  std::vector<ScoredColumnHint> hints;

  bool operator<(const IndexResult& rhs) const {
    if (hints.empty()) return true;
    auto sz = std::min(hints.size(), rhs.hints.size());
    for (size_t i = 0; i < sz; i++) {
      if (hints[i].score < rhs.hints[i].score) {
        return true;
      }
      if (hints[i].score > rhs.hints[i].score) {
        return false;
      }
    }
    return hints.size() < rhs.hints.size();
  }
};

Status handleRangeIndex(const meta::cpp2::ColumnDef& field,
                        const Expression* expr,
                        const Value& value,
                        IndexColumnHint* hint) {
  if (field.get_type().get_type() == nebula::cpp2::PropertyType::BOOL) {
    return Status::Error("Range scan for bool type is illegal");
  }
  bool include = false;
  switch (expr->kind()) {
    case Expression::Kind::kRelGE:
      include = true;
      [[fallthrough]];
    case Expression::Kind::kRelGT:
      hint->begin_value_ref() = value;
      hint->include_begin_ref() = include;
      break;
    case Expression::Kind::kRelLE:
      include = true;
      [[fallthrough]];
    case Expression::Kind::kRelLT:
      hint->end_value_ref() = value;
      hint->include_end_ref() = include;
      break;
    default:
      break;
  }
  hint->scan_type_ref() = storage::cpp2::ScanType::RANGE;
  hint->column_name_ref() = field.get_name();
  return Status::OK();
}

void handleEqualIndex(const ColumnDef& field, const Value& value, IndexColumnHint* hint) {
  hint->scan_type_ref() = storage::cpp2::ScanType::PREFIX;
  hint->column_name_ref() = field.get_name();
  hint->begin_value_ref() = value;
}

StatusOr<ScoredColumnHint> selectRelExprIndex(const ColumnDef& field,
                                              const RelationalExpression* expr) {
  // TODO(yee): Reverse expression
  auto left = expr->left();
  DCHECK(left->kind() == Expression::Kind::kEdgeProperty ||
         left->kind() == Expression::Kind::kTagProperty);
  auto propExpr = static_cast<const PropertyExpression*>(left);
  if (propExpr->prop() != field.get_name()) {
    return Status::Error("Invalid field name.");
  }

  auto right = expr->right();

  // At this point all foldable expressions should already be folded.
  if (right->kind() != Expression::Kind::kConstant) {
    return Status::Error("The expression %s cannot be used to generate a index column hint",
                         right->toString().c_str());
  }
  DCHECK(right->kind() == Expression::Kind::kConstant);
  const auto& value = static_cast<const ConstantExpression*>(right)->value();

  ScoredColumnHint hint;
  switch (expr->kind()) {
    case Expression::Kind::kRelEQ: {
      handleEqualIndex(field, value, &hint.hint);
      hint.score = IndexScore::kPrefix;
      break;
    }
    case Expression::Kind::kRelGE:
    case Expression::Kind::kRelGT:
    case Expression::Kind::kRelLE:
    case Expression::Kind::kRelLT: {
      NG_RETURN_IF_ERROR(handleRangeIndex(field, expr, value, &hint.hint));
      hint.score = IndexScore::kRange;
      break;
    }
    case Expression::Kind::kRelNE: {
      hint.score = IndexScore::kNotEqual;
      break;
    }
    default: {
      return Status::Error("Invalid expression kind");
    }
  }
  return hint;
}

StatusOr<IndexResult> selectRelExprIndex(const RelationalExpression* expr, const IndexItem& index) {
  const auto& fields = index.get_fields();
  if (fields.empty()) {
    return Status::Error("Index(%s) does not have any fields.", index.get_index_name().c_str());
  }
  auto status = selectRelExprIndex(fields[0], expr);
  NG_RETURN_IF_ERROR(status);
  IndexResult result;
  result.hints.emplace_back(std::move(status).value());
  result.index = &index;
  return result;
}

bool mergeRangeColumnHints(const std::vector<ScoredColumnHint>& hints,
                           std::pair<Value, bool>* begin,
                           std::pair<Value, bool>* end) {
  for (auto& h : hints) {
    switch (h.score) {
      case IndexScore::kRange: {
        if (h.hint.begin_value_ref().is_set()) {
          auto tmp = std::make_pair(h.hint.get_begin_value(), h.hint.get_include_begin());
          if (begin->first.empty()) {
            *begin = std::move(tmp);
          } else {
            OptimizerUtils::compareAndSwapBound(tmp, *begin);
          }
        }
        if (h.hint.end_value_ref().is_set()) {
          auto tmp = std::make_pair(h.hint.get_end_value(), h.hint.get_include_end());
          if (end->first.empty()) {
            *end = std::move(tmp);
          } else {
            OptimizerUtils::compareAndSwapBound(*end, tmp);
          }
        }
        break;
      }
      case IndexScore::kPrefix: {
        auto tmp = std::make_pair(h.hint.get_begin_value(), true);
        if (begin->first.empty()) {
          *begin = tmp;
        } else {
          OptimizerUtils::compareAndSwapBound(tmp, *begin);
        }
        tmp = std::make_pair(h.hint.get_begin_value(), true);
        if (end->first.empty()) {
          *end = std::move(tmp);
        } else {
          OptimizerUtils::compareAndSwapBound(*end, tmp);
        }
        break;
      }
      case IndexScore::kNotEqual: {
        return false;
      }
    }
  }
  bool ret = true;
  if (begin->first > end->first) {
    ret = false;
  } else if (begin->first == end->first) {
    if (!(begin->second && end->second)) {
      ret = false;
    }
  }
  return ret;
}

bool getIndexColumnHintInExpr(const ColumnDef& field,
                              const LogicalExpression* expr,
                              ScoredColumnHint* hint,
                              std::vector<Expression*>* operands) {
  std::vector<ScoredColumnHint> hints;
  for (auto& operand : expr->operands()) {
    if (!operand->isRelExpr()) continue;
    auto relExpr = static_cast<const RelationalExpression*>(operand);
    auto status = selectRelExprIndex(field, relExpr);
    if (status.ok()) {
      hints.emplace_back(std::move(status).value());
      operands->emplace_back(operand);
    }
  }

  if (hints.empty()) return false;

  if (hints.size() == 1) {
    *hint = hints.front();
    return true;
  }
  std::pair<Value, bool> begin, end;
  if (!mergeRangeColumnHints(hints, &begin, &end)) {
    return false;
  }
  ScoredColumnHint h;
  h.hint.column_name_ref() = field.get_name();
  if (begin.first < end.first) {
    h.hint.scan_type_ref() = storage::cpp2::ScanType::RANGE;
    h.hint.begin_value_ref() = std::move(begin.first);
    h.hint.end_value_ref() = std::move(end.first);
    h.hint.include_begin_ref() = begin.second;
    h.hint.include_end_ref() = end.second;
    h.score = IndexScore::kRange;
  } else if (begin.first == end.first) {
    if (begin.second == false && end.second == false) {
      return false;
    } else {
      h.hint.scan_type_ref() = storage::cpp2::ScanType::RANGE;
      h.hint.begin_value_ref() = std::move(begin.first);
      h.hint.end_value_ref() = std::move(end.first);
      h.hint.include_begin_ref() = begin.second;
      h.hint.include_end_ref() = end.second;
      h.score = IndexScore::kRange;
    }
  } else {
    return false;
  }
  *hint = std::move(h);
  return true;
}

std::vector<const Expression*> collectUnusedExpr(
    const LogicalExpression* expr, const std::unordered_set<const Expression*>& usedOperands) {
  std::vector<const Expression*> unusedOperands;
  for (auto& operand : expr->operands()) {
    auto iter = std::find(usedOperands.begin(), usedOperands.end(), operand);
    if (iter == usedOperands.end()) {
      unusedOperands.emplace_back(operand);
    }
  }
  return unusedOperands;
}

StatusOr<IndexResult> selectLogicalAndExprIndex(const LogicalExpression* expr,
                                                const IndexItem& index) {
  if (expr->kind() != Expression::Kind::kLogicalAnd) {
    return Status::Error("Invalid expression kind.");
  }
  IndexResult result;
  result.hints.reserve(index.get_fields().size());
  std::unordered_set<const Expression*> usedOperands;
  for (auto& field : index.get_fields()) {
    ScoredColumnHint hint;
    std::vector<Expression*> operands;
    if (!getIndexColumnHintInExpr(field, expr, &hint, &operands)) {
      break;
    }
    result.hints.emplace_back(std::move(hint));
    for (auto op : operands) {
      usedOperands.insert(op);
    }
  }
  if (result.hints.empty()) {
    return Status::Error("There is not index to use.");
  }
  result.unusedExprs = collectUnusedExpr(expr, usedOperands);
  result.index = &index;
  return result;
}

StatusOr<IndexResult> selectIndex(const Expression* expr, const IndexItem& index) {
  if (expr->isRelExpr()) {
    return selectRelExprIndex(static_cast<const RelationalExpression*>(expr), index);
  }

  if (expr->kind() == Expression::Kind::kLogicalAnd) {
    return selectLogicalAndExprIndex(static_cast<const LogicalExpression*>(expr), index);
  }

  return Status::Error("Invalid expression kind.");
}

}  // namespace

void OptimizerUtils::eraseInvalidIndexItems(
    int32_t schemaId, std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>* indexItems) {
  // Erase invalid index items
  for (auto iter = indexItems->begin(); iter != indexItems->end();) {
    auto schema = (*iter)->get_schema_id();
    if (schema.tag_id_ref().has_value() && schema.get_tag_id() != schemaId) {
      iter = indexItems->erase(iter);
    } else if (schema.edge_type_ref().has_value() && schema.get_edge_type() != schemaId) {
      iter = indexItems->erase(iter);
    } else {
      iter++;
    }
  }
}

bool OptimizerUtils::findOptimalIndex(const Expression* condition,
                                      const std::vector<std::shared_ptr<IndexItem>>& indexItems,
                                      bool* isPrefixScan,
                                      IndexQueryContext* ictx) {
  // Return directly if there is no valid index to use.
  if (indexItems.empty()) {
    return false;
  }

  std::vector<IndexResult> results;
  for (auto& index : indexItems) {
    auto resStatus = selectIndex(condition, *index);
    if (resStatus.ok()) {
      results.emplace_back(std::move(resStatus).value());
    }
  }

  if (results.empty()) {
    return false;
  }

  std::sort(results.begin(), results.end());

  auto& index = results.back();
  if (index.hints.empty()) {
    return false;
  }

  *isPrefixScan = false;
  std::vector<storage::cpp2::IndexColumnHint> hints;
  hints.reserve(index.hints.size());
  auto iter = index.hints.begin();

  // Use full scan if the highest index score is NotEqual
  if (iter->score == IndexScore::kNotEqual) {
    return false;
  }

  for (; iter != index.hints.end(); ++iter) {
    auto& hint = *iter;
    if (hint.score == IndexScore::kPrefix) {
      hints.emplace_back(std::move(hint.hint));
      *isPrefixScan = true;
      continue;
    }
    if (hint.score == IndexScore::kRange) {
      hints.emplace_back(std::move(hint.hint));
      // skip the case first range hint is the last hint
      // when set filter in index query context
      ++iter;
    }
    break;
  }
  // The filter can always be pushed down for lookup query
  if (iter != index.hints.end() || !index.unusedExprs.empty()) {
    ictx->filter_ref() = condition->encode();
  }
  ictx->index_id_ref() = index.index->get_index_id();
  ictx->column_hints_ref() = std::move(hints);
  return true;
}

// Check if the relational expression has a valid index
// The left operand should either be a kEdgeProperty or kTagProperty expr
bool OptimizerUtils::relExprHasIndex(
    const Expression* expr,
    const std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>& indexItems) {
  DCHECK(expr->isRelExpr());

  for (auto& index : indexItems) {
    const auto& fields = index->get_fields();
    if (fields.empty()) {
      return false;
    }

    auto left = static_cast<const RelationalExpression*>(expr)->left();
    DCHECK(left->kind() == Expression::Kind::kEdgeProperty ||
           left->kind() == Expression::Kind::kTagProperty);

    auto propExpr = static_cast<const PropertyExpression*>(left);
    if (propExpr->prop() == fields[0].get_name()) {
      return true;
    }
  }

  return false;
}

void OptimizerUtils::copyIndexScanData(const nebula::graph::IndexScan* from,
                                       nebula::graph::IndexScan* to,
                                       QueryContext* qctx) {
  to->setSpace(from->space());
  to->setReturnCols(from->returnColumns());
  to->setIsEdge(from->isEdge());
  to->setSchemaId(from->schemaId());
  to->setDedup(from->dedup());
  to->setOrderBy(from->orderBy());
  to->setLimit(from->limit(qctx));
  to->setFilter(from->filter() == nullptr ? nullptr : from->filter()->clone());
  to->setYieldColumns(from->yieldColumns());
}

Status OptimizerUtils::compareAndSwapBound(std::pair<Value, bool>& a, std::pair<Value, bool>& b) {
  if (a.first > b.first) {
    std::swap(a, b);
  } else if (a.first < b.first) {  // do nothing
  } else if (a.second > b.second) {
    std::swap(a, b);
  }
  return Status::OK();
}

template <typename E, typename>
Status OptimizerUtils::addFilterItem(RelationalExpression* expr,
                                     std::vector<FilterItem>* items,
                                     QueryContext* qctx) {
  // TODO (sky) : Check illegal filter. for example : where c1 == 1 and c1 == 2
  Expression::Kind relType;
  if constexpr (std::is_same<E, EdgePropertyExpression>::value) {
    relType = Expression::Kind::kEdgeProperty;
  } else if constexpr (std::is_same<E, LabelTagPropertyExpression>::value) {
    relType = Expression::Kind::kLabelTagProperty;
  } else {
    relType = Expression::Kind::kTagProperty;
  }
  if (expr->left()->kind() == relType && ExpressionUtils::isEvaluableExpr(expr->right(), qctx)) {
    auto* l = static_cast<const E*>(expr->left());
    auto rValue = expr->right()->eval(graph::QueryExpressionContext(qctx->ectx())());
    items->emplace_back(l->prop(), expr->kind(), rValue);
  } else if (ExpressionUtils::isEvaluableExpr(expr->left(), qctx) &&
             expr->right()->kind() == relType) {
    auto* r = static_cast<const E*>(expr->right());
    auto lValue = expr->left()->eval(graph::QueryExpressionContext(qctx->ectx())());
    items->emplace_back(r->prop(), IndexUtil::reverseRelationalExprKind(expr->kind()), lValue);
  } else {
    return Status::Error("Optimizer error, when rewrite relational expression");
  }

  return Status::OK();
}

Status OptimizerUtils::analyzeExpression(Expression* expr,
                                         std::vector<FilterItem>* items,
                                         ScanKind* kind,
                                         bool isEdge,
                                         QueryContext* qctx) {
  // TODO (sky) : Currently only simple logical expressions are supported,
  //              such as all AND or all OR expressions, example :
  //              where c1 > 1 and c1 < 2 and c2 == 1
  //              where c1 == 1 or c2 == 1 or c3 == 1
  //
  //              Hybrid logical expressions are not supported yet, example :
  //              where c1 > 1 and c2 >1 or c3 > 1
  //              where c1 > 1 and c1 < 2 or c2 > 1
  //              where c1 < 1 and (c2 == 1 or c2 == 2)
  switch (expr->kind()) {
    case Expression::Kind::kLogicalOr:
    case Expression::Kind::kLogicalAnd: {
      auto lExpr = static_cast<LogicalExpression*>(expr);
      auto k = expr->kind() == Expression::Kind::kLogicalAnd ? ScanKind::Kind::kSingleScan
                                                             : ScanKind::Kind::kMultipleScan;
      if (kind->getKind() == ScanKind::Kind::kUnknown) {
        kind->setKind(k);
      } else if (kind->getKind() != k) {
        return Status::NotSupported("Condition not support yet : %s", expr->toString().c_str());
      }
      for (size_t i = 0; i < lExpr->operands().size(); ++i) {
        NG_RETURN_IF_ERROR(analyzeExpression(lExpr->operand(i), items, kind, isEdge, qctx));
      }
      break;
    }
    case Expression::Kind::kRelLE:
    case Expression::Kind::kRelGE:
    case Expression::Kind::kRelEQ:
    case Expression::Kind::kRelLT:
    case Expression::Kind::kRelGT:
    case Expression::Kind::kRelNE: {
      auto* rExpr = static_cast<RelationalExpression*>(expr);
      if (isEdge) {
        NG_RETURN_IF_ERROR(addFilterItem<EdgePropertyExpression>(rExpr, items, qctx));
      } else if (ExpressionUtils::hasAny(rExpr, {Expression::Kind::kLabelTagProperty})) {
        NG_RETURN_IF_ERROR(addFilterItem<LabelTagPropertyExpression>(rExpr, items, qctx));
      } else {
        NG_RETURN_IF_ERROR(addFilterItem<TagPropertyExpression>(rExpr, items, qctx));
      }
      if (kind->getKind() == ScanKind::Kind::kMultipleScan &&
          expr->kind() == Expression::Kind::kRelNE) {
        kind->setKind(ScanKind::Kind::kSingleScan);
        return Status::OK();
      }
      break;
    }
    default: {
      return Status::NotSupported("Filter not support yet : %s", expr->toString().c_str());
    }
  }
  return Status::OK();
}

std::vector<IndexItemPtr> OptimizerUtils::allIndexesBySchema(graph::QueryContext* qctx,
                                                             const IndexScan* node) {
  auto spaceId = node->space();
  auto isEdge = node->isEdge();
  auto metaClient = qctx->getMetaClient();
  auto ret = isEdge ? metaClient->getEdgeIndexesFromCache(spaceId)
                    : metaClient->getTagIndexesFromCache(spaceId);
  if (!ret.ok()) {
    LOG(ERROR) << "No index was found";
    return {};
  }
  std::vector<IndexItemPtr> indexes;
  for (auto& index : ret.value()) {
    const auto& schemaId = index->get_schema_id();
    // TODO (sky) : ignore rebuilding indexes
    auto id = isEdge ? schemaId.get_edge_type() : schemaId.get_tag_id();
    if (id == node->schemaId()) {
      indexes.emplace_back(index);
    }
  }
  if (indexes.empty()) {
    LOG(ERROR) << "No index was found";
    return {};
  }
  return indexes;
}

std::vector<IndexItemPtr> OptimizerUtils::findValidIndex(graph::QueryContext* qctx,
                                                         const IndexScan* node,
                                                         const std::vector<FilterItem>& items) {
  auto indexes = allIndexesBySchema(qctx, node);
  if (indexes.empty()) {
    return indexes;
  }
  std::vector<IndexItemPtr> validIndexes;
  // Find indexes for match all fields by where condition.
  for (const auto& index : indexes) {
    const auto& fields = index->get_fields();
    for (const auto& item : items) {
      auto it = std::find_if(fields.begin(), fields.end(), [item](const auto& field) {
        return field.get_name() == item.col_;
      });
      if (it != fields.end()) {
        validIndexes.emplace_back(index);
      }
    }
  }
  // If the first field of the index does not match any condition, the index is
  // invalid. remove it from validIndexes.
  if (!validIndexes.empty()) {
    auto index = validIndexes.begin();
    while (index != validIndexes.end()) {
      const auto& fields = index->get()->get_fields();
      auto it = std::find_if(items.begin(), items.end(), [fields](const auto& item) {
        return item.col_ == fields[0].get_name();
      });
      if (it == items.end()) {
        index = validIndexes.erase(index);
      } else {
        index++;
      }
    }
  }
  return validIndexes;
}

std::vector<IndexItemPtr> OptimizerUtils::findIndexForEqualScan(
    const std::vector<IndexItemPtr>& indexes, const std::vector<FilterItem>& items) {
  std::vector<std::pair<int32_t, IndexItemPtr>> eqIndexHint;
  for (auto& index : indexes) {
    int32_t hintCount = 0;
    for (const auto& field : index->get_fields()) {
      auto it = std::find_if(items.begin(), items.end(), [field](const auto& item) {
        return item.col_ == field.get_name();
      });
      if (it == items.end()) {
        break;
      }
      if (it->relOP_ == RelationalExpression::Kind::kRelEQ) {
        ++hintCount;
      } else {
        break;
      }
    }
    eqIndexHint.emplace_back(hintCount, index);
  }
  // Sort the priorityIdxs for equivalent condition.
  std::vector<IndexItemPtr> priorityIdxs;
  auto comp = [](std::pair<int32_t, IndexItemPtr>& lhs, std::pair<int32_t, IndexItemPtr>& rhs) {
    return lhs.first > rhs.first;
  };
  std::sort(eqIndexHint.begin(), eqIndexHint.end(), comp);
  // Get the index with the highest hit rate from eqIndexHint.
  int32_t maxHint = eqIndexHint[0].first;
  for (const auto& hint : eqIndexHint) {
    if (hint.first < maxHint) {
      break;
    }
    priorityIdxs.emplace_back(hint.second);
  }
  return priorityIdxs;
}

std::vector<IndexItemPtr> OptimizerUtils::findIndexForRangeScan(
    const std::vector<IndexItemPtr>& indexes, const std::vector<FilterItem>& items) {
  std::map<int32_t, IndexItemPtr> rangeIndexHint;
  for (const auto& index : indexes) {
    int32_t hintCount = 0;
    for (const auto& field : index->get_fields()) {
      auto fi = std::find_if(items.begin(), items.end(), [field](const auto& item) {
        return item.col_ == field.get_name();
      });
      if (fi == items.end()) {
        break;
      }
      if ((*fi).relOP_ == RelationalExpression::Kind::kRelEQ) {
        continue;
      }
      if ((*fi).relOP_ == RelationalExpression::Kind::kRelGE ||
          (*fi).relOP_ == RelationalExpression::Kind::kRelGT ||
          (*fi).relOP_ == RelationalExpression::Kind::kRelLE ||
          (*fi).relOP_ == RelationalExpression::Kind::kRelLT) {
        hintCount++;
      } else {
        break;
      }
    }
    rangeIndexHint[hintCount] = index;
  }
  std::vector<IndexItemPtr> priorityIdxs;
  int32_t maxHint = rangeIndexHint.rbegin()->first;
  for (auto iter = rangeIndexHint.rbegin(); iter != rangeIndexHint.rend(); iter++) {
    if (iter->first < maxHint) {
      break;
    }
    priorityIdxs.emplace_back(iter->second);
  }
  return priorityIdxs;
}

IndexItemPtr OptimizerUtils::findOptimalIndex(graph::QueryContext* qctx,
                                              const IndexScan* node,
                                              const std::vector<FilterItem>& items) {
  // The rule of priority is '==' --> '< > <= >=' --> '!='
  // Step 1 : find out all valid indexes for where condition.
  auto validIndexes = findValidIndex(qctx, node, items);
  if (validIndexes.empty()) {
    LOG(ERROR) << "No valid index found";
    return nullptr;
  }
  // Step 2 : find optimal indexes for equal condition.
  auto indexesEq = findIndexForEqualScan(validIndexes, items);
  if (indexesEq.size() == 1) {
    return indexesEq[0];
  }
  // Step 3 : find optimal indexes for range condition.
  auto indexesRange = findIndexForRangeScan(indexesEq, items);

  // At this stage, all the optimizations are done.
  // Because the storage layer only needs one. So return first one of
  // indexesRange.
  return indexesRange[0];
}

size_t OptimizerUtils::hintCount(const std::vector<FilterItem>& items) {
  std::unordered_set<std::string> hintCols;
  for (const auto& i : items) {
    hintCols.emplace(i.col_);
  }
  return hintCols.size();
}

bool OptimizerUtils::verifyType(const Value& val) {
  switch (val.type()) {
    case Value::Type::__EMPTY__:
    case Value::Type::NULLVALUE:
    case Value::Type::VERTEX:
    case Value::Type::EDGE:
    case Value::Type::LIST:
    case Value::Type::SET:
    case Value::Type::MAP:
    case Value::Type::DATASET:
    case Value::Type::DURATION:
    case Value::Type::GEOGRAPHY:  // TODO(jie)
    case Value::Type::PATH: {
      return false;
    }
    default: {
      return true;
    }
  }
}

Status OptimizerUtils::appendColHint(std::vector<IndexColumnHint>& hints,
                                     const std::vector<FilterItem>& items,
                                     const meta::cpp2::ColumnDef& col) {
  IndexColumnHint hint;
  std::pair<Value, bool> begin, end;
  bool isRangeScan = true;
  for (const auto& item : items) {
    if (!verifyType(item.value_)) {
      return Status::SemanticError(
          fmt::format("Not supported value type {} for index.", item.value_.type()));
    }
    if (item.relOP_ == Expression::Kind::kRelEQ) {
      isRangeScan = false;
      begin = {item.value_, true};
      break;
    }
    // because only type for bool is true/false, which can not satisfy [start,
    // end)
    if (col.get_type().get_type() == nebula::cpp2::PropertyType::BOOL) {
      return Status::SemanticError("Range scan for bool type is illegal");
    }
    if (item.value_.type() != graph::SchemaUtil::propTypeToValueType(col.type.type)) {
      return Status::SemanticError("Data type error of field : %s", col.get_name().c_str());
    }
    bool include = false;
    switch (item.relOP_) {
      case Expression::Kind::kRelLE:
        include = true;
        [[fallthrough]];
      case Expression::Kind::kRelLT: {
        if (end.first.empty()) {
          end.first = item.value_;
          end.second = include;
        } else {
          auto tmp = std::make_pair(item.value_, include);
          OptimizerUtils::compareAndSwapBound(end, tmp);
        }
      } break;
      case Expression::Kind::kRelGE:
        include = true;
        [[fallthrough]];
      case Expression::Kind::kRelGT: {
        if (begin.first.empty()) {
          begin.first = item.value_;
          begin.second = include;
        } else {
          auto tmp = std::make_pair(item.value_, include);
          OptimizerUtils::compareAndSwapBound(tmp, begin);
        }
      } break;
      default:
        return Status::Error("Invalid expression kind.");
    }
  }

  if (isRangeScan) {
    if (!begin.first.empty()) {
      hint.begin_value_ref() = begin.first;
      hint.include_begin_ref() = begin.second;
    }
    if (!end.first.empty()) {
      hint.end_value_ref() = end.first;
      hint.include_end_ref() = end.second;
    }
    hint.scan_type_ref() = storage::cpp2::ScanType::RANGE;
  } else {
    hint.begin_value_ref() = begin.first;
    hint.scan_type_ref() = storage::cpp2::ScanType::PREFIX;
  }
  hint.column_name_ref() = col.get_name();
  hints.emplace_back(std::move(hint));
  return Status::OK();
}

Status OptimizerUtils::appendIQCtx(const std::shared_ptr<IndexItem>& index,
                                   std::vector<IndexQueryContext>& iqctx) {
  IndexQueryContext ctx;
  ctx.index_id_ref() = index->get_index_id();
  ctx.filter_ref() = "";
  iqctx.emplace_back(std::move(ctx));
  return Status::OK();
}

Status OptimizerUtils::appendIQCtx(const IndexItemPtr& index,
                                   const std::vector<FilterItem>& items,
                                   std::vector<IndexQueryContext>& iqctx,
                                   const Expression* filter) {
  auto hc = hintCount(items);
  auto fields = index->get_fields();
  IndexQueryContext ctx;
  std::vector<nebula::storage::cpp2::IndexColumnHint> hints;
  for (const auto& field : fields) {
    bool found = false;
    std::vector<FilterItem> filterItems;
    for (const auto& item : items) {
      if (item.col_ == field.get_name()) {
        filterItems.emplace_back(item.col_, item.relOP_, item.value_);
        found = true;
      }
    }
    if (!found) break;
    auto it = std::find_if(filterItems.begin(), filterItems.end(), [](const auto& ite) {
      return ite.relOP_ == RelationalExpression::Kind::kRelNE;
    });
    if (it != filterItems.end()) {
      // TODO (sky) : rewrite filter expr. NE expr should be add filter expr .
      if (filter != nullptr) {
        ctx.filter_ref() = Expression::encode(*filter);
      }
      break;
    }
    NG_RETURN_IF_ERROR(appendColHint(hints, filterItems, field));
    hc--;
    if (filterItems.begin()->relOP_ != RelationalExpression::Kind::kRelEQ) {
      break;
    }
  }
  ctx.index_id_ref() = index->get_index_id();
  if (hc > 0) {
    // TODO (sky) : rewrite expr and set filter
    if (filter != nullptr) {
      ctx.filter_ref() = Expression::encode(*filter);
    }
  }
  ctx.column_hints_ref() = std::move(hints);
  iqctx.emplace_back(std::move(ctx));
  return Status::OK();
}

// Single ColumnHints item
Status OptimizerUtils::createSingleIQC(std::vector<IndexQueryContext>& iqctx,
                                       const std::vector<FilterItem>& items,
                                       graph::QueryContext* qctx,
                                       const IndexScan* node) {
  auto index = findOptimalIndex(qctx, node, items);
  if (index == nullptr) {
    return Status::IndexNotFound("No valid index found");
  }
  auto in = static_cast<const IndexScan*>(node);
  auto* filter = Expression::decode(qctx->objPool(), in->queryContext().begin()->get_filter());
  auto* newFilter = ExpressionUtils::rewriteParameter(filter, qctx);
  return appendIQCtx(index, items, iqctx, newFilter);
}

// Multiple ColumnHints items
Status OptimizerUtils::createMultipleIQC(std::vector<IndexQueryContext>& iqctx,
                                         const std::vector<FilterItem>& items,
                                         graph::QueryContext* qctx,
                                         const IndexScan* node) {
  for (auto const& item : items) {
    NG_RETURN_IF_ERROR(createSingleIQC(iqctx, {item}, qctx, node));
  }
  return Status::OK();
}

// Find the index with the fewest fields
// Only use "lookup on tagname"
IndexItemPtr OptimizerUtils::findLightestIndex(graph::QueryContext* qctx, const IndexScan* node) {
  auto indexes = allIndexesBySchema(qctx, node);
  if (indexes.empty()) {
    return nullptr;
  }

  auto result = indexes[0];
  for (size_t i = 1; i < indexes.size(); i++) {
    if (result->get_fields().size() > indexes[i]->get_fields().size()) {
      result = indexes[i];
    }
  }
  return result;
}

Status OptimizerUtils::createIndexQueryCtx(std::vector<IndexQueryContext>& iqctx,
                                           ScanKind kind,
                                           const std::vector<FilterItem>& items,
                                           graph::QueryContext* qctx,
                                           const IndexScan* node) {
  return kind.isSingleScan() ? createSingleIQC(iqctx, items, qctx, node)
                             : createMultipleIQC(iqctx, items, qctx, node);
}

Status OptimizerUtils::createIndexQueryCtx(Expression* filter,
                                           QueryContext* qctx,
                                           const IndexScan* node,
                                           std::vector<IndexQueryContext>& iqctx) {
  if (!filter) {
    // Only filter is nullptr when lookup on tagname
    // Degenerate back to tag lookup
    return createIndexQueryCtx(iqctx, qctx, node);
  }

  // items used for optimal index fetch and index scan context optimize.
  // for example : where c1 > 1 and c1 < 2 , the FilterItems should be :
  //               {c1, kRelGT, 1} , {c1, kRelLT, 2}
  std::vector<FilterItem> items;
  ScanKind kind;
  // rewrite ParameterExpression to ConstantExpression
  // TODO: refactor index selector logic to avoid this rewriting
  auto* newFilter = ExpressionUtils::rewriteParameter(filter, qctx);
  NG_RETURN_IF_ERROR(analyzeExpression(newFilter, &items, &kind, node->isEdge(), qctx));
  auto status = createIndexQueryCtx(iqctx, kind, items, qctx, node);
  if (!status.ok()) {
    return createIndexQueryCtx(iqctx, qctx, node);
  }
  return Status::OK();
}

Status OptimizerUtils::createIndexQueryCtx(std::vector<IndexQueryContext>& iqctx,
                                           graph::QueryContext* qctx,
                                           const IndexScan* node) {
  auto index = findLightestIndex(qctx, node);
  if (index == nullptr) {
    return Status::IndexNotFound("No valid index found");
  }
  return appendIQCtx(index, iqctx);
}

}  // namespace graph
}  // namespace nebula
