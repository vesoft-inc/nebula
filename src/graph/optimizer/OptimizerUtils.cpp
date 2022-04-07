/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/OptimizerUtils.h"

#include "common/base/Status.h"
#include "common/datatypes/Value.h"
#include "graph/planner/plan/Query.h"

using nebula::meta::cpp2::ColumnDef;
using nebula::meta::cpp2::IndexItem;
using nebula::storage::cpp2::IndexColumnHint;
using nebula::storage::cpp2::IndexQueryContext;

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

StatusOr<IndexResult> selectLogicalExprIndex(const LogicalExpression* expr,
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

  if (expr->isLogicalExpr()) {
    return selectLogicalExprIndex(static_cast<const LogicalExpression*>(expr), index);
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
  to->setEmptyResultSet(from->isEmptyResultSet());
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

}  // namespace graph
}  // namespace nebula
