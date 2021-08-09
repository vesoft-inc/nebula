/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/rule/IndexScanRule.h"
#include <vector>
#include "common/expression/LabelAttributeExpression.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "optimizer/OptRule.h"
#include "optimizer/OptimizerUtils.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/IndexUtil.h"

using nebula::graph::IndexScan;
using nebula::graph::IndexUtil;
using nebula::graph::IndexScan;
using nebula::graph::OptimizerUtils;

using IndexQueryCtx = std::vector<nebula::graph::IndexScan::IndexQueryContext>;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> IndexScanRule::kInstance =
    std::unique_ptr<IndexScanRule>(new IndexScanRule());

IndexScanRule::IndexScanRule() {
    RuleSet::DefaultRules().addRule(this);
}

const Pattern& IndexScanRule::pattern() const {
    static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kIndexScan);
    return pattern;
}

bool IndexScanRule::match(OptContext* ctx, const MatchedResult& matched) const {
    if (!OptRule::match(ctx, matched)) {
        return false;
    }
    auto scan = static_cast<const IndexScan*>(matched.planNode());
    // Has been optimized, skip this rule
    for (auto& ictx : scan->queryContext()) {
        if (ictx.index_id_ref().is_set()) {
            return false;
        }
    }
    return true;
}

StatusOr<OptRule::TransformResult> IndexScanRule::transform(OptContext* ctx,
                                                            const MatchedResult& matched) const {
    auto groupNode = matched.node;
    if (isEmptyResultSet(groupNode)) {
        return TransformResult::noTransform();
    }

    auto filter = filterExpr(groupNode);
    auto qctx = ctx->qctx();
    std::vector<IndexQueryContext> iqctx;
    if (filter == nullptr) {
        // Only filter is nullptr when lookup on tagname
        NG_RETURN_IF_ERROR(createIndexQueryCtx(iqctx, qctx, groupNode));
    } else {
        FilterItems items;
        ScanKind kind;
        NG_RETURN_IF_ERROR(analyzeExpression(filter, &items, &kind, isEdge(groupNode)));
        NG_RETURN_IF_ERROR(createIndexQueryCtx(iqctx, kind, items, qctx, groupNode));
    }

    const auto* oldIN = groupNode->node();
    DCHECK_EQ(oldIN->kind(), graph::PlanNode::Kind::kIndexScan);
    auto* newIN = static_cast<IndexScan*>(oldIN->clone());
    newIN->setIndexQueryContext(std::move(iqctx));
    auto newGroupNode = OptGroupNode::create(ctx, newIN, groupNode->group());
    if (groupNode->dependencies().size() != 1) {
        return Status::Error("Plan node dependencies error");
    }
    newGroupNode->dependsOn(groupNode->dependencies()[0]);
    TransformResult result;
    result.newGroupNodes.emplace_back(newGroupNode);
    result.eraseAll = true;
    return result;
}

std::string IndexScanRule::toString() const {
    return "IndexScanRule";
}

Status IndexScanRule::createIndexQueryCtx(IndexQueryCtx &iqctx,
                                          ScanKind kind,
                                          const FilterItems& items,
                                          graph::QueryContext *qctx,
                                          const OptGroupNode *groupNode) const {
    return kind.isSingleScan()
           ? createSingleIQC(iqctx, items, qctx, groupNode)
           : createMultipleIQC(iqctx, items, qctx, groupNode);
}

Status IndexScanRule::createIndexQueryCtx(IndexQueryCtx &iqctx,
                                          graph::QueryContext *qctx,
                                          const OptGroupNode *groupNode) const {
    auto index = findLightestIndex(qctx, groupNode);
    if (index == nullptr) {
        return Status::IndexNotFound("No valid index found");
    }
    auto ret = appendIQCtx(index, iqctx);
    NG_RETURN_IF_ERROR(ret);

    return Status::OK();
}


Status IndexScanRule::createSingleIQC(IndexQueryCtx &iqctx,
                                      const FilterItems& items,
                                      graph::QueryContext *qctx,
                                      const OptGroupNode *groupNode) const {
    auto index = findOptimalIndex(qctx, groupNode, items);
    if (index == nullptr) {
        return Status::IndexNotFound("No valid index found");
    }
    auto in = static_cast<const IndexScan *>(groupNode->node());
    const auto& filter = in->queryContext().begin()->get_filter();
    return appendIQCtx(index, items, iqctx, filter);
}

Status IndexScanRule::createMultipleIQC(IndexQueryCtx &iqctx,
                                        const FilterItems& items,
                                        graph::QueryContext *qctx,
                                        const OptGroupNode *groupNode) const {
    for (auto const& item : items.items) {
        auto ret = createSingleIQC(iqctx, FilterItems({item}), qctx, groupNode);
        NG_RETURN_IF_ERROR(ret);
    }
    return Status::OK();
}

size_t IndexScanRule::hintCount(const FilterItems& items) const noexcept {
    std::unordered_set<std::string> hintCols;
    for (const auto& i : items.items) {
        hintCols.emplace(i.col_);
    }
    return hintCols.size();
}

Status IndexScanRule::appendIQCtx(const IndexItem& index,
                                  const FilterItems& items,
                                  IndexQueryCtx &iqctx,
                                  const std::string& filter) const {
    auto hc = hintCount(items);
    auto fields = index->get_fields();
    IndexQueryContext ctx;
    std::vector<nebula::storage::cpp2::IndexColumnHint> hints;
    for (const auto& field : fields) {
        bool found = false;
        FilterItems filterItems;
        for (const auto& item : items.items) {
            if (item.col_ != field.get_name()) {
                continue;
            }
            filterItems.addItem(item.col_, item.relOP_, item.value_);
            found = true;
        }
        if (!found) break;
        auto it = std::find_if(filterItems.items.begin(), filterItems.items.end(),
                               [](const auto &ite) {
                                   return ite.relOP_ == RelationalExpression::Kind::kRelNE;
                               });
        if (it != filterItems.items.end()) {
            // TODO (sky) : rewrite filter expr. NE expr should be add filter expr .
            ctx.set_filter(filter);
            break;
        }
        NG_RETURN_IF_ERROR(appendColHint(hints, filterItems, field));
        hc--;
        if (filterItems.items.begin()->relOP_ != RelationalExpression::Kind::kRelEQ) {
            break;
        }
    }
    ctx.set_index_id(index->get_index_id());
    if (hc > 0) {
        // TODO (sky) : rewrite expr and set filter
        ctx.set_filter(filter);
    }
    ctx.set_column_hints(std::move(hints));
    iqctx.emplace_back(std::move(ctx));
    return Status::OK();
}

Status IndexScanRule::appendIQCtx(const IndexItem& index,
                                  IndexQueryCtx &iqctx) const {
    IndexQueryContext ctx;
    ctx.set_index_id(index->get_index_id());
    ctx.set_filter("");
    iqctx.emplace_back(std::move(ctx));
    return Status::OK();
}

#define CHECK_BOUND_VALUE(v, name)                                                                 \
    do {                                                                                           \
        if (v == Value::kNullBadType) {                                                            \
            LOG(ERROR) << "Get bound value error. field : "  << name;                              \
            return Status::Error("Get bound value error. field : %s", name.c_str());               \
        }                                                                                          \
    } while (0)

Status IndexScanRule::appendColHint(std::vector<IndexColumnHint>& hints,
                                    const FilterItems& items,
                                    const meta::cpp2::ColumnDef& col) const {
    IndexColumnHint hint;
    Value begin, end;
    bool isRangeScan = true;
    for (const auto& item : items.items) {
        if (item.relOP_ == Expression::Kind::kRelEQ) {
            // check the items, don't allow where c1 == 1 and c1 == 2 and c1 > 3....
            // If EQ item appears, only one element is allowed
            if (items.items.size() > 1) {
                return Status::SemanticError();
            }
            isRangeScan = false;
            begin = OptimizerUtils::normalizeValue(col, item.value_);
            break;
        }
        // because only type for bool is true/false, which can not satisify [start, end)
        if (col.get_type().get_type() == meta::cpp2::PropertyType::BOOL) {
            return Status::SemanticError("Range scan for bool type is illegal");
        }
        NG_RETURN_IF_ERROR(OptimizerUtils::boundValue(item.relOP_, item.value_, col, begin, end));
    }

    if (isRangeScan) {
        if (begin.empty()) {
            begin = OptimizerUtils::boundValue(col, BVO::MIN, Value());
            CHECK_BOUND_VALUE(begin, col.get_name());
        }
        if (end.empty()) {
            end = OptimizerUtils::boundValue(col, BVO::MAX, Value());
            CHECK_BOUND_VALUE(end, col.get_name());
        }
        hint.set_scan_type(storage::cpp2::ScanType::RANGE);
        hint.set_end_value(std::move(end));
    } else {
        hint.set_scan_type(storage::cpp2::ScanType::PREFIX);
    }
    hint.set_begin_value(std::move(begin));
    hint.set_column_name(col.get_name());
    hints.emplace_back(std::move(hint));
    return Status::OK();
}

bool IndexScanRule::isEdge(const OptGroupNode *groupNode) const {
    auto in = static_cast<const IndexScan *>(groupNode->node());
    return in->isEdge();
}

int32_t IndexScanRule::schemaId(const OptGroupNode *groupNode) const {
    auto in = static_cast<const IndexScan *>(groupNode->node());
    return in->schemaId();
}

GraphSpaceID IndexScanRule::spaceId(const OptGroupNode *groupNode) const {
    auto in = static_cast<const IndexScan *>(groupNode->node());
    return in->space();
}

Expression* IndexScanRule::filterExpr(const OptGroupNode* groupNode) const {
    auto in = static_cast<const IndexScan*>(groupNode->node());
    const auto& qct = in->queryContext();
    // The initial IndexScan plan node has only zero or one queryContext.
    // TODO(yee): Move this condition to match interface
    if (qct.empty()) {
        return nullptr;
    }

    if (qct.size() != 1) {
        LOG(ERROR) << "Index Scan plan node error";
        return nullptr;
    }
    auto* pool = in->qctx()->objPool();
    return Expression::decode(pool, qct.begin()->get_filter());
}

Status IndexScanRule::analyzeExpression(Expression* expr,
                                        FilterItems* items,
                                        ScanKind* kind,
                                        bool isEdge) const {
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
        case Expression::Kind::kLogicalOr :
        case Expression::Kind::kLogicalAnd : {
            auto lExpr = static_cast<LogicalExpression*>(expr);
            auto k = expr->kind() == Expression::Kind::kLogicalAnd
                     ? ScanKind::Kind::kSingleScan : ScanKind::Kind::kMultipleScan;
            if (kind->getKind() == ScanKind::Kind::kUnknown) {
                kind->setKind(k);
            } else if (kind->getKind() != k) {
                return Status::NotSupported("Condition not support yet : %s",
                                            expr->toString().c_str());
            }
            for (size_t i = 0; i < lExpr->operands().size(); ++i) {
                NG_RETURN_IF_ERROR(analyzeExpression(lExpr->operand(i), items, kind, isEdge));
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
            auto ret = isEdge
                       ? addFilterItem<EdgePropertyExpression>(rExpr, items)
                       : addFilterItem<TagPropertyExpression>(rExpr, items);
            NG_RETURN_IF_ERROR(ret);
            if (kind->getKind() == ScanKind::Kind::kMultipleScan &&
                expr->kind() == Expression::Kind::kRelNE) {
                kind->setKind(ScanKind::Kind::kSingleScan);
                return Status::OK();
            }
            break;
        }
        default: {
            return Status::NotSupported("Filter not support yet : %s",
                                        expr->toString().c_str());
        }
    }
    return Status::OK();
}

template <typename E, typename>
Status IndexScanRule::addFilterItem(RelationalExpression* expr, FilterItems* items) const {
    // TODO (sky) : Check illegal filter. for example : where c1 == 1 and c1 == 2
    auto relType = std::is_same<E, EdgePropertyExpression>::value
                   ? Expression::Kind::kEdgeProperty
                   : Expression::Kind::kTagProperty;
    if (expr->left()->kind() == relType &&
        expr->right()->kind() == Expression::Kind::kConstant) {
        auto* l = static_cast<const E*>(expr->left());
        auto* r = static_cast<ConstantExpression*>(expr->right());
        items->addItem(l->prop(), expr->kind(), r->value());
    } else if (expr->left()->kind() == Expression::Kind::kConstant &&
               expr->right()->kind() == relType) {
        auto* r = static_cast<const E*>(expr->right());
        auto* l = static_cast<ConstantExpression*>(expr->left());
        items->addItem(r->prop(), IndexUtil::reverseRelationalExprKind(expr->kind()), l->value());
    } else {
        return Status::Error("Optimizer error, when rewrite relational expression");
    }

    return Status::OK();
}

IndexItem IndexScanRule::findOptimalIndex(graph::QueryContext *qctx,
                                          const OptGroupNode *groupNode,
                                          const FilterItems& items) const {
    // The rule of priority is '==' --> '< > <= >=' --> '!='
    // Step 1 : find out all valid indexes for where condition.
    auto validIndexes = findValidIndex(qctx, groupNode, items);
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
    // Because the storage layer only needs one. So return first one of indexesRange.
    return indexesRange[0];
}

// Find the index with the fewest fields
// Only use "lookup on tagname"
IndexItem IndexScanRule::findLightestIndex(graph::QueryContext *qctx,
                                           const OptGroupNode *groupNode) const {
    auto indexes = allIndexesBySchema(qctx, groupNode);
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

std::vector<IndexItem>
IndexScanRule::allIndexesBySchema(graph::QueryContext *qctx,
                                  const OptGroupNode *groupNode) const {
    auto ret = isEdge(groupNode)
               ? qctx->getMetaClient()->getEdgeIndexesFromCache(spaceId(groupNode))
               : qctx->getMetaClient()->getTagIndexesFromCache(spaceId(groupNode));
    if (!ret.ok()) {
        LOG(ERROR) << "No index was found";
        return {};
    }
    std::vector<IndexItem> indexes;
    for (auto& index : ret.value()) {
        // TODO (sky) : ignore rebuilding indexes
        auto id = isEdge(groupNode)
                  ? index->get_schema_id().get_edge_type()
                  : index->get_schema_id().get_tag_id();
        if (id == schemaId(groupNode)) {
            indexes.emplace_back(index);
        }
    }
    if (indexes.empty()) {
        LOG(ERROR) << "No index was found";
        return {};
    }
    return indexes;
}

std::vector<IndexItem>
IndexScanRule::findValidIndex(graph::QueryContext *qctx,
                              const OptGroupNode *groupNode,
                              const FilterItems& items) const {
    auto indexes = allIndexesBySchema(qctx, groupNode);
    if (indexes.empty()) {
        return indexes;
    }
    std::vector<IndexItem> validIndexes;
    // Find indexes for match all fields by where condition.
    for (const auto& index : indexes) {
        bool allColsHint = true;
        const auto& fields = index->get_fields();
        for (const auto& item : items.items) {
            auto it = std::find_if(fields.begin(), fields.end(),
                                   [item](const auto &field) {
                                       return field.get_name() == item.col_;
                                   });
            if (it == fields.end()) {
                allColsHint = false;
                break;
            }
        }
        if (allColsHint) {
            validIndexes.emplace_back(index);
        }
    }
    // If the first field of the index does not match any condition, the index is invalid.
    // remove it from validIndexes.
    if (!validIndexes.empty()) {
        auto index = validIndexes.begin();
        while (index != validIndexes.end()) {
            const auto& fields = index->get()->get_fields();
            auto it = std::find_if(items.items.begin(), items.items.end(),
                                   [fields](const auto &item) {
                                       return item.col_ == fields[0].get_name();
                                   });
            if (it == items.items.end()) {
                validIndexes.erase(index);
            } else {
                index++;
            }
        }
    }
    return validIndexes;
}

std::vector<IndexItem>
IndexScanRule::findIndexForEqualScan(const std::vector<IndexItem>& indexes,
                                     const FilterItems& items) const {
    std::vector<std::pair<int32_t, IndexItem>> eqIndexHint;
    for (auto& index : indexes) {
        int32_t hintCount = 0;
        for (const auto& field : index->get_fields()) {
            auto it = std::find_if(items.items.begin(), items.items.end(),
                                   [field](const auto &item) {
                                       return item.col_ == field.get_name();
                                   });
            if (it == items.items.end()) {
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
    std::vector<IndexItem> priorityIdxs;
    auto comp = [] (std::pair<int32_t, IndexItem>& lhs,
                    std::pair<int32_t, IndexItem>& rhs) {
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

std::vector<IndexItem>
IndexScanRule::findIndexForRangeScan(const std::vector<IndexItem>& indexes,
                                     const FilterItems& items) const {
    std::map<int32_t, IndexItem> rangeIndexHint;
    for (const auto& index : indexes) {
        int32_t hintCount = 0;
        for (const auto& field : index->get_fields()) {
            auto fi = std::find_if(items.items.begin(), items.items.end(),
                                   [field](const auto &item) {
                                       return item.col_ == field.get_name();
                                   });
            if (fi == items.items.end()) {
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
    std::vector<IndexItem> priorityIdxs;
    int32_t maxHint = rangeIndexHint.rbegin()->first;
    for (auto iter = rangeIndexHint.rbegin(); iter != rangeIndexHint.rend(); iter++) {
        if (iter->first < maxHint) {
            break;
        }
        priorityIdxs.emplace_back(iter->second);
    }
    return priorityIdxs;
}

bool IndexScanRule::isEmptyResultSet(const OptGroupNode *groupNode) const {
    auto in = static_cast<const IndexScan *>(groupNode->node());
    return in->isEmptyResultSet();
}
}   // namespace opt
}   // namespace nebula
