/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/LookupValidator.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"
#include "util/FTIndexUtils.h"

namespace nebula {
namespace graph {

/*static*/ constexpr char LookupValidator::kSrcVID[];
/*static*/ constexpr char LookupValidator::kDstVID[];
/*static*/ constexpr char LookupValidator::kRanking[];

/*static*/ constexpr char LookupValidator::kVertexID[];

Status LookupValidator::validateImpl() {
    NG_RETURN_IF_ERROR(prepareFrom());
    NG_RETURN_IF_ERROR(prepareYield());
    NG_RETURN_IF_ERROR(prepareFilter());
    return Status::OK();
}

Status LookupValidator::toPlan() {
    auto* is = IndexScan::make(qctx_,
                               nullptr,
                               spaceId_,
                               std::move(contexts_),
                               std::move(returnCols_),
                               isEdge_,
                               schemaId_,
                               isEmptyResultSet_);
    is->setColNames(std::move(idxScanColNames_));
    PlanNode* current = is;

    if (withProject_) {
        auto* projectNode = Project::make(qctx_, current, newYieldColumns_);
        projectNode->setInputVar(current->outputVar());
        projectNode->setColNames(colNames_);
        current = projectNode;
    }

    if (dedup_) {
        auto* dedupNode = Dedup::make(qctx_, current);
        dedupNode->setInputVar(current->outputVar());
        dedupNode->setColNames(colNames_);
        current = dedupNode;

        // the framework will add data collect to collect the result
        // if the result is required
    }

    root_ = current;
    tail_ = is;
    return Status::OK();
}

Status LookupValidator::prepareFrom() {
    auto* sentence = static_cast<const LookupSentence*>(sentence_);
    spaceId_ = vctx_->whichSpace().id;
    from_ = *sentence->from();
    auto ret = qctx_->schemaMng()->getSchemaIDByName(spaceId_, from_);
    if (!ret.ok()) {
        return ret.status();
    }
    isEdge_ = ret.value().first;
    schemaId_ = ret.value().second;
    return Status::OK();
}

Status LookupValidator::prepareYield() {
    auto* sentence = static_cast<const LookupSentence*>(sentence_);
    returnCols_ = std::make_unique<std::vector<std::string>>();
    // always return
    if (isEdge_) {
        returnCols_->emplace_back(kSrc);
        idxScanColNames_.emplace_back(kSrcVID);
        colNames_.emplace_back(idxScanColNames_.back());
        outputs_.emplace_back(colNames_.back(), vidType_);
        returnCols_->emplace_back(kDst);
        idxScanColNames_.emplace_back(kDstVID);
        colNames_.emplace_back(idxScanColNames_.back());
        outputs_.emplace_back(colNames_.back(), vidType_);
        returnCols_->emplace_back(kRank);
        idxScanColNames_.emplace_back(kRanking);
        colNames_.emplace_back(idxScanColNames_.back());
        outputs_.emplace_back(colNames_.back(), Value::Type::INT);
    } else {
        returnCols_->emplace_back(kVid);
        idxScanColNames_.emplace_back(kVertexID);
        colNames_.emplace_back(idxScanColNames_.back());
        outputs_.emplace_back(colNames_.back(), vidType_);
    }
    if (sentence->yieldClause() == nullptr) {
        return Status::OK();
    }
    withProject_ = true;
    if (sentence->yieldClause()->isDistinct()) {
        dedup_ = true;
    }
    newYieldColumns_ = qctx_->objPool()->makeAndAdd<YieldColumns>();
    if (isEdge_) {
        // default columns
        newYieldColumns_->addColumn(
            new YieldColumn(new InputPropertyExpression(kSrcVID), kSrcVID));
        newYieldColumns_->addColumn(
            new YieldColumn(new InputPropertyExpression(kDstVID), kDstVID));
        newYieldColumns_->addColumn(
            new YieldColumn(new InputPropertyExpression(kRanking), kRanking));
    } else {
        newYieldColumns_->addColumn(
            new YieldColumn(new InputPropertyExpression(kVertexID), kVertexID));
    }
    auto columns = sentence->yieldClause()->columns();
    auto schema = isEdge_ ? qctx_->schemaMng()->getEdgeSchema(spaceId_, schemaId_)
                          : qctx_->schemaMng()->getTagSchema(spaceId_, schemaId_);
    if (schema == nullptr) {
        return isEdge_ ? Status::EdgeNotFound("Edge schema not found : %s", from_.c_str())
                       : Status::TagNotFound("Tag schema not found : %s", from_.c_str());
    }
    for (auto col : columns) {
        // TODO(shylock) support more expr
        if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
            auto la = static_cast<LabelAttributeExpression*>(col->expr());
            const std::string& schemaName = la->left()->name();
            const auto& value = la->right()->value();
            const std::string& colName = value.getStr();
            if (isEdge_) {
                newYieldColumns_->addColumn(
                    new YieldColumn(new EdgePropertyExpression(schemaName, colName)));
            } else {
                newYieldColumns_->addColumn(
                    new YieldColumn(new TagPropertyExpression(schemaName, colName)));
            }
            if (!col->alias().empty()) {
                newYieldColumns_->back()->setAlias(col->alias());
            }
            if (schemaName != from_) {
                return Status::SemanticError("Schema name error : %s", schemaName.c_str());
            }
            auto ret = schema->getFieldType(colName);
            if (ret == meta::cpp2::PropertyType::UNKNOWN) {
                return Status::SemanticError(
                    "Column %s not found in schema %s", colName.c_str(), from_.c_str());
            }
            returnCols_->emplace_back(colName);
            idxScanColNames_.emplace_back(from_ + "." + colName);
            colNames_.emplace_back(deduceColName(newYieldColumns_->back()));
            outputs_.emplace_back(colNames_.back(), SchemaUtil::propTypeToValueType(ret));
        } else {
            return Status::SemanticError("Yield clauses are not supported : %s",
                                         col->expr()->toString().c_str());
        }
    }
    return Status::OK();
}

Status LookupValidator::prepareFilter() {
    auto* sentence = static_cast<const LookupSentence*>(sentence_);
    if (sentence->whereClause() == nullptr) {
        return Status::OK();
    }

    auto* filter = sentence->whereClause()->filter();
    storage::cpp2::IndexQueryContext ctx;
    if (FTIndexUtils::needTextSearch(filter)) {
        auto tsRet = FTIndexUtils::getTSClients(qctx_->getMetaClient());
        NG_RETURN_IF_ERROR(tsRet);
        tsClients_ = std::move(tsRet).value();
        auto tsIndex = checkTSExpr(filter);
        NG_RETURN_IF_ERROR(tsIndex);
        auto retFilter = FTIndexUtils::rewriteTSFilter(isEdge_,
                                                       filter,
                                                       tsIndex.value(),
                                                       tsClients_);
        if (!retFilter.ok()) {
            return retFilter.status();
        }
        if (retFilter.value().empty()) {
            // return empty result direct.
            isEmptyResultSet_ = true;
            return Status::OK();
        }
        ctx.set_filter(std::move(retFilter).value());
    } else {
        auto ret = checkFilter(filter);
        NG_RETURN_IF_ERROR(ret);
        ctx.set_filter(Expression::encode(*ret.value()));
    }
    contexts_ = std::make_unique<std::vector<storage::cpp2::IndexQueryContext>>();
    contexts_->emplace_back(std::move(ctx));
    return Status::OK();
}

StatusOr<Expression*> LookupValidator::checkFilter(Expression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalAnd: {
            // TODO(dutor) Deal with n-ary operands
            auto lExpr = static_cast<LogicalExpression*>(expr);
            auto& operands = lExpr->operands();
            for (auto i = 0u; i < operands.size(); i++) {
                auto ret = checkFilter(lExpr->operand(i));
                NG_RETURN_IF_ERROR(ret);
                lExpr->setOperand(i, ret.value()->clone().release());
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
            return checkRelExpr(rExpr);
        }
        default: {
            return Status::SemanticError("Expression %s not supported yet",
                                         expr->toString().c_str());
        }
    }
    return expr;
}

StatusOr<Expression*> LookupValidator::checkRelExpr(RelationalExpression* expr) {
    auto* left = expr->left();
    auto* right = expr->right();
    // Does not support filter : schema.col1 > schema.col2
    if (left->kind() == Expression::Kind::kLabelAttribute &&
        right->kind() == Expression::Kind::kLabelAttribute) {
        return Status::SemanticError("Expression %s not supported yet", expr->toString().c_str());
    } else if (left->kind() == Expression::Kind::kLabelAttribute ||
               right->kind() == Expression::Kind::kLabelAttribute) {
        auto ret = rewriteRelExpr(expr);
        NG_RETURN_IF_ERROR(ret);
        return ret.value();
    }
    return Status::SemanticError("Expression %s not supported yet", expr->toString().c_str());
}

StatusOr<Expression*> LookupValidator::rewriteRelExpr(RelationalExpression* expr) {
    // swap LHS and RHS of relExpr if LabelAttributeExpr in on the right,
    // so that LabelAttributeExpr is always on the left
    auto right = expr->right();
    if (right->kind() == Expression::Kind::kLabelAttribute) {
        expr = qctx_->objPool()->add(
            static_cast<RelationalExpression*>(reverseRelKind(expr).release()));
    }

    auto left = expr->left();
    auto* la = static_cast<LabelAttributeExpression*>(left);
    if (la->left()->name() != from_) {
        return Status::SemanticError("Schema name error : %s", la->left()->name().c_str());
    }

    // fold constant expression
    auto pool = qctx_->objPool();
    expr = static_cast<RelationalExpression*>(ExpressionUtils::foldConstantExpr(expr, pool));
    DCHECK_EQ(expr->left()->kind(), Expression::Kind::kLabelAttribute);

    std::string prop = la->right()->value().getStr();
    auto relExprType = expr->kind();
    auto c = checkConstExpr(expr->right(), prop, relExprType);
    if (!c.ok()) {
        return Status::SemanticError("expression error : %s", expr->right()->toString().c_str());
    }
    expr->setRight(new ConstantExpression(std::move(c).value()));

    // rewrite PropertyExpression
    if (isEdge_) {
        expr->setLeft(ExpressionUtils::rewriteLabelAttr2EdgeProp(la));
    } else {
        expr->setLeft(ExpressionUtils::rewriteLabelAttr2TagProp(la));
    }
    return expr;
}

StatusOr<Value> LookupValidator::checkConstExpr(Expression* expr,
                                                const std::string& prop,
                                                const Expression::Kind kind) {
    if (!evaluableExpr(expr)) {
        return Status::SemanticError("'%s' is not an evaluable expression.",
                                     expr->toString().c_str());
    }
    auto schema = isEdge_ ? qctx_->schemaMng()->getEdgeSchema(spaceId_, schemaId_)
                          : qctx_->schemaMng()->getTagSchema(spaceId_, schemaId_);
    auto type = schema->getFieldType(prop);
    QueryExpressionContext dummy(nullptr);
    auto v = Expression::eval(expr, dummy);
    // TODO(Aiee) extract the type cast logic as a method if we decide to support more cross-type
    // comparisons.

    // Allow different numeric type to compare
    if (graph::SchemaUtil::propTypeToValueType(type) == Value::Type::FLOAT && v.isInt()) {
        return v.toFloat();
    } else if (graph::SchemaUtil::propTypeToValueType(type) == Value::Type::INT && v.isFloat()) {
        // col1 < 10.5 range: [min, 11), col1 < 10 range: [min, 10)
        double f = v.getFloat();
        int iCeil = ceil(f);
        int iFloor = floor(f);
        if (kind == Expression::Kind::kRelGE || kind == Expression::Kind::kRelLT) {
            // edge case col1 >= 40.0, no need to round up
            if (std::abs(f - iCeil) < kEpsilon) {
                return iFloor;
            }
            return iCeil;
        }
        return iFloor;
    }

    if (v.type() != SchemaUtil::propTypeToValueType(type)) {
        return Status::SemanticError("Column type error : %s", prop.c_str());
    }
    return v;
}

StatusOr<std::string> LookupValidator::checkTSExpr(Expression* expr) {
    auto tsi = qctx_->getMetaClient()->getFTIndexBySpaceSchemaFromCache(spaceId_, schemaId_);
    if (!tsi.ok()) {
        return tsi.status();
    }
    auto tsExpr = static_cast<TextSearchExpression*>(expr);
    auto tsName = tsi.value().first;
    auto ret = FTIndexUtils::checkTSIndex(tsClients_, tsName);
    NG_RETURN_IF_ERROR(ret);
    if (!ret.value()) {
        return Status::SemanticError("text search index not found : %s", tsName.c_str());
    }
    auto ftFields = tsi.value().second.get_fields();
    auto prop = tsExpr->arg()->prop();

    auto iter = std::find(ftFields.begin(), ftFields.end(), prop);
    if (iter == ftFields.end()) {
        return Status::SemanticError("Column %s not found in %s", prop.c_str(), tsName.c_str());
    }
    return tsName;
}
// Transform (A > B) to (B < A)
std::unique_ptr<Expression> LookupValidator::reverseRelKind(RelationalExpression* expr) {
    auto kind = expr->kind();
    auto reversedKind = kind;

    switch (kind) {
        case Expression::Kind::kRelEQ:
            break;
        case Expression::Kind::kRelNE:
            break;
        case Expression::Kind::kRelLT:
            reversedKind = Expression::Kind::kRelGT;
            break;
        case Expression::Kind::kRelLE:
            reversedKind = Expression::Kind::kRelGE;
            break;
        case Expression::Kind::kRelGT:
            reversedKind = Expression::Kind::kRelLT;
            break;
        case Expression::Kind::kRelGE:
            reversedKind = Expression::Kind::kRelLE;
            break;
        default:
            LOG(FATAL) << "Invalid relational expression kind: " << static_cast<uint8_t>(kind);
            break;
    }

    auto left = expr->left();
    auto right = expr->right();

    return std::make_unique<RelationalExpression>(
        reversedKind, right->clone().release(), left->clone().release());
}
}   // namespace graph
}   // namespace nebula
