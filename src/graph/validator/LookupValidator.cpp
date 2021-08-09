/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/LookupValidator.h"

#include "common/base/Status.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "context/ast/QueryAstContext.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"
#include "util/FTIndexUtils.h"
#include "util/SchemaUtil.h"

using nebula::meta::NebulaSchemaProvider;
using std::shared_ptr;
using std::unique_ptr;

namespace nebula {
namespace graph {

LookupValidator::LookupValidator(Sentence* sentence, QueryContext* context)
    : Validator(sentence, context) {}

const LookupSentence* LookupValidator::sentence() const {
    return static_cast<LookupSentence*>(sentence_);
}

int32_t LookupValidator::schemaId() const {
    return DCHECK_NOTNULL(lookupCtx_)->schemaId;
}

GraphSpaceID LookupValidator::spaceId() const {
    return DCHECK_NOTNULL(lookupCtx_)->space.id;
}

AstContext* LookupValidator::getAstContext() {
    return lookupCtx_.get();
}

Status LookupValidator::validateImpl() {
    lookupCtx_ = getContext<LookupContext>();

    NG_RETURN_IF_ERROR(prepareFrom());
    NG_RETURN_IF_ERROR(prepareYield());
    NG_RETURN_IF_ERROR(prepareFilter());
    return Status::OK();
}

Status LookupValidator::prepareFrom() {
    auto spaceId = lookupCtx_->space.id;
    auto from = sentence()->from();
    auto ret = qctx_->schemaMng()->getSchemaIDByName(spaceId, from);
    NG_RETURN_IF_ERROR(ret);
    lookupCtx_->isEdge = ret.value().first;
    lookupCtx_->schemaId = ret.value().second;
    return Status::OK();
}

Status LookupValidator::prepareYield() {
    if (lookupCtx_->isEdge) {
        outputs_.emplace_back(kSrcVID, vidType_);
        outputs_.emplace_back(kDstVID, vidType_);
        outputs_.emplace_back(kRanking, Value::Type::INT);
    } else {
        outputs_.emplace_back(kVertexID, vidType_);
    }

    auto yieldClause = sentence()->yieldClause();
    if (yieldClause == nullptr) {
        return Status::OK();
    }
    lookupCtx_->dedup = yieldClause->isDistinct();

    shared_ptr<const NebulaSchemaProvider> schemaProvider;
    NG_RETURN_IF_ERROR(getSchemaProvider(&schemaProvider));

    auto from = sentence()->from();
    for (auto col : yieldClause->columns()) {
        if (col->expr()->kind() != Expression::Kind::kLabelAttribute) {
            // TODO(yee): support more exprs, such as (player.age + 1) AS age
            return Status::SemanticError("Yield clauses are not supported: `%s'",
                                         col->toString().c_str());
        }
        auto la = static_cast<LabelAttributeExpression*>(col->expr());
        const std::string& schemaName = la->left()->name();
        if (schemaName != from) {
            return Status::SemanticError("Schema name error: %s", schemaName.c_str());
        }

        const auto& value = la->right()->value();
        DCHECK(value.isStr());
        const std::string& colName = value.getStr();
        auto ret = schemaProvider->getFieldType(colName);
        if (ret == meta::cpp2::PropertyType::UNKNOWN) {
            return Status::SemanticError(
                "Column `%s' not found in schema `%s'", colName.c_str(), from.c_str());
        }
        outputs_.emplace_back(col->name(), SchemaUtil::propTypeToValueType(ret));
    }
    return Status::OK();
}

Status LookupValidator::prepareFilter() {
    auto whereClause = sentence()->whereClause();
    if (whereClause == nullptr) {
        return Status::OK();
    }

    auto* filter = whereClause->filter();
    if (FTIndexUtils::needTextSearch(filter)) {
        auto retFilter = genTsFilter(filter);
        NG_RETURN_IF_ERROR(retFilter);
        auto filterExpr = std::move(retFilter).value();
        if (filterExpr == nullptr) {
            // return empty result direct.
            lookupCtx_->isEmptyResultSet = true;
            return Status::OK();
        }
        lookupCtx_->filter = filterExpr;
    } else {
        auto ret = checkFilter(filter);
        NG_RETURN_IF_ERROR(ret);
        lookupCtx_->filter = std::move(ret).value();
    }
    return Status::OK();
}

StatusOr<Expression*> LookupValidator::handleLogicalExprOperands(LogicalExpression* lExpr) {
    auto& operands = lExpr->operands();
    for (auto i = 0u; i < operands.size(); i++) {
        auto operand = lExpr->operand(i);
        if (operand->isLogicalExpr()) {
            // Not allow different logical expression to use: A AND B OR C
            return Status::SemanticError("Not supported filter: %s", lExpr->toString().c_str());
        }
        auto ret = checkFilter(operand);
        NG_RETURN_IF_ERROR(ret);
        auto newOperand = ret.value();
        if (operand != newOperand) {
            lExpr->setOperand(i, newOperand);
        }
    }
    return lExpr;
}

StatusOr<Expression*> LookupValidator::checkFilter(Expression* expr) {
    // TODO: Support IN expression push down
    if (expr->isRelExpr()) {
        return checkRelExpr(static_cast<RelationalExpression*>(expr));
    }
    switch (expr->kind()) {
        case Expression::Kind::kLogicalOr: {
            ExpressionUtils::pullOrs(expr);
            return handleLogicalExprOperands(static_cast<LogicalExpression*>(expr));
        }
        case Expression::Kind::kLogicalAnd: {
            ExpressionUtils::pullAnds(expr);
            return handleLogicalExprOperands(static_cast<LogicalExpression*>(expr));
        }
        default: {
            return Status::SemanticError("Expression %s not supported yet",
                                         expr->toString().c_str());
        }
    }
}

StatusOr<Expression*> LookupValidator::checkRelExpr(RelationalExpression* expr) {
    auto* left = expr->left();
    auto* right = expr->right();
    // Does not support filter : schema.col1 > schema.col2
    if (left->kind() == Expression::Kind::kLabelAttribute &&
        right->kind() == Expression::Kind::kLabelAttribute) {
        return Status::SemanticError("Expression %s not supported yet", expr->toString().c_str());
    }
    if (left->kind() == Expression::Kind::kLabelAttribute ||
        right->kind() == Expression::Kind::kLabelAttribute) {
        return rewriteRelExpr(expr);
    }
    return Status::SemanticError("Expression %s not supported yet", expr->toString().c_str());
}

StatusOr<Expression*> LookupValidator::rewriteRelExpr(RelationalExpression* expr) {
    // swap LHS and RHS of relExpr if LabelAttributeExpr in on the right,
    // so that LabelAttributeExpr is always on the left
    auto right = expr->right();
    if (right->kind() == Expression::Kind::kLabelAttribute) {
        expr = static_cast<RelationalExpression*>(reverseRelKind(expr));
    }

    auto left = expr->left();
    auto* la = static_cast<LabelAttributeExpression*>(left);
    if (la->left()->name() != sentence()->from()) {
        return Status::SemanticError("Schema name error: %s", la->left()->name().c_str());
    }

    // fold constant expression
    auto pool = qctx_->objPool();
    auto foldRes = ExpressionUtils::foldConstantExpr(expr);
    NG_RETURN_IF_ERROR(foldRes);
    expr = static_cast<RelationalExpression*>(foldRes.value());
    DCHECK_EQ(expr->left()->kind(), Expression::Kind::kLabelAttribute);

    std::string prop = la->right()->value().getStr();
    auto relExprType = expr->kind();
    auto c = checkConstExpr(expr->right(), prop, relExprType);
    NG_RETURN_IF_ERROR(c);
    expr->setRight(ConstantExpression::make(pool, std::move(c).value()));

    // rewrite PropertyExpression
    if (lookupCtx_->isEdge) {
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
    auto schemaMgr = qctx_->schemaMng();
    auto schema = lookupCtx_->isEdge ? schemaMgr->getEdgeSchema(spaceId(), schemaId())
                                     : schemaMgr->getTagSchema(spaceId(), schemaId());
    auto type = schema->getFieldType(prop);
    if (type == meta::cpp2::PropertyType::UNKNOWN) {
        return Status::SemanticError("Invalid column: %s", prop.c_str());
    }
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
        // allow diffrent types in the IN expression, such as "abc" IN ["abc"]
        if (v.type() != Value::Type::LIST) {
            return Status::SemanticError("Column type error : %s", prop.c_str());
        }
    }
    return v;
}

StatusOr<std::string> LookupValidator::checkTSExpr(Expression* expr) {
    auto metaClient = qctx_->getMetaClient();
    auto tsi = metaClient->getFTIndexBySpaceSchemaFromCache(spaceId(), schemaId());
    NG_RETURN_IF_ERROR(tsi);
    auto tsName = tsi.value().first;
    auto ret = FTIndexUtils::checkTSIndex(tsClients_, tsName);
    NG_RETURN_IF_ERROR(ret);
    if (!ret.value()) {
        return Status::SemanticError("text search index not found : %s", tsName.c_str());
    }
    auto ftFields = tsi.value().second.get_fields();
    auto tsExpr = static_cast<TextSearchExpression*>(expr);
    auto prop = tsExpr->arg()->prop();

    auto iter = std::find(ftFields.begin(), ftFields.end(), prop);
    if (iter == ftFields.end()) {
        return Status::SemanticError("Column %s not found in %s", prop.c_str(), tsName.c_str());
    }
    return tsName;
}
// Transform (A > B) to (B < A)
Expression* LookupValidator::reverseRelKind(RelationalExpression* expr) {
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
    auto* pool = qctx_->objPool();
    return RelationalExpression::makeKind(pool, reversedKind, right->clone(), left->clone());
}

Status LookupValidator::getSchemaProvider(shared_ptr<const NebulaSchemaProvider>* provider) const {
    auto from = sentence()->from();
    auto schemaMgr = qctx_->schemaMng();
    if (lookupCtx_->isEdge) {
        *provider = schemaMgr->getEdgeSchema(spaceId(), schemaId());
        if (*provider == nullptr) {
            return Status::EdgeNotFound("Edge schema not found : %s", from.c_str());
        }
    } else {
        *provider = schemaMgr->getTagSchema(spaceId(), schemaId());
        if (*provider == nullptr) {
            return Status::TagNotFound("Tag schema not found : %s", from.c_str());
        }
    }
    return Status::OK();
}

StatusOr<Expression*> LookupValidator::genTsFilter(Expression* filter) {
    auto tsRet = FTIndexUtils::getTSClients(qctx_->getMetaClient());
    NG_RETURN_IF_ERROR(tsRet);
    tsClients_ = std::move(tsRet).value();
    auto tsIndex = checkTSExpr(filter);
    NG_RETURN_IF_ERROR(tsIndex);
    return FTIndexUtils::rewriteTSFilter(
        qctx_->objPool(), lookupCtx_->isEdge, filter, tsIndex.value(), tsClients_);
}

}   // namespace graph
}   // namespace nebula
