/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/IndexScanValidator.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

Status IndexScanValidator::validateImpl() {
    NG_RETURN_IF_ERROR(prepareFrom());
    NG_RETURN_IF_ERROR(prepareYield());
    NG_RETURN_IF_ERROR(prepareFilter());
    return Status::OK();
}

Status IndexScanValidator::toPlan() {
    return genSingleNodePlan<IndexScan>(spaceId_,
                                        std::move(contexts_),
                                        std::move(returnCols_),
                                        isEdge_,
                                        schemaId_);
}

Status IndexScanValidator::prepareFrom() {
    auto *sentence = static_cast<const LookupSentence *>(sentence_);
    spaceId_ = vctx_->whichSpace().id;
    const auto* from = sentence->from();
    auto ret = qctx_->schemaMng()->toEdgeType(spaceId_, *from);
    if (ret.ok()) {
        isEdge_ = true;
    } else {
        ret = qctx_->schemaMng()->toTagID(spaceId_, *from);
        NG_RETURN_IF_ERROR(ret);
        isEdge_ = false;
    }
    schemaId_ = ret.value();
    return Status::OK();
}

Status IndexScanValidator::prepareYield() {
    auto *sentence = static_cast<const LookupSentence *>(sentence_);
    auto columns = sentence->yieldClause()->columns();
    auto schema = qctx_->schemaMng()->getEdgeSchema(spaceId_, schemaId_);
    const auto* from = sentence->from();
    if (schema == nullptr) {
        return isEdge_
               ? Status::EdgeNotFound("Edge schema not found : %s", from->c_str())
               : Status::TagNotFound("Tag schema not found : %s", from->c_str());
    }
    returnCols_ = std::make_unique<std::vector<std::string>>();
    for (const auto* col : columns) {
        if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
            auto laExpr = static_cast<LabelAttributeExpression *>(col->expr());
            if (*laExpr->left()->name() != *from) {
                return Status::SemanticError("Schema name error : %s",
                                             laExpr->left()->name()->c_str());
            }
            auto ret = schema->getFieldType(*laExpr->right()->name());
            if (ret == meta::cpp2::PropertyType::UNKNOWN) {
                return Status::SemanticError("Column %s not found in schema %s",
                                             laExpr->right()->name()->c_str(),
                                             from->c_str());
            }
            returnCols_->emplace_back(*laExpr->right()->name());
            auto typeResult = deduceExprType(col->expr());
            NG_RETURN_IF_ERROR(typeResult);
            outputs_.emplace_back(deduceColName(col), typeResult.value());
        } else {
            return Status::SemanticError();
        }
    }
    return Status::OK();
}

Status IndexScanValidator::prepareFilter() {
    auto *sentence = static_cast<const LookupSentence *>(sentence_);
    auto *filter = sentence->whereClause()->filter();
    auto ret = checkFilter(filter, *sentence->from());
    NG_RETURN_IF_ERROR(ret);
    storage::cpp2::IndexQueryContext ctx;
    ctx.set_filter(Expression::encode(*filter));
    contexts_ = std::make_unique<std::vector<storage::cpp2::IndexQueryContext>>();
    contexts_->emplace_back(std::move(ctx));
    return Status::OK();
}

Status IndexScanValidator::checkFilter(Expression* expr, const std::string& from) {
    // TODO (sky) : Rewrite simple expressions,
    //              for example rewrite expr from col1 > 1 + 2 to col > 3
    switch (expr->kind()) {
        case Expression::Kind::kLogicalOr :
        case Expression::Kind::kLogicalAnd : {
            auto lExpr = static_cast<LogicalExpression*>(expr);
            auto ret = checkFilter(lExpr->left(), from);
            NG_RETURN_IF_ERROR(ret);
            ret = checkFilter(lExpr->right(), from);
            NG_RETURN_IF_ERROR(ret);
            break;
        }
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelNE: {
            auto* rExpr = static_cast<RelationalExpression*>(expr);
            auto* left = rExpr->left();
            auto* right = rExpr->right();
            // Does not support filter : schema.col1 > schema.col2
            if (left->kind() == Expression::Kind::kLabelAttribute &&
                right->kind() == Expression::Kind::kLabelAttribute) {
                return Status::NotSupported("Expression %s not supported yet",
                                            rExpr->toString().c_str());
            } else if (left->kind() == Expression::Kind::kLabelAttribute) {
                auto* attExpr = static_cast<LabelAttributeExpression *>(left);
                if (*attExpr->left()->name() != from) {
                    return Status::SemanticError("Schema name error : %s",
                                                 attExpr->left()->name()->c_str());
                }
            } else if (right->kind() == Expression::Kind::kLabelAttribute) {
                auto* attExpr = static_cast<LabelAttributeExpression *>(right);
                if (*attExpr->left()->name() != from) {
                    return Status::SemanticError("Schema name error : %s",
                                                  attExpr->left()->name()->c_str());
                }
            } else {
                return Status::NotSupported("Expression %s not supported yet",
                                            rExpr->toString().c_str());
            }
            break;
        }
        default: {
            return Status::NotSupported("Expression %s not supported yet",
                                        expr->toString().c_str());
        }
    }
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
