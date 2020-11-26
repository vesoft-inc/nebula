/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/IndexScanValidator.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"

DECLARE_uint32(ft_request_retry_times);

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
                                        schemaId_,
                                        isEmptyResultSet_);
}

Status IndexScanValidator::prepareFrom() {
    auto *sentence = static_cast<const LookupSentence *>(sentence_);
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

Status IndexScanValidator::prepareYield() {
    auto *sentence = static_cast<const LookupSentence *>(sentence_);
    returnCols_ = std::make_unique<std::vector<std::string>>();
    if (sentence->yieldClause() == nullptr) {
        return Status::OK();
    }
    // When whereClause is nullptr, yieldClause is not nullptr,
    // only return vid for tag. return src, ranking, dst for edge
    if (sentence->whereClause() == nullptr) {
        return Status::SemanticError("Yield clauses are not supported "
                                     "when WHERE clause does not exist");
    }
    auto columns = sentence->yieldClause()->columns();
    auto schema = isEdge_
                  ? qctx_->schemaMng()->getEdgeSchema(spaceId_, schemaId_)
                  : qctx_->schemaMng()->getTagSchema(spaceId_, schemaId_);
    if (schema == nullptr) {
        return isEdge_
               ? Status::EdgeNotFound("Edge schema not found : %s", from_.c_str())
               : Status::TagNotFound("Tag schema not found : %s", from_.c_str());
    }
    for (auto col : columns) {
        std::string schemaName, colName;
        if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
            auto la = static_cast<LabelAttributeExpression *>(col->expr());
            schemaName = *la->left()->name();
            colName = *la->right()->name();
        } else {
            return Status::SemanticError("Yield clauses are not supported : %s",
                                         col->expr()->toString().c_str());
        }

        if (schemaName != from_) {
            return Status::SemanticError("Schema name error : %s", schemaName.c_str());
        }
        auto ret = schema->getFieldType(colName);
        if (ret == meta::cpp2::PropertyType::UNKNOWN) {
            return Status::SemanticError("Column %s not found in schema %s",
                                         colName.c_str(), from_.c_str());
        }
        returnCols_->emplace_back(colName);
    }
    return Status::OK();
}

Status IndexScanValidator::prepareFilter() {
    auto *sentence = static_cast<const LookupSentence *>(sentence_);
    if (sentence->whereClause() == nullptr) {
        return Status::OK();
    }

    auto *filter = sentence->whereClause()->filter();
    storage::cpp2::IndexQueryContext ctx;
    if (needTextSearch(filter)) {
        NG_RETURN_IF_ERROR(checkTSService());
        if (!textSearchReady_) {
            return Status::Error("Text search service not ready");
        }
        auto retFilter = rewriteTSFilter(filter);
        if (!retFilter.ok()) {
            return retFilter.status();
        }
        if (isEmptyResultSet_) {
            // return empty result direct.
            return Status::OK();
        }
        ctx.set_filter(std::move(retFilter).value());
    } else {
        auto ret = checkFilter(filter);
        NG_RETURN_IF_ERROR(ret);
        ctx.set_filter(Expression::encode(*filter));
    }
    contexts_ = std::make_unique<std::vector<storage::cpp2::IndexQueryContext>>();
    contexts_->emplace_back(std::move(ctx));
    return Status::OK();
}

StatusOr<std::string>
IndexScanValidator::rewriteTSFilter(Expression* expr) {
    std::vector<std::string> values;
    auto tsExpr = static_cast<TextSearchExpression*>(expr);
    auto vRet = textSearch(tsExpr);
    if (!vRet.ok()) {
        return Status::Error("Text search error.");
    }
    if (vRet.value().empty()) {
        isEmptyResultSet_ = true;
        return Status::OK();
    }
    std::vector<std::unique_ptr<RelationalExpression>> rels;
    for (const auto& row : vRet.value()) {
        std::unique_ptr<RelationalExpression> r;
        if (isEdge_) {
            r = std::make_unique<RelationalExpression>(
                Expression::Kind::kRelEQ,
                new EdgePropertyExpression(new std::string(*tsExpr->arg()->from()),
                                           new std::string(*tsExpr->arg()->prop())),
                new ConstantExpression(Value(row)));
        } else {
            r = std::make_unique<RelationalExpression>(
                Expression::Kind::kRelEQ,
                new TagPropertyExpression(new std::string(*tsExpr->arg()->from()),
                                          new std::string(*tsExpr->arg()->prop())),
                new ConstantExpression(Value(row)));
        }
        rels.emplace_back(std::move(r));
    }
    if (rels.size() == 1) {
        return rels[0]->encode();
    }
    auto newExpr = ExpressionUtils::pushOrs(rels);
    return newExpr->encode();
}

StatusOr<std::vector<std::string>> IndexScanValidator::textSearch(TextSearchExpression* expr) {
    if (*expr->arg()->from() != from_) {
        return Status::SemanticError("Schema name error : %s", expr->arg()->from()->c_str());
    }
    auto index = nebula::plugin::IndexTraits::indexName(space_.spaceDesc.space_name, isEdge_);
    nebula::plugin::DocItem doc(index, *expr->arg()->prop(), schemaId_, *expr->arg()->val());
    nebula::plugin::LimitItem limit(expr->arg()->timeout(), expr->arg()->limit());
    std::vector<std::string> result;
    // TODO (sky) : External index load balancing
    auto retryCnt = FLAGS_ft_request_retry_times;
    while (--retryCnt > 0) {
        StatusOr<bool> ret = Status::Error();
        switch (expr->kind()) {
            case Expression::Kind::kTSFuzzy: {
                folly::dynamic fuzz = folly::dynamic::object();
                if (expr->arg()->fuzziness() < 0) {
                    fuzz = "AUTO";
                } else {
                    fuzz = expr->arg()->fuzziness();
                }
                std::string op = (expr->arg()->op() == nullptr) ? "or" : *expr->arg()->op();
                ret = nebula::plugin::ESGraphAdapter::kAdapter->fuzzy(randomFTClient(),
                                                                      doc,
                                                                      limit,
                                                                      fuzz,
                                                                      op,
                                                                      result);
                break;
            }
            case Expression::Kind::kTSPrefix: {
                ret = nebula::plugin::ESGraphAdapter::kAdapter->prefix(randomFTClient(),
                                                                       doc,
                                                                       limit,
                                                                       result);
                break;
            }
            case Expression::Kind::kTSRegexp: {
                ret = nebula::plugin::ESGraphAdapter::kAdapter->regexp(randomFTClient(),
                                                                       doc,
                                                                       limit,
                                                                       result);
                break;
            }
            case Expression::Kind::kTSWildcard: {
                ret = nebula::plugin::ESGraphAdapter::kAdapter->wildcard(randomFTClient(),
                                                                         doc,
                                                                         limit,
                                                                         result);
                break;
            }
            default:
                return Status::Error("text search expression error");
        }
        if (!ret.ok()) {
            continue;
        } else if (ret.value()) {
            return result;
        } else {
            return Status::Error("External index error. "
                                 "please check the status of fulltext cluster");
        }
    }
    return Status::Error("scan external index failed");
}

bool IndexScanValidator::needTextSearch(Expression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kTSFuzzy:
        case Expression::Kind::kTSPrefix:
        case Expression::Kind::kTSRegexp:
        case Expression::Kind::kTSWildcard: {
            return true;
        }
        default:
            return false;
    }
}

Status IndexScanValidator::checkFilter(Expression* expr) {
    // TODO (sky) : Rewrite simple expressions,
    //              for example rewrite expr from col1 > 1 + 2 to col > 3
    switch (expr->kind()) {
        case Expression::Kind::kLogicalOr :
        case Expression::Kind::kLogicalAnd : {
            // TODO(dutor) Deal with n-ary operands
            auto lExpr = static_cast<LogicalExpression*>(expr);
            auto ret = checkFilter(lExpr->operand(0));
            NG_RETURN_IF_ERROR(ret);
            ret = checkFilter(lExpr->operand(1));
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
            return checkRelExpr(rExpr);
        }
        default: {
            return Status::NotSupported("Expression %s not supported yet",
                                        expr->toString().c_str());
        }
    }
    return Status::OK();
}

Status IndexScanValidator::checkRelExpr(RelationalExpression* expr) {
    auto* left = expr->left();
    auto* right = expr->right();
    // Does not support filter : schema.col1 > schema.col2
    if (left->kind() == Expression::Kind::kLabelAttribute &&
        right->kind() == Expression::Kind::kLabelAttribute) {
        return Status::NotSupported("Expression %s not supported yet",
                                    expr->toString().c_str());
    } else if (left->kind() == Expression::Kind::kLabelAttribute ||
               right->kind() == Expression::Kind::kLabelAttribute) {
        auto ret = rewriteRelExpr(expr);
        NG_RETURN_IF_ERROR(ret);
    } else {
        return Status::NotSupported("Expression %s not supported yet",
                                    expr->toString().c_str());
    }
    return Status::OK();
}

Status IndexScanValidator::rewriteRelExpr(RelationalExpression* expr) {
    auto* left = expr->left();
    auto* right = expr->right();
    auto leftIsAE = left->kind() == Expression::Kind::kLabelAttribute;

    auto* la = leftIsAE
               ? static_cast<LabelAttributeExpression *>(left)
               : static_cast<LabelAttributeExpression *>(right);
    if (*la->left()->name() != from_) {
        return Status::SemanticError("Schema name error : %s",
                                     la->left()->name()->c_str());
    }

    std::string prop = *la->right()->name();
    // rewrite ConstantExpression
    auto c = leftIsAE
             ? checkConstExpr(right, prop)
             : checkConstExpr(left, prop);

    if (!c.ok()) {
        return Status::SemanticError("expression error : %s", left->toString().c_str());
    }

    if (leftIsAE) {
        expr->setRight(new ConstantExpression(std::move(c).value()));
    } else {
        expr->setLeft(new ConstantExpression(std::move(c).value()));
    }

    // rewrite PropertyExpression
    if (leftIsAE) {
        if (isEdge_) {
            expr->setLeft(ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(la));
        } else {
            expr->setLeft(ExpressionUtils::rewriteLabelAttribute<TagPropertyExpression>(la));
        }
    } else {
        if (isEdge_) {
            expr->setRight(ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(la));
        } else {
            expr->setRight(ExpressionUtils::rewriteLabelAttribute<TagPropertyExpression>(la));
        }
    }
    return Status::OK();
}

StatusOr<Value> IndexScanValidator::checkConstExpr(Expression* expr,
                                                   const std::string& prop) {
    auto schema = isEdge_
                  ? qctx_->schemaMng()->getEdgeSchema(spaceId_, schemaId_)
                  : qctx_->schemaMng()->getTagSchema(spaceId_, schemaId_);
    auto type = schema->getFieldType(prop);
    QueryExpressionContext dummy(nullptr);
    auto v = Expression::eval(expr, dummy);
    if (v.type() != SchemaUtil::propTypeToValueType(type)) {
        return Status::SemanticError("Column type error : %s", prop.c_str());
    }
    return v;
}

Status IndexScanValidator::checkTSService() {
    auto tcs = qctx_->getMetaClient()->getFTClientsFromCache();
    if (!tcs.ok()) {
        return tcs.status();
    }
    if (tcs.value().empty()) {
        return Status::Error("No full text client found");
    }
    textSearchReady_ = true;
    for (const auto& c : tcs.value()) {
        nebula::plugin::HttpClient hc;
        hc.host = c.host;
        if (c.__isset.user && c.__isset.pwd) {
            hc.user = c.user;
            hc.password = c.pwd;
        }
        esClients_.emplace_back(std::move(hc));
    }
    return checkTSIndex();
}

Status IndexScanValidator::checkTSIndex() {
    auto ftIndex = nebula::plugin::IndexTraits::indexName(space_.name, isEdge_);
    auto retryCnt = FLAGS_ft_request_retry_times;
    StatusOr<bool> ret = Status::Error("fulltext index not found : %s", ftIndex.c_str());
    while (--retryCnt > 0) {
        ret = nebula::plugin::ESGraphAdapter::kAdapter->indexExists(randomFTClient(), ftIndex);
        if (!ret.ok()) {
            continue;
        } else if (ret.value()) {
            return Status::OK();
        } else {
            return Status::Error("fulltext index not found : %s", ftIndex.c_str());
        }
    }
    return ret.status();
}

const nebula::plugin::HttpClient& IndexScanValidator::randomFTClient() const {
    auto i = folly::Random::rand32(esClients_.size() - 1);
    return esClients_[i];
}
}  // namespace graph
}  // namespace nebula
