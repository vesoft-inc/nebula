/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ExpressionUtils.h"

#include <memory>

#include "common/expression/PropertyExpression.h"
#include "common/function/AggFunctionManager.h"
#include "context/QueryExpressionContext.h"
#include "visitor/FoldConstantExprVisitor.h"
#include "common/base/ObjectPool.h"

namespace nebula {
namespace graph {

const Expression *ExpressionUtils::findAny(const Expression *self,
                                     const std::unordered_set<Expression::Kind> &expected) {
    auto finder = [&expected](const Expression *expr) -> bool {
        if (expected.find(expr->kind()) != expected.end()) {
            return true;
        }
        return false;
    };
    FindVisitor visitor(finder);
    const_cast<Expression *>(self)->accept(&visitor);
    auto res = visitor.results();

    if (res.size() == 1) {
        // findAny only produce one result
        return res.front();
    }

    return nullptr;
}

// Find all expression fit any kind
// Empty for not found any one
std::vector<const Expression *> ExpressionUtils::collectAll(
    const Expression *self,
    const std::unordered_set<Expression::Kind> &expected) {
    auto finder = [&expected](const Expression *expr) -> bool {
        if (expected.find(expr->kind()) != expected.end()) {
            return true;
        }
        return false;
    };
    FindVisitor visitor(finder, true);
    const_cast<Expression *>(self)->accept(&visitor);
    return std::move(visitor).results();
}

std::vector<const Expression *> ExpressionUtils::findAllStorage(const Expression *expr) {
    return collectAll(expr,
                      {Expression::Kind::kTagProperty,
                       Expression::Kind::kEdgeProperty,
                       Expression::Kind::kDstProperty,
                       Expression::Kind::kSrcProperty,
                       Expression::Kind::kEdgeSrc,
                       Expression::Kind::kEdgeType,
                       Expression::Kind::kEdgeRank,
                       Expression::Kind::kEdgeDst,
                       Expression::Kind::kVertex,
                       Expression::Kind::kEdge});
}

std::vector<const Expression *> ExpressionUtils::findAllInputVariableProp(const Expression *expr) {
    return collectAll(expr, {Expression::Kind::kInputProperty, Expression::Kind::kVarProperty});
}

bool ExpressionUtils::isConstExpr(const Expression *expr) {
    return !hasAny(expr,
                   {Expression::Kind::kInputProperty,
                    Expression::Kind::kVarProperty,
                    Expression::Kind::kVar,
                    Expression::Kind::kVersionedVar,
                    Expression::Kind::kLabelAttribute,
                    Expression::Kind::kTagProperty,
                    Expression::Kind::kEdgeProperty,
                    Expression::Kind::kDstProperty,
                    Expression::Kind::kSrcProperty,
                    Expression::Kind::kEdgeSrc,
                    Expression::Kind::kEdgeType,
                    Expression::Kind::kEdgeRank,
                    Expression::Kind::kEdgeDst,
                    Expression::Kind::kVertex,
                    Expression::Kind::kEdge});
}

bool ExpressionUtils::isEvaluableExpr(const Expression *expr) {
    EvaluableExprVisitor visitor;
    const_cast<Expression *>(expr)->accept(&visitor);
    return visitor.ok();
}

// rewrite LabelAttr to EdgeProp
Expression *ExpressionUtils::rewriteLabelAttr2EdgeProp(const Expression *expr) {
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kLabelAttribute;
    };
    auto rewriter = [](const Expression *e) -> Expression * {
        DCHECK_EQ(e->kind(), Expression::Kind::kLabelAttribute);
        auto labelAttrExpr = static_cast<const LabelAttributeExpression *>(e);
        auto leftName = new std::string(*labelAttrExpr->left()->name());
        auto rightName = new std::string(labelAttrExpr->right()->value().getStr());
        return new EdgePropertyExpression(leftName, rightName);
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

// rewrite LabelAttr to tagProp
Expression *ExpressionUtils::rewriteLabelAttr2TagProp(const Expression *expr) {
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kLabelAttribute;
    };
    auto rewriter = [](const Expression *e) -> Expression * {
        DCHECK_EQ(e->kind(), Expression::Kind::kLabelAttribute);
        auto labelAttrExpr = static_cast<const LabelAttributeExpression *>(e);
        auto leftName = new std::string(*labelAttrExpr->left()->name());
        auto rightName = new std::string(labelAttrExpr->right()->value().getStr());
        return new TagPropertyExpression(leftName, rightName);
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

// rewrite Agg to VarProp
Expression *ExpressionUtils::rewriteAgg2VarProp(const Expression *expr) {
        auto matcher = [](const Expression* e) -> bool {
            return e->kind() == Expression::Kind::kAggregate;
        };
        auto rewriter = [](const Expression* e) -> Expression* {
            return new VariablePropertyExpression(new std::string(""),
                                                  new std::string(e->toString()));
        };

        return RewriteVisitor::transform(expr,
                                         std::move(matcher),
                                         std::move(rewriter));
    }

Expression *ExpressionUtils::foldConstantExpr(const Expression *expr, ObjectPool *objPool) {
    auto newExpr = expr->clone();
    FoldConstantExprVisitor visitor;
    newExpr->accept(&visitor);
    if (visitor.canBeFolded()) {
        return objPool->add(visitor.fold(newExpr.get()));
    }
    return objPool->add(newExpr.release());
}

Expression *ExpressionUtils::reduceUnaryNotExpr(const Expression *expr, ObjectPool *pool) {
    // Match the operand
    auto operandMatcher = [](const Expression *operandExpr) -> bool {
        return (operandExpr->kind() == Expression::Kind::kUnaryNot ||
                (operandExpr->isRelExpr() && operandExpr->kind() != Expression::Kind::kRelREG) ||
                operandExpr->isLogicalExpr());
    };

    // Match the root expression
    auto rootMatcher = [&](const Expression *e) -> bool {
        if (e->kind() == Expression::Kind::kUnaryNot) {
            auto operand = static_cast<const UnaryExpression *>(e)->operand();
            return (operandMatcher(operand));
        }
        return false;
    };

    std::function<Expression *(const Expression *)> rewriter =
        [&](const Expression *e) -> Expression * {
        auto operand = static_cast<const UnaryExpression *>(e)->operand();
        auto reducedExpr = pool->add(operand->clone().release());

        if (reducedExpr->kind() == Expression::Kind::kUnaryNot) {
            auto castedExpr = static_cast<UnaryExpression *>(reducedExpr);
            reducedExpr = castedExpr->operand();
        } else if (reducedExpr->isRelExpr() && reducedExpr->kind() != Expression::Kind::kRelREG) {
            auto castedExpr = static_cast<RelationalExpression *>(reducedExpr);
            reducedExpr = pool->add(reverseRelExpr(castedExpr).release());
        } else if (reducedExpr->isLogicalExpr()) {
            auto castedExpr = static_cast<LogicalExpression *>(reducedExpr);
            reducedExpr = pool->add(reverseLogicalExpr(castedExpr).release());
        }
        // Rewrite the output of rewrite if possible
        return operandMatcher(reducedExpr)
                   ? RewriteVisitor::transform(reducedExpr, rootMatcher, rewriter)
                   : reducedExpr;
    };

    return pool->add(RewriteVisitor::transform(expr, rootMatcher, rewriter));
}

Expression *ExpressionUtils::rewriteRelExpr(const Expression *expr, ObjectPool *pool) {
    // Match relational expressions containing at least one airthmetic expr
    auto matcher = [&](const Expression *e) -> bool {
        if (e->isRelExpr()) {
            auto relExpr = static_cast<const RelationalExpression *>(e);
            if (isEvaluableExpr(relExpr->right())) {
                return true;
            }
            // TODO: To match arithmetic expression on both side
            auto lExpr = relExpr->left();
            if (lExpr->isArithmeticExpr()) {
                auto arithmExpr = static_cast<const ArithmeticExpression *>(lExpr);
                return isEvaluableExpr(arithmExpr->left()) || isEvaluableExpr(arithmExpr->right());
            }
        }
        return false;
    };

    // Simplify relational expressions involving boolean literals
    auto simplifyBoolOperand =
        [&](RelationalExpression *relExpr, Expression *lExpr, Expression *rExpr) -> Expression * {
        QueryExpressionContext ctx(nullptr);
        if (rExpr->kind() == Expression::Kind::kConstant) {
            auto conExpr = static_cast<ConstantExpression *>(rExpr);
            auto val = conExpr->eval(ctx(nullptr));
            auto valType = val.type();
            // Rewrite to null if the expression contains any operand that is null
            if (valType == Value::Type::NULLVALUE) {
                return rExpr->clone().release();
            }
            if (relExpr->kind() == Expression::Kind::kRelEQ) {
                if (valType == Value::Type::BOOL) {
                    return val.getBool() ? lExpr->clone().release()
                                         : new UnaryExpression(Expression::Kind::kUnaryNot,
                                                               lExpr->clone().release());
                }
            } else if (relExpr->kind() == Expression::Kind::kRelNE) {
                if (valType == Value::Type::BOOL) {
                    return val.getBool() ? new UnaryExpression(Expression::Kind::kUnaryNot,
                                                               lExpr->clone().release())
                                         : lExpr->clone().release();
                }
            }
        }
        return nullptr;
    };

    std::function<Expression *(const Expression *)> rewriter =
        [&](const Expression *e) -> Expression * {
        auto exprCopy = pool->add(e->clone().release());
        auto relExpr = static_cast<RelationalExpression *>(exprCopy);
        auto lExpr = relExpr->left();
        auto rExpr = relExpr->right();

        // Simplify relational expressions involving boolean literals
        auto simplifiedExpr = simplifyBoolOperand(relExpr, lExpr, rExpr);
        if (simplifiedExpr) {
            return simplifiedExpr;
        }
        // Move all evaluable expression to the right side
        auto relRightOperandExpr = relExpr->right()->clone();
        auto relLeftOperandExpr = rewriteRelExprHelper(relExpr->left(), relRightOperandExpr);
        return new RelationalExpression(relExpr->kind(),
                                        relLeftOperandExpr->clone().release(),
                                        relRightOperandExpr->clone().release());
    };

    return pool->add(RewriteVisitor::transform(expr, matcher, rewriter));
}

Expression *ExpressionUtils::rewriteRelExprHelper(
    const Expression *expr,
    std::unique_ptr<Expression> &relRightOperandExpr) {
    // TODO: Support rewrite mul/div expressoion after fixing overflow
    auto matcher = [](const Expression *e) -> bool {
        if (!e->isArithmeticExpr() ||
            e->kind() == Expression::Kind::kMultiply ||
            e->kind() == Expression::Kind::kDivision)
            return false;
        auto arithExpr = static_cast<const ArithmeticExpression *>(e);
        return ExpressionUtils::isEvaluableExpr(arithExpr->left()) ||
               ExpressionUtils::isEvaluableExpr(arithExpr->right());
    };

    if (!matcher(expr)) {
        return const_cast<Expression *>(expr);
    }

    auto arithExpr = static_cast<const ArithmeticExpression *>(expr);
    auto kind = getNegatedArithmeticType(arithExpr->kind());
    auto lexpr = relRightOperandExpr->clone().release();
    const Expression *root = nullptr;
    Expression *rexpr = nullptr;

    // Use left operand as root
    if (ExpressionUtils::isEvaluableExpr(arithExpr->right())) {
        rexpr = arithExpr->right()->clone().release();
        root = arithExpr->left();
    } else {
        rexpr = arithExpr->left()->clone().release();
        root = arithExpr->right();
    }

    relRightOperandExpr.reset(new ArithmeticExpression(kind, lexpr, rexpr));
    return rewriteRelExprHelper(root, relRightOperandExpr);
}

Expression *ExpressionUtils::filterTransform(const Expression *filter, ObjectPool *pool) {
    auto rewrittenExpr = const_cast<Expression *>(filter);
    // Rewrite relational expression
    rewrittenExpr = rewriteRelExpr(rewrittenExpr, pool);
    // Fold constant expression
    rewrittenExpr = foldConstantExpr(rewrittenExpr, pool);
    // Reduce Unary expression
    rewrittenExpr = reduceUnaryNotExpr(rewrittenExpr, pool);
    return rewrittenExpr;
}

void ExpressionUtils::pullAnds(Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
    auto *logic = static_cast<LogicalExpression *>(expr);
    std::vector<std::unique_ptr<Expression>> operands;
    pullAndsImpl(logic, operands);
    logic->setOperands(std::move(operands));
}

void ExpressionUtils::pullOrs(Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
    auto *logic = static_cast<LogicalExpression *>(expr);
    std::vector<std::unique_ptr<Expression>> operands;
    pullOrsImpl(logic, operands);
    logic->setOperands(std::move(operands));
}

void ExpressionUtils::pullAndsImpl(LogicalExpression *expr,
                                   std::vector<std::unique_ptr<Expression>> &operands) {
    for (auto &operand : expr->operands()) {
        if (operand->kind() != Expression::Kind::kLogicalAnd) {
            operands.emplace_back(std::move(operand));
            continue;
        }
        pullAndsImpl(static_cast<LogicalExpression *>(operand.get()), operands);
    }
}

void ExpressionUtils::pullOrsImpl(LogicalExpression *expr,
                                  std::vector<std::unique_ptr<Expression>> &operands) {
    for (auto &operand : expr->operands()) {
        if (operand->kind() != Expression::Kind::kLogicalOr) {
            operands.emplace_back(std::move(operand));
            continue;
        }
        pullOrsImpl(static_cast<LogicalExpression *>(operand.get()), operands);
    }
}

std::unique_ptr<Expression> ExpressionUtils::flattenInnerLogicalAndExpr(const Expression *expr) {
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kLogicalAnd;
    };
    auto rewriter = [](const Expression *e) -> Expression * {
        pullAnds(const_cast<Expression *>(e));
        return e->clone().release();
    };

    return std::unique_ptr<Expression>(
        RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter)));
}

std::unique_ptr<Expression> ExpressionUtils::flattenInnerLogicalOrExpr(const Expression *expr) {
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kLogicalOr;
    };
    auto rewriter = [](const Expression *e) -> Expression * {
        pullOrs(const_cast<Expression *>(e));
        return e->clone().release();
    };

    return std::unique_ptr<Expression>(
        RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter)));
}

std::unique_ptr<Expression> ExpressionUtils::flattenInnerLogicalExpr(const Expression *expr) {
    auto andFlattenExpr = flattenInnerLogicalAndExpr(expr);
    auto allFlattenExpr = flattenInnerLogicalOrExpr(andFlattenExpr.get());

    return allFlattenExpr;
}

VariablePropertyExpression *ExpressionUtils::newVarPropExpr(const std::string &prop,
                                                            const std::string &var) {
    return new VariablePropertyExpression(new std::string(var), new std::string(prop));
}

std::unique_ptr<InputPropertyExpression> ExpressionUtils::inputPropExpr(const std::string &prop) {
    return std::make_unique<InputPropertyExpression>(new std::string(prop));
}

std::unique_ptr<Expression> ExpressionUtils::pushOrs(
    const std::vector<std::unique_ptr<Expression>> &rels) {
    return pushImpl(Expression::Kind::kLogicalOr, rels);
}

std::unique_ptr<Expression> ExpressionUtils::pushAnds(
    const std::vector<std::unique_ptr<Expression>> &rels) {
    return pushImpl(Expression::Kind::kLogicalAnd, rels);
}

std::unique_ptr<Expression> ExpressionUtils::pushImpl(
    Expression::Kind kind,
    const std::vector<std::unique_ptr<Expression>> &rels) {
    DCHECK_GT(rels.size(), 1);
    DCHECK(kind == Expression::Kind::kLogicalOr || kind == Expression::Kind::kLogicalAnd);
    auto root = std::make_unique<LogicalExpression>(kind);
    root->addOperand(rels[0]->clone().release());
    root->addOperand(rels[1]->clone().release());
    for (size_t i = 2; i < rels.size(); i++) {
        auto l = std::make_unique<LogicalExpression>(kind);
        l->addOperand(root->clone().release());
        l->addOperand(rels[i]->clone().release());
        root = std::move(l);
    }
    return root;
}

std::unique_ptr<Expression> ExpressionUtils::expandExpr(const Expression *expr) {
    auto kind = expr->kind();
    std::vector<std::unique_ptr<Expression>> target;
    switch (kind) {
        case Expression::Kind::kLogicalOr: {
            const auto *logic = static_cast<const LogicalExpression *>(expr);
            for (const auto &e : logic->operands()) {
                if (e->kind() == Expression::Kind::kLogicalAnd) {
                    target.emplace_back(expandImplAnd(e.get()));
                } else {
                    target.emplace_back(expandExpr(e.get()));
                }
            }
            break;
        }
        case Expression::Kind::kLogicalAnd: {
            target.emplace_back(expandImplAnd(expr));
            break;
        }
        default: { return expr->clone(); }
    }
    DCHECK_GT(target.size(), 0);
    if (target.size() == 1) {
        if (target[0]->kind() == Expression::Kind::kLogicalAnd) {
            const auto *logic = static_cast<const LogicalExpression *>(target[0].get());
            const auto &ops = logic->operands();
            DCHECK_EQ(ops.size(), 2);
            if (ops[0]->kind() == Expression::Kind::kLogicalOr ||
                ops[1]->kind() == Expression::Kind::kLogicalOr) {
                return expandExpr(target[0].get());
            }
        }
        return std::move(target[0]);
    }
    return pushImpl(kind, target);
}

std::unique_ptr<Expression> ExpressionUtils::expandImplAnd(const Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
    const auto *logic = static_cast<const LogicalExpression *>(expr);
    DCHECK_EQ(logic->operands().size(), 2);
    std::vector<std::unique_ptr<Expression>> subL;
    auto &ops = logic->operands();
    if (ops[0]->kind() == Expression::Kind::kLogicalOr) {
        auto target = expandImplOr(ops[0].get());
        for (const auto &e : target) {
            subL.emplace_back(e->clone().release());
        }
    } else {
        subL.emplace_back(expandExpr(std::move(ops[0]).get()));
    }
    std::vector<std::unique_ptr<Expression>> subR;
    if (ops[1]->kind() == Expression::Kind::kLogicalOr) {
        auto target = expandImplOr(ops[1].get());
        for (const auto &e : target) {
            subR.emplace_back(e->clone().release());
        }
    } else {
        subR.emplace_back(expandExpr(std::move(ops[1]).get()));
    }

    DCHECK_GT(subL.size(), 0);
    DCHECK_GT(subR.size(), 0);
    std::vector<std::unique_ptr<Expression>> target;
    for (auto &le : subL) {
        for (auto &re : subR) {
            auto l = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd);
            l->addOperand(le->clone().release());
            l->addOperand(re->clone().release());
            target.emplace_back(std::move(l));
        }
    }
    DCHECK_GT(target.size(), 0);
    if (target.size() == 1) {
        return std::move(target[0]);
    }
    return pushImpl(Expression::Kind::kLogicalOr, target);
}

std::vector<std::unique_ptr<Expression>> ExpressionUtils::expandImplOr(const Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
    const auto *logic = static_cast<const LogicalExpression *>(expr);
    std::vector<std::unique_ptr<Expression>> exprs;
    auto &ops = logic->operands();
    for (const auto &op : ops) {
        if (op->kind() == Expression::Kind::kLogicalOr) {
            auto target = expandImplOr(op.get());
            for (const auto &e : target) {
                exprs.emplace_back(e->clone().release());
            }
        } else {
            exprs.emplace_back(op->clone().release());
        }
    }
    return exprs;
}

Status ExpressionUtils::checkAggExpr(const AggregateExpression *aggExpr) {
    auto func = *aggExpr->name();
    std::transform(func.begin(), func.end(), func.begin(), ::toupper);

    NG_RETURN_IF_ERROR(AggFunctionManager::find(func));

    auto *aggArg = aggExpr->arg();
    if (graph::ExpressionUtils::findAny(aggArg, {Expression::Kind::kAggregate})) {
        return Status::SemanticError("Aggregate function nesting is not allowed: `%s'",
                                     aggExpr->toString().c_str());
    }

    // check : $-.* or $var.* can only be applied on `COUNT`
    if (func.compare("COUNT") && (aggArg->kind() == Expression::Kind::kInputProperty ||
                                  aggArg->kind() == Expression::Kind::kVarProperty)) {
        auto propExpr = static_cast<const PropertyExpression *>(aggArg);
        if (*propExpr->prop() == "*") {
            return Status::SemanticError("Could not apply aggregation function `%s' on `%s'",
                                         aggExpr->toString().c_str(),
                                         propExpr->toString().c_str());
        }
    }

    return Status::OK();
}

// Negate the given relational expr
std::unique_ptr<RelationalExpression> ExpressionUtils::reverseRelExpr(RelationalExpression *expr) {
    auto left = static_cast<RelationalExpression *>(expr)->left();
    auto right = static_cast<RelationalExpression *>(expr)->right();
    auto negatedKind = getNegatedRelExprKind(expr->kind());

    return std::make_unique<RelationalExpression>(
        negatedKind, left->clone().release(), right->clone().release());
}

// Return the negation of the given relational kind
Expression::Kind ExpressionUtils::getNegatedRelExprKind(const Expression::Kind kind) {
    switch (kind) {
        case Expression::Kind::kRelEQ:
            return Expression::Kind::kRelNE;
        case Expression::Kind::kRelNE:
            return Expression::Kind::kRelEQ;
        case Expression::Kind::kRelLT:
            return Expression::Kind::kRelGE;
        case Expression::Kind::kRelLE:
            return Expression::Kind::kRelGT;
        case Expression::Kind::kRelGT:
            return Expression::Kind::kRelLE;
        case Expression::Kind::kRelGE:
            return Expression::Kind::kRelLT;
        case Expression::Kind::kRelIn:
            return Expression::Kind::kRelNotIn;
        case Expression::Kind::kRelNotIn:
            return Expression::Kind::kRelIn;
        case Expression::Kind::kContains:
            return Expression::Kind::kNotContains;
        case Expression::Kind::kNotContains:
            return Expression::Kind::kContains;
        case Expression::Kind::kStartsWith:
            return Expression::Kind::kNotStartsWith;
        case Expression::Kind::kNotStartsWith:
            return Expression::Kind::kStartsWith;
        case Expression::Kind::kEndsWith:
            return Expression::Kind::kNotEndsWith;
        case Expression::Kind::kNotEndsWith:
            return Expression::Kind::kEndsWith;
        default:
            LOG(FATAL) << "Invalid relational expression kind: " << static_cast<uint8_t>(kind);
            break;
    }
}

Expression::Kind ExpressionUtils::getNegatedArithmeticType(const Expression::Kind kind) {
    switch (kind) {
        case Expression::Kind::kAdd:
            return Expression::Kind::kMinus;
        case Expression::Kind::kMinus:
            return Expression::Kind::kAdd;
        case Expression::Kind::kMultiply:
            return Expression::Kind::kDivision;
        case Expression::Kind::kDivision:
            return Expression::Kind::kMultiply;
        case Expression::Kind::kMod:
            LOG(FATAL) << "Unsupported expression kind: " << static_cast<uint8_t>(kind);
            break;
        default:
            LOG(FATAL) << "Invalid arithmetic expression kind: " << static_cast<uint8_t>(kind);
            break;
    }
}

std::unique_ptr<LogicalExpression> ExpressionUtils::reverseLogicalExpr(LogicalExpression *expr) {
    std::vector<std::unique_ptr<Expression>> operands;
    if (expr->kind() == Expression::Kind::kLogicalAnd) {
        pullAnds(expr);
    } else {
        pullOrs(expr);
    }

    auto &flattenOperands = static_cast<LogicalExpression *>(expr)->operands();
    auto negatedKind = getNegatedLogicalExprKind(expr->kind());
    auto logic = std::make_unique<LogicalExpression>(negatedKind);

    // negate each item in the operands list
    for (auto &operand : flattenOperands) {
        auto tempExpr =
            std::make_unique<UnaryExpression>(Expression::Kind::kUnaryNot, operand.release());
        operands.emplace_back(std::move(tempExpr));
    }
    logic->setOperands(std::move(operands));
    return logic;
}

Expression::Kind ExpressionUtils::getNegatedLogicalExprKind(const Expression::Kind kind) {
    switch (kind) {
        case Expression::Kind::kLogicalAnd:
            return Expression::Kind::kLogicalOr;
        case Expression::Kind::kLogicalOr:
            return Expression::Kind::kLogicalAnd;
        case Expression::Kind::kLogicalXor:
            LOG(FATAL) << "Unsupported logical expression kind: " << static_cast<uint8_t>(kind);
            break;
        default:
            LOG(FATAL) << "Invalid logical expression kind: " << static_cast<uint8_t>(kind);
            break;
    }
}

}   // namespace graph
}   // namespace nebula
