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

bool ExpressionUtils::isPropertyExpr(const Expression *expr) {
    auto kind = expr->kind();

    return std::unordered_set<Expression::Kind>{Expression::Kind::kTagProperty,
                                                Expression::Kind::kEdgeProperty,
                                                Expression::Kind::kInputProperty,
                                                Expression::Kind::kVarProperty,
                                                Expression::Kind::kDstProperty,
                                                Expression::Kind::kSrcProperty}
        .count(kind);
}

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
    ObjectPool *pool = expr->getObjPool();
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kLabelAttribute;
    };
    auto rewriter = [pool](const Expression *e) -> Expression * {
        DCHECK_EQ(e->kind(), Expression::Kind::kLabelAttribute);
        auto labelAttrExpr = static_cast<const LabelAttributeExpression *>(e);
        auto leftName = labelAttrExpr->left()->name();
        auto rightName = labelAttrExpr->right()->value().getStr();
        return EdgePropertyExpression::make(pool, leftName, rightName);
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

// rewrite var in VariablePropExpr to another var
Expression *ExpressionUtils::rewriteInnerVar(const Expression *expr, std::string newVar) {
    ObjectPool* pool = expr->getObjPool();
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kVarProperty;
    };
    auto rewriter = [pool, newVar](const Expression *e) -> Expression * {
        DCHECK_EQ(e->kind(), Expression::Kind::kVarProperty);
        auto varPropExpr = static_cast<const VariablePropertyExpression *>(e);
        auto newProp = varPropExpr->prop();
        return VariablePropertyExpression::make(pool, newVar, newProp);
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

// rewrite LabelAttr to tagProp
Expression *ExpressionUtils::rewriteLabelAttr2TagProp(const Expression *expr) {
    ObjectPool* pool = expr->getObjPool();
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kLabelAttribute;
    };
    auto rewriter = [pool](const Expression *e) -> Expression * {
        DCHECK_EQ(e->kind(), Expression::Kind::kLabelAttribute);
        auto labelAttrExpr = static_cast<const LabelAttributeExpression *>(e);
        auto leftName = labelAttrExpr->left()->name();
        auto rightName = labelAttrExpr->right()->value().getStr();
        return TagPropertyExpression::make(pool, leftName, rightName);
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

// rewrite Agg to VarProp
Expression *ExpressionUtils::rewriteAgg2VarProp(const Expression *expr) {
    ObjectPool* pool = expr->getObjPool();
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kAggregate;
    };
    auto rewriter = [pool](const Expression *e) -> Expression * {
        return VariablePropertyExpression::make(pool, "", e->toString());
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

StatusOr<Expression *> ExpressionUtils::foldConstantExpr(const Expression *expr) {
    ObjectPool* objPool = expr->getObjPool();
    auto newExpr = expr->clone();
    FoldConstantExprVisitor visitor(objPool);
    newExpr->accept(&visitor);
    if (!visitor.ok()) {
        return std::move(visitor).status();
    }
    if (visitor.canBeFolded()) {
        auto foldedExpr = visitor.fold(newExpr);
        if (!visitor.ok()) {
            return std::move(visitor).status();
        }
        return foldedExpr;
    }
    return newExpr;
}

Expression *ExpressionUtils::reduceUnaryNotExpr(const Expression *expr) {
    // Match the operand
    auto operandMatcher = [](const Expression *operandExpr) -> bool {
        return (operandExpr->kind() == Expression::Kind::kUnaryNot ||
                (operandExpr->isRelExpr() && operandExpr->kind() != Expression::Kind::kRelREG) ||
                operandExpr->isLogicalExpr());
    };

    // Match the root expression
    auto rootMatcher = [&operandMatcher](const Expression *e) -> bool {
        if (e->kind() == Expression::Kind::kUnaryNot) {
            auto operand = static_cast<const UnaryExpression *>(e)->operand();
            return (operandMatcher(operand));
        }
        return false;
    };

    std::function<Expression *(const Expression *)> rewriter =
        [&](const Expression *e) -> Expression * {
        auto operand = static_cast<const UnaryExpression *>(e)->operand();
        auto reducedExpr = operand->clone();

        if (reducedExpr->kind() == Expression::Kind::kUnaryNot) {
            auto castedExpr = static_cast<UnaryExpression *>(reducedExpr);
            reducedExpr = castedExpr->operand();
        } else if (reducedExpr->isRelExpr() && reducedExpr->kind() != Expression::Kind::kRelREG) {
            auto castedExpr = static_cast<RelationalExpression *>(reducedExpr);
            reducedExpr = reverseRelExpr(castedExpr);
        } else if (reducedExpr->isLogicalExpr()) {
            auto castedExpr = static_cast<LogicalExpression *>(reducedExpr);
            reducedExpr = reverseLogicalExpr(castedExpr);
        }
        // Rewrite the output of rewrite if possible
        return operandMatcher(reducedExpr)
                   ? RewriteVisitor::transform(reducedExpr, rootMatcher, rewriter)
                   : reducedExpr;
    };

    return RewriteVisitor::transform(expr, rootMatcher, rewriter);
}

Expression *ExpressionUtils::rewriteRelExpr(const Expression *expr) {
    ObjectPool* pool = expr->getObjPool();
    // Match relational expressions containing at least one airthmetic expr
    auto matcher = [](const Expression *e) -> bool {
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
    auto simplifyBoolOperand = [pool](RelationalExpression *relExpr,
                                       Expression *lExpr,
                                       Expression *rExpr) -> Expression * {
        QueryExpressionContext ctx(nullptr);
        if (rExpr->kind() == Expression::Kind::kConstant) {
            auto conExpr = static_cast<ConstantExpression *>(rExpr);
            auto val = conExpr->eval(ctx(nullptr));
            auto valType = val.type();
            // Rewrite to null if the expression contains any operand that is null
            if (valType == Value::Type::NULLVALUE) {
                return rExpr->clone();
            }
            if (relExpr->kind() == Expression::Kind::kRelEQ) {
                if (valType == Value::Type::BOOL) {
                    return val.getBool() ? lExpr->clone()
                                         : UnaryExpression::makeNot(pool, lExpr->clone());
                }
            } else if (relExpr->kind() == Expression::Kind::kRelNE) {
                if (valType == Value::Type::BOOL) {
                    return val.getBool() ? UnaryExpression::makeNot(pool, lExpr->clone())
                                         : lExpr->clone();
                }
            }
        }
        return nullptr;
    };

    std::function<Expression *(const Expression *)> rewriter =
        [&](const Expression *e) -> Expression * {
        auto exprCopy = e->clone();
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
        return RelationalExpression::makeKind(
            pool, relExpr->kind(), relLeftOperandExpr->clone(), relRightOperandExpr->clone());
    };

    return RewriteVisitor::transform(expr, matcher, rewriter);
}

Expression *ExpressionUtils::rewriteRelExprHelper(const Expression *expr,
                                                  Expression *&relRightOperandExpr) {
    ObjectPool* pool = expr->getObjPool();
    // TODO: Support rewrite mul/div expressoion after fixing overflow
    auto matcher = [](const Expression *e) -> bool {
        if (!e->isArithmeticExpr() || e->kind() == Expression::Kind::kMultiply ||
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
    auto lexpr = relRightOperandExpr->clone();
    const Expression *root = nullptr;
    Expression *rexpr = nullptr;

    // Use left operand as root
    if (ExpressionUtils::isEvaluableExpr(arithExpr->right())) {
        rexpr = arithExpr->right()->clone();
        root = arithExpr->left();
    } else {
        rexpr = arithExpr->left()->clone();
        root = arithExpr->right();
    }
    switch (kind) {
        case Expression::Kind::kAdd:
            relRightOperandExpr = ArithmeticExpression::makeAdd(pool, lexpr, rexpr);
            break;
        case Expression::Kind::kMinus:
            relRightOperandExpr = ArithmeticExpression::makeMinus(pool, lexpr, rexpr);
            break;
        // Unsupported arithm kind
        // case Expression::Kind::kMultiply:
        // case Expression::Kind::kDivision:
        default:
            LOG(FATAL) << "Unsupported expression kind: " << static_cast<uint8_t>(kind);
            break;
    }

    return rewriteRelExprHelper(root, relRightOperandExpr);
}

StatusOr<Expression*> ExpressionUtils::filterTransform(const Expression *filter) {
    auto rewrittenExpr = const_cast<Expression *>(filter);
    // Rewrite relational expression
    rewrittenExpr = rewriteRelExpr(rewrittenExpr);
    // Fold constant expression
    auto constantFoldRes = foldConstantExpr(rewrittenExpr);
    NG_RETURN_IF_ERROR(constantFoldRes);
    rewrittenExpr = constantFoldRes.value();
    // Reduce Unary expression
    rewrittenExpr = reduceUnaryNotExpr(rewrittenExpr);
    return rewrittenExpr;
}

void ExpressionUtils::pullAnds(Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
    auto *logic = static_cast<LogicalExpression *>(expr);
    std::vector<Expression*> operands;
    pullAndsImpl(logic, operands);
    logic->setOperands(std::move(operands));
}

void ExpressionUtils::pullOrs(Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
    auto *logic = static_cast<LogicalExpression *>(expr);
    std::vector<Expression*> operands;
    pullOrsImpl(logic, operands);
    logic->setOperands(std::move(operands));
}

void ExpressionUtils::pullAndsImpl(LogicalExpression *expr,
                                   std::vector<Expression*> &operands) {
    for (auto &operand : expr->operands()) {
        if (operand->kind() != Expression::Kind::kLogicalAnd) {
            operands.emplace_back(std::move(operand));
            continue;
        }
        pullAndsImpl(static_cast<LogicalExpression *>(operand), operands);
    }
}

void ExpressionUtils::pullOrsImpl(LogicalExpression *expr,
                                  std::vector<Expression*> &operands) {
    for (auto &operand : expr->operands()) {
        if (operand->kind() != Expression::Kind::kLogicalOr) {
            operands.emplace_back(std::move(operand));
            continue;
        }
        pullOrsImpl(static_cast<LogicalExpression *>(operand), operands);
    }
}

Expression* ExpressionUtils::flattenInnerLogicalAndExpr(const Expression *expr) {
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kLogicalAnd;
    };
    auto rewriter = [](const Expression *e) -> Expression * {
        pullAnds(const_cast<Expression *>(e));
        return e->clone();
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression* ExpressionUtils::flattenInnerLogicalOrExpr(const Expression *expr) {
    auto matcher = [](const Expression *e) -> bool {
        return e->kind() == Expression::Kind::kLogicalOr;
    };
    auto rewriter = [](const Expression *e) -> Expression * {
        pullOrs(const_cast<Expression *>(e));
        return e->clone();
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression* ExpressionUtils::flattenInnerLogicalExpr(const Expression *expr) {
    auto andFlattenExpr = flattenInnerLogicalAndExpr(expr);
    auto allFlattenExpr = flattenInnerLogicalOrExpr(andFlattenExpr);

    return allFlattenExpr;
}

// pick the subparts of expression that meet picker's criteria
void ExpressionUtils::splitFilter(const Expression *expr,
                                  std::function<bool(const Expression *)> picker,
                                  Expression **filterPicked,
                                  Expression **filterUnpicked) {
    ObjectPool* pool = expr->getObjPool();
    // Pick the non-LogicalAndExpr directly
    if (expr->kind() != Expression::Kind::kLogicalAnd) {
        if (picker(expr)) {
            *filterPicked = expr->clone();
        } else {
            *filterUnpicked = expr->clone();
        }
        return;
    }

    auto flattenExpr = ExpressionUtils::flattenInnerLogicalExpr(expr);
    DCHECK(flattenExpr->kind() == Expression::Kind::kLogicalAnd);
    auto *logicExpr = static_cast<LogicalExpression *>(flattenExpr);
    auto filterPickedPtr = LogicalExpression::makeAnd(pool);
    auto filterUnpickedPtr = LogicalExpression::makeAnd(pool);

    std::vector<Expression*> &operands = logicExpr->operands();
    for (auto iter = operands.begin(); iter != operands.end(); ++iter) {
        if (picker((*iter))) {
            filterPickedPtr->addOperand((*iter)->clone());
        } else {
            filterUnpickedPtr->addOperand((*iter)->clone());
        }
    }
    auto foldLogicalExpr = [](const LogicalExpression *e) -> Expression * {
        const auto &ops = e->operands();
        auto size = ops.size();
        if (size > 1) {
            return e->clone();
        } else if (size == 1) {
            return ops[0]->clone();
        } else {
            return nullptr;
        }
    };
    *filterPicked = foldLogicalExpr(filterPickedPtr);
    *filterUnpicked = foldLogicalExpr(filterUnpickedPtr);
}

Expression *ExpressionUtils::pushOrs(ObjectPool *pool, const std::vector<Expression *> &rels) {
    return pushImpl(pool, Expression::Kind::kLogicalOr, rels);
}

Expression *ExpressionUtils::pushAnds(ObjectPool *pool, const std::vector<Expression *> &rels) {
    return pushImpl(pool, Expression::Kind::kLogicalAnd, rels);
}

Expression *ExpressionUtils::pushImpl(ObjectPool *pool,
                                      Expression::Kind kind,
                                      const std::vector<Expression *> &rels) {
    DCHECK_GT(rels.size(), 1);
    DCHECK(kind == Expression::Kind::kLogicalOr || kind == Expression::Kind::kLogicalAnd);
    auto root = LogicalExpression::makeKind(pool, kind);
    root->addOperand(rels[0]->clone());
    root->addOperand(rels[1]->clone());
    for (size_t i = 2; i < rels.size(); i++) {
        auto l = LogicalExpression::makeKind(pool, kind);
        l->addOperand(root->clone());
        l->addOperand(rels[i]->clone());
        root = std::move(l);
    }
    return root;
}

Expression *ExpressionUtils::expandExpr(ObjectPool *pool, const Expression *expr) {
    auto kind = expr->kind();
    std::vector<Expression*> target;
    switch (kind) {
        case Expression::Kind::kLogicalOr: {
            const auto *logic = static_cast<const LogicalExpression *>(expr);
            for (const auto &e : logic->operands()) {
                if (e->kind() == Expression::Kind::kLogicalAnd) {
                    target.emplace_back(expandImplAnd(pool, e));
                } else {
                    target.emplace_back(expandExpr(pool, e));
                }
            }
            break;
        }
        case Expression::Kind::kLogicalAnd: {
            target.emplace_back(expandImplAnd(pool, expr));
            break;
        }
        default: { return expr->clone(); }
    }
    DCHECK_GT(target.size(), 0);
    if (target.size() == 1) {
        if (target[0]->kind() == Expression::Kind::kLogicalAnd) {
            const auto *logic = static_cast<const LogicalExpression *>(target[0]);
            const auto &ops = logic->operands();
            DCHECK_EQ(ops.size(), 2);
            if (ops[0]->kind() == Expression::Kind::kLogicalOr ||
                ops[1]->kind() == Expression::Kind::kLogicalOr) {
                return expandExpr(pool, target[0]);
            }
        }
        return std::move(target[0]);
    }
    return pushImpl(pool, kind, target);
}

Expression* ExpressionUtils::expandImplAnd(ObjectPool* pool, const Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
    const auto *logic = static_cast<const LogicalExpression *>(expr);
    DCHECK_EQ(logic->operands().size(), 2);
    std::vector<Expression*> subL;
    auto &ops = logic->operands();
    if (ops[0]->kind() == Expression::Kind::kLogicalOr) {
        auto target = expandImplOr(ops[0]);
        for (const auto &e : target) {
            subL.emplace_back(e->clone());
        }
    } else {
        subL.emplace_back(expandExpr(pool, std::move(ops[0])));
    }
    std::vector<Expression*> subR;
    if (ops[1]->kind() == Expression::Kind::kLogicalOr) {
        auto target = expandImplOr(ops[1]);
        for (const auto &e : target) {
            subR.emplace_back(e->clone());
        }
    } else {
        subR.emplace_back(expandExpr(pool, std::move(ops[1])));
    }

    DCHECK_GT(subL.size(), 0);
    DCHECK_GT(subR.size(), 0);
    std::vector<Expression*> target;
    for (auto &le : subL) {
        for (auto &re : subR) {
            auto l = LogicalExpression::makeAnd(pool);
            l->addOperand(le->clone());
            l->addOperand(re->clone());
            target.emplace_back(std::move(l));
        }
    }
    DCHECK_GT(target.size(), 0);
    if (target.size() == 1) {
        return std::move(target[0]);
    }
    return pushImpl(pool, Expression::Kind::kLogicalOr, target);
}

std::vector<Expression*> ExpressionUtils::expandImplOr(const Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
    const auto *logic = static_cast<const LogicalExpression *>(expr);
    std::vector<Expression*> exprs;
    auto &ops = logic->operands();
    for (const auto &op : ops) {
        if (op->kind() == Expression::Kind::kLogicalOr) {
            auto target = expandImplOr(op);
            for (const auto &e : target) {
                exprs.emplace_back(e->clone());
            }
        } else {
            exprs.emplace_back(op->clone());
        }
    }
    return exprs;
}

Status ExpressionUtils::checkAggExpr(const AggregateExpression *aggExpr) {
    auto func = aggExpr->name();
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
        if (propExpr->prop() == "*") {
            return Status::SemanticError("Could not apply aggregation function `%s' on `%s'",
                                         aggExpr->toString().c_str(),
                                         propExpr->toString().c_str());
        }
    }

    return Status::OK();
}

bool ExpressionUtils::findInnerRandFunction(const Expression *expr) {
    auto finder = [](const Expression *e) -> bool {
        if (e->kind() == Expression::Kind::kFunctionCall) {
            auto func = static_cast<const FunctionCallExpression *>(e)->name();
            std::transform(func.begin(), func.end(), func.begin(), ::tolower);
            return !func.compare("rand") || !func.compare("rand32") || !func.compare("rand64");
        }
        return false;
    };
    if (finder(expr)) {
        return true;
    }
    FindVisitor visitor(finder);
    const_cast<Expression *>(expr)->accept(&visitor);
    if (!visitor.results().empty()) {
        return true;
    }
    return false;
}

// Negate the given relational expr
RelationalExpression *ExpressionUtils::reverseRelExpr(RelationalExpression *expr) {
    ObjectPool *pool = expr->getObjPool();
    auto left = static_cast<RelationalExpression *>(expr)->left();
    auto right = static_cast<RelationalExpression *>(expr)->right();
    auto negatedKind = getNegatedRelExprKind(expr->kind());

    return RelationalExpression::makeKind(pool, negatedKind, left->clone(), right->clone());
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

LogicalExpression*ExpressionUtils::reverseLogicalExpr(LogicalExpression *expr) {
    ObjectPool* pool = expr->getObjPool();
    std::vector<Expression *> operands;
    if (expr->kind() == Expression::Kind::kLogicalAnd) {
        pullAnds(expr);
    } else {
        pullOrs(expr);
    }

    auto &flattenOperands = static_cast<LogicalExpression *>(expr)->operands();
    auto negatedKind = getNegatedLogicalExprKind(expr->kind());
    auto logic = LogicalExpression::makeKind(pool, negatedKind);

    // negate each item in the operands list
    for (auto &operand : flattenOperands) {
        auto tempExpr = UnaryExpression::makeNot(pool, operand);
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

// ++loopStep <= steps
Expression *ExpressionUtils::stepCondition(ObjectPool *pool,
                                           const std::string &loopStep,
                                           uint32_t steps) {
    return RelationalExpression::makeLE(
        pool,
        UnaryExpression::makeIncr(pool, VariableExpression::make(pool, loopStep)),
        ConstantExpression::make(pool, static_cast<int32_t>(steps)));
}

// size(var) != 0
Expression *ExpressionUtils::neZeroCondition(ObjectPool *pool, const std::string &var) {
    auto *args = ArgumentList::make(pool);
    args->addArgument(VariableExpression::make(pool, var));
    return RelationalExpression::makeNE(
        pool, FunctionCallExpression::make(pool, "size", args), ConstantExpression::make(pool, 0));
}

// size(var) == 0
Expression *ExpressionUtils::zeroCondition(ObjectPool *pool, const std::string &var) {
    auto *args = ArgumentList::make(pool);
    args->addArgument(VariableExpression::make(pool, var));
    return RelationalExpression::makeEQ(
        pool, FunctionCallExpression::make(pool, "size", args), ConstantExpression::make(pool, 0));
}

// var == value
Expression *ExpressionUtils::equalCondition(ObjectPool *pool,
                                            const std::string &var,
                                            const Value &value) {
    return RelationalExpression::makeEQ(
        pool, VariableExpression::make(pool, var), ConstantExpression::make(pool, value));
}
}   // namespace graph
}   // namespace nebula
