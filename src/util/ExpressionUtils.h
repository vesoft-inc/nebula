/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _UTIL_EXPRESSION_UTILS_H_
#define _UTIL_EXPRESSION_UTILS_H_

#include "common/expression/BinaryExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/LabelAttributeExpression.h"

namespace nebula {
namespace graph {

class ExpressionUtils {
public:
    explicit ExpressionUtils(...) = delete;

    // return true for continue, false return directly
    using Visitor = std::function<bool(const Expression*)>;
    using MutableVisitor = std::function<bool(Expression*)>;

    // preorder traverse in fact for tail call optimization
    // if want to do some thing like eval, don't try it
    template <typename T,
              typename V,
              typename = std::enable_if_t<std::is_same<std::remove_const_t<T>, Expression>::value>>
    static bool traverse(T* expr, V visitor) {
        if (!visitor(expr)) {
            return false;
        }
        switch (expr->kind()) {
            case Expression::Kind::kDstProperty:
            case Expression::Kind::kSrcProperty:
            case Expression::Kind::kTagProperty:
            case Expression::Kind::kEdgeProperty:
            case Expression::Kind::kEdgeSrc:
            case Expression::Kind::kEdgeType:
            case Expression::Kind::kEdgeRank:
            case Expression::Kind::kEdgeDst:
            case Expression::Kind::kInputProperty:
            case Expression::Kind::kVarProperty:
            case Expression::Kind::kVertex:
            case Expression::Kind::kEdge:
            case Expression::Kind::kUUID:
            case Expression::Kind::kVar:
            case Expression::Kind::kVersionedVar:
            case Expression::Kind::kLabelAttribute:
            case Expression::Kind::kConstant: {
                return true;
            }
            case Expression::Kind::kAdd:
            case Expression::Kind::kMinus:
            case Expression::Kind::kMultiply:
            case Expression::Kind::kDivision:
            case Expression::Kind::kMod:
            case Expression::Kind::kRelEQ:
            case Expression::Kind::kRelNE:
            case Expression::Kind::kRelLT:
            case Expression::Kind::kRelLE:
            case Expression::Kind::kRelGT:
            case Expression::Kind::kRelGE:
            case Expression::Kind::kRelIn:
            case Expression::Kind::kRelNotIn:
            case Expression::Kind::kContains:
            case Expression::Kind::kLogicalAnd:
            case Expression::Kind::kLogicalOr:
            case Expression::Kind::kLogicalXor: {
                using ToType = keep_const_t<T, BinaryExpression>;
                auto biExpr = static_cast<ToType*>(expr);
                if (!traverse(biExpr->left(), visitor)) {
                    return false;
                }
                return traverse(biExpr->right(), visitor);
            }
            case Expression::Kind::kUnaryIncr:
            case Expression::Kind::kUnaryDecr:
            case Expression::Kind::kUnaryPlus:
            case Expression::Kind::kUnaryNegate:
            case Expression::Kind::kUnaryNot: {
                using ToType = keep_const_t<T, UnaryExpression>;
                auto unaryExpr = static_cast<ToType*>(expr);
                return traverse(unaryExpr->operand(), visitor);
            }
            case Expression::Kind::kTypeCasting: {
                using ToType = keep_const_t<T, TypeCastingExpression>;
                auto typeCastingExpr = static_cast<ToType*>(expr);
                return traverse(typeCastingExpr->operand(), visitor);
            }
            case Expression::Kind::kFunctionCall: {
                using ToType = keep_const_t<T, FunctionCallExpression>;
                auto funcExpr = static_cast<ToType*>(expr);
                for (auto& arg : funcExpr->args()->args()) {
                    if (!traverse(arg.get(), visitor)) {
                        return false;
                    }
                }
                return true;
            }
            case Expression::Kind::kList:   // FIXME(dutor)
            case Expression::Kind::kSet:
            case Expression::Kind::kMap:
            case Expression::Kind::kSubscript:
            case Expression::Kind::kAttribute:
            case Expression::Kind::kLabel: {
                return false;
            }
        }
        DLOG(FATAL) << "Impossible expression kind " << static_cast<int>(expr->kind());
        return false;
    }

    static inline bool isKindOf(const Expression* expr,
                                const std::unordered_set<Expression::Kind>& expected) {
        return expected.find(expr->kind()) != expected.end();
    }

    // null for not found
    static const Expression* findAnyKind(const Expression* self,
                                         const std::unordered_set<Expression::Kind>& expected) {
        const Expression* found = nullptr;
        traverse(self, [&expected, &found](const Expression* expr) -> bool {
            if (isKindOf(expr, expected)) {
                found = expr;
                return false;   // Already find so return now
            }
            return true;   // Not find so continue traverse
        });
        return found;
    }

    // Find all expression fit any kind
    // Empty for not found any one
    static std::vector<const Expression*> findAnyKindInAll(
        const Expression* self,
        const std::unordered_set<Expression::Kind>& expected) {
        std::vector<const Expression*> exprs;
        traverse(self, [&expected, &exprs](const Expression* expr) -> bool {
            if (isKindOf(expr, expected)) {
                exprs.emplace_back(expr);
            }
            return true;   // Not return always to traverse entire expression tree
        });
        return exprs;
    }

    static bool hasAnyKind(const Expression* expr,
                           const std::unordered_set<Expression::Kind>& expected) {
        return findAnyKind(expr, expected) != nullptr;
    }

    // Require data from input/variable
    static bool hasInput(const Expression* expr) {
        return hasAnyKind(expr,
                          {Expression::Kind::kInputProperty,
                           Expression::Kind::kVarProperty,
                           Expression::Kind::kVar,
                           Expression::Kind::kVersionedVar});
    }

    // require data from graph storage
    static const Expression* findStorage(const Expression* expr) {
        return findAnyKind(expr,
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

    static std::vector<const Expression*> findAllStorage(const Expression* expr) {
        return findAnyKindInAll(expr,
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

    static std::vector<const Expression*> findAllInputVariableProp(const Expression* expr) {
        return findAnyKindInAll(expr,
                                {Expression::Kind::kInputProperty, Expression::Kind::kVarProperty});
    }

    static bool hasStorage(const Expression* expr) {
        return findStorage(expr) != nullptr;
    }

    static bool isStorage(const Expression* expr) {
        return isKindOf(expr,
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

    static bool isConstExpr(const Expression* expr) {
        return !hasAnyKind(expr,
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

    // clone expression
    static std::unique_ptr<Expression> clone(const Expression* expr) {
        // TODO(shylock) optimize
        if (expr == nullptr) {
            return nullptr;
        }
        return CHECK_NOTNULL(Expression::decode(expr->encode()));
    }

    // determine the detail about symbol property expression
    template <typename To,
              typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                          std::is_same<To, TagPropertyExpression>::value>>
    static void rewriteLabelAttribute(Expression* expr) {
        traverse(expr, [](Expression* current) -> bool {
            switch (current->kind()) {
                case Expression::Kind::kDstProperty:
                case Expression::Kind::kSrcProperty:
                case Expression::Kind::kLabelAttribute:
                case Expression::Kind::kTagProperty:
                case Expression::Kind::kEdgeProperty:
                case Expression::Kind::kEdgeSrc:
                case Expression::Kind::kEdgeType:
                case Expression::Kind::kEdgeRank:
                case Expression::Kind::kEdgeDst:
                case Expression::Kind::kVertex:
                case Expression::Kind::kEdge:
                case Expression::Kind::kInputProperty:
                case Expression::Kind::kVarProperty:
                case Expression::Kind::kUUID:
                case Expression::Kind::kVar:
                case Expression::Kind::kVersionedVar:
                case Expression::Kind::kConstant: {
                    return true;
                }
                case Expression::Kind::kAdd:
                case Expression::Kind::kMinus:
                case Expression::Kind::kMultiply:
                case Expression::Kind::kDivision:
                case Expression::Kind::kMod:
                case Expression::Kind::kRelEQ:
                case Expression::Kind::kRelNE:
                case Expression::Kind::kRelLT:
                case Expression::Kind::kRelLE:
                case Expression::Kind::kRelGT:
                case Expression::Kind::kRelGE:
                case Expression::Kind::kRelIn:
                case Expression::Kind::kRelNotIn:
                case Expression::Kind::kContains:
                case Expression::Kind::kLogicalAnd:
                case Expression::Kind::kLogicalOr:
                case Expression::Kind::kLogicalXor: {
                    auto* biExpr = static_cast<BinaryExpression*>(current);
                    if (biExpr->left()->kind() == Expression::Kind::kLabelAttribute) {
                        auto* laExpr = static_cast<LabelAttributeExpression*>(biExpr->left());
                        biExpr->setLeft(rewriteLabelAttribute<To>(laExpr));
                    }
                    if (biExpr->right()->kind() == Expression::Kind::kLabelAttribute) {
                        auto* laExpr = static_cast<LabelAttributeExpression*>(biExpr->right());
                        biExpr->setRight(rewriteLabelAttribute<To>(laExpr));
                    }
                    return true;
                }
                case Expression::Kind::kUnaryIncr:
                case Expression::Kind::kUnaryDecr:
                case Expression::Kind::kUnaryPlus:
                case Expression::Kind::kUnaryNegate:
                case Expression::Kind::kUnaryNot: {
                    auto* unaryExpr = static_cast<UnaryExpression*>(current);
                    if (unaryExpr->operand()->kind() == Expression::Kind::kLabelAttribute) {
                        auto* laExpr =
                            static_cast<LabelAttributeExpression*>(unaryExpr->operand());
                        unaryExpr->setOperand(rewriteLabelAttribute<To>(laExpr));
                    }
                    return true;
                }
                case Expression::Kind::kTypeCasting: {
                    auto* typeCastingExpr = static_cast<TypeCastingExpression*>(current);
                    if (typeCastingExpr->operand()->kind() == Expression::Kind::kLabelAttribute) {
                        auto* laExpr =
                            static_cast<LabelAttributeExpression*>(typeCastingExpr->operand());
                        typeCastingExpr->setOperand(rewriteLabelAttribute<To>(laExpr));
                    }
                    return true;
                }
                case Expression::Kind::kFunctionCall: {
                    auto* funcExpr = static_cast<FunctionCallExpression*>(current);
                    for (auto& arg : funcExpr->args()->args()) {
                        if (arg->kind() == Expression::Kind::kLabelAttribute) {
                            auto* laExpr = static_cast<LabelAttributeExpression*>(arg.get());
                            arg.reset(rewriteLabelAttribute<To>(laExpr));
                        }
                    }
                    return true;
                }
                case Expression::Kind::kList:   // FIXME(dutor)
                case Expression::Kind::kSet:
                case Expression::Kind::kMap:
                case Expression::Kind::kSubscript:
                case Expression::Kind::kAttribute:
                case Expression::Kind::kLabel: {
                    return false;
                }
            }   // switch
            DLOG(FATAL) << "Impossible expression kind " << static_cast<int>(current->kind());
            return false;
        });   // traverse
    }

    template <typename To,
              typename = std::enable_if_t<std::is_same<To, EdgePropertyExpression>::value ||
                                          std::is_same<To, TagPropertyExpression>::value>>
    static To* rewriteLabelAttribute(LabelAttributeExpression* expr) {
        return new To(new std::string(std::move(*expr->left()->name())),
                      new std::string(std::move(*expr->right()->name())));
    }

private:
    // keep const or non-const with T
    template <typename T, typename To>
    using keep_const_t = std::conditional_t<std::is_const<T>::value,
                                            const std::remove_const_t<To>,
                                            std::remove_const_t<To>>;
};

}   // namespace graph
}   // namespace nebula

#endif  // _UTIL_EXPRESSION_UTILS_H_
