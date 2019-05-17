/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_COMMON_H_
#define STORAGE_COMMON_H_

#include "base/Base.h"
#include "filter/Expressions.h"

namespace nebula {
namespace storage {

class CommonUtils {
public:
    /**
     * Return false if expression is invalid.
     * */
    static bool checkExp(const Expression* exp) {
        switch (exp->kind()) {
            case Expression::kPrimary:
                return true;
            case Expression::kFunctionCall:
                // TODO(heng): we should support it in the future.
                return false;
            case Expression::kUnary: {
                auto* unaExp = static_cast<const UnaryExpression*>(exp);
                return checkExp(unaExp->operand());
            }
            case Expression::kTypeCasting: {
                auto* typExp = static_cast<const TypeCastingExpression*>(exp);
                return checkExp(typExp->operand());
            }
            case Expression::kArithmetic: {
                auto* ariExp = static_cast<const ArithmeticExpression*>(exp);
                return checkExp(ariExp->left()) && checkExp(ariExp->right());
            }
            case Expression::kRelational: {
                auto* relExp = static_cast<const RelationalExpression*>(exp);
                return checkExp(relExp->left()) && checkExp(relExp->right());
            }
            case Expression::kLogical: {
                auto* logExp = static_cast<const LogicalExpression*>(exp);
                return checkExp(logExp->left()) && checkExp(logExp->right());
            }
            case Expression::kSourceProp:
                // TODO(heng): we should check if related prop existed or not.
                return true;
            case Expression::kEdgeRank:
            case Expression::kEdgeDstId:
            case Expression::kEdgeSrcId:
            case Expression::kEdgeType:
                return true;
            case Expression::kEdgeProp:
                // TODO(heng): we should check if related prop existed or not.
                return true;
            case Expression::kVariableProp:
            case Expression::kDestProp:
            case Expression::kInputProp:
                return false;
            default:
                LOG(FATAL) << "Unsupport expression type! kind = "
                           << std::to_string(static_cast<uint8_t>(exp->kind()));
        }
        return true;
    }
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COMMON_H_

