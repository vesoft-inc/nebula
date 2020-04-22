/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "expression/Expression.h"

namespace nebula {
std::ostream& operator<<(std::ostream& os, Expression::Type type) {
    switch (type) {
        case Expression::Type::EXP_CONSTANT:
            os << "Constant";
            break;
        case Expression::Type::EXP_ADD:
            os << "Add";
            break;
        case Expression::Type::EXP_MINUS:
            os << "Minus";
            break;
        case Expression::Type::EXP_MULTIPLY:
            os << "Multiply";
            break;
        case Expression::Type::EXP_DIVIDE:
            os << "Divide";
            break;
        case Expression::Type::EXP_MOD:
            os << "Mod";
            break;
        case Expression::Type::EXP_UNARY_PLUS:
            os << "UnaryPlus";
            break;
        case Expression::Type::EXP_UNARY_NEGATE:
            os << "UnaryNegate";
            break;
        case Expression::Type::EXP_UNARY_NOT:
            os << "UnaryNot";
            break;
        case Expression::Type::EXP_REL_EQ:
            os << "Equal";
            break;
        case Expression::Type::EXP_REL_NE:
            os << "NotEuqal";
            break;
        case Expression::Type::EXP_REL_LT:
            os << "LessThan";
            break;
        case Expression::Type::EXP_REL_LE:
            os << "LessEqual";
            break;
        case Expression::Type::EXP_REL_GT:
            os << "GreaterThan";
            break;
        case Expression::Type::EXP_REL_GE:
            os << "GreaterEqual";
            break;
        case Expression::Type::EXP_LOGICAL_AND:
            os << "LogicalAnd";
            break;
        case Expression::Type::EXP_LOGICAL_OR:
            os << "LogicalOr";
            break;
        case Expression::Type::EXP_LOGICAL_XOR:
            os << "LogicalXor";
            break;
        case Expression::Type::EXP_TYPE_CASTING:
            os << "TypeCasting";
            break;
        case Expression::Type::EXP_FUNCTION_CALL:
            os << "FunctionCall";
            break;
        case Expression::Type::EXP_ALIAS_PROPERTY:
            os << "AliasProp";
            break;
        case Expression::Type::EXP_INPUT_PROPERTY:
            os << "InputProp";
            break;
        case Expression::Type::EXP_VAR_PROPERTY:
            os << "VarProp";
            break;
        case Expression::Type::EXP_DST_PROPERTY:
            os << "DstProp";
            break;
        case Expression::Type::EXP_SRC_PROPERTY:
            os << "SrcProp";
            break;
        case Expression::Type::EXP_EDGE_SRC:
            os << "EdgeSrc";
            break;
        case Expression::Type::EXP_EDGE_TYPE:
            os << "EdgeType";
            break;
        case Expression::Type::EXP_EDGE_RANK:
            os << "EdgeRank";
            break;
        case Expression::Type::EXP_EDGE_DST:
            os << "EdgeDst";
            break;
        case Expression::Type::EXP_UUID:
            os << "UUID";
            break;
    }
    return os;
}
}  // namespace nebula
