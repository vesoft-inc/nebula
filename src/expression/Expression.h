/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXPRESSION_EXPRESSION_H_
#define EXPRESSION_EXPRESSION_H_

#include "base/Base.h"
#include "datatypes/Value.h"

namespace nebula {

class Expression {
public:
    enum class Kind {
        kConstant,

        kAdd,
        kMinus,
        kMultiply,
        kDivision,
        kMod,

        kUnaryPlus,
        kUnaryNegate,
        kUnaryNot,

        kRelEQ,
        kRelNE,
        kRelLT,
        kRelLE,
        kRelGT,
        kRelGE,

        kLogicalAnd,
        kLogicalOr,
        kLogicalXor,

        kTypeCasting,

        kFunctionCall,

        kAliasProperty,
        kInputProperty,
        kVarProperty,
        kDstProperty,
        kSrcProperty,
        kEdgeSrc,
        kEdgeType,
        kEdgeRank,
        kEdgeDst,

        kUUID,
    };

    explicit Expression(Kind kind) : kind_(kind) {}

    virtual ~Expression() = default;

    Kind kind() const {
        return kind_;
    }

    virtual Value eval() const = 0;

    virtual std::string toString() const = 0;

    virtual std::string encode() const = 0;

    virtual std::string decode() const = 0;

protected:
    Kind kind_;
};

std::ostream& operator<<(std::ostream& os, Expression::Kind kind);
}   // namespace nebula
#endif   // EXPRESSION_EXPRESSION_H_
