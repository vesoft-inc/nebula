/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXPRESSION_TYPECASTINGEXPRESSION_H_
#define EXPRESSION_TYPECASTINGEXPRESSION_H_

#include "expression/Expression.h"

namespace nebula {
class TypeCastingExpression final : public Expression {
public:
    TypeCastingExpression(Value::Type vType,
                          Expression* operand)
        : Expression(Type::EXP_TYPE_CASTING), vType_(vType) {
        operand_.reset(operand);
    }

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }

private:
    Value::Type                                 vType_;
    std::unique_ptr<Expression>                 operand_;
};
}  // namespace nebula
#endif
