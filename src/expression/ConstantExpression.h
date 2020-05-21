/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXPRESSION_CONSTANTEXPRESSION_H_
#define EXPRESSION_CONSTANTEXPRESSION_H_

#include "base/Base.h"
#include "expression/Expression.h"

namespace nebula {

class ConstantExpression : public Expression {
public:
    explicit ConstantExpression(Value v) : Expression(Kind::kConstant), val_(std::move(v)) {}

    Value eval() const override {
        return val_;
    }

    std::string encode() const override;

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }

private:
    Value val_;
};

}   // namespace nebula
#endif   // EXPRESSION_CONSTANTEXPRESSION_H_
