/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_CONSTANTEXPRESSION_H_
#define COMMON_EXPRESSION_CONSTANTEXPRESSION_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"

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
#endif   // COMMON_EXPRESSION_CONSTANTEXPRESSION_H_
