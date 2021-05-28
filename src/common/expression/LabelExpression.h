/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_LABELEXPRESSION_H_
#define COMMON_EXPRESSION_LABELEXPRESSION_H_

#include "common/expression/Expression.h"

// The LabelExpression use for name_label in base_expression,
// need to rewrite it based on the usage scenario
namespace nebula {

class LabelExpression: public Expression {
public:
    explicit LabelExpression(const std::string& name = "")
        : Expression(Kind::kLabel), name_(name) {}

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    const std::string &name() const {
        return name_;
    }

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<LabelExpression>(name());
    }

protected:
    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

    std::string name_;
    Value result_;
};

}  // namespace nebula
#endif   // COMMON_EXPRESSION_LABELEXPRESSION_H_
