/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_VERTEXEXPRESSION_H_
#define COMMON_EXPRESSION_VERTEXEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

/**
 * This is an internal type of expression to denote a Vertex value.
 * It has no corresponding rules in parser.
 * It is generated from LabelExpression during semantic analysis
 * and expression rewrite.
 */
class VertexExpression final : public Expression {
public:
    VertexExpression() : Expression(Kind::kVertex) {}

    const Value& eval(ExpressionContext &ctx) override;

    void accept(ExprVisitor *visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<VertexExpression>();
    }

    std::string toString() const override {
        return "VERTEX";
    }

    bool operator==(const Expression &expr) const override {
        return kind() == expr.kind();
    }

private:
    void writeTo(Encoder &encoder) const override {
        encoder << kind();
    }

    void resetFrom(Decoder&) override {}

private:
    Value                                   result_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_VERTEXEXPRESSION_H_
