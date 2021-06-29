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
    static VertexExpression *make(ObjectPool *pool) {
        DCHECK(!!pool);
        return pool->add(new VertexExpression(pool));
    }

    const Value &eval(ExpressionContext &ctx) override;

    void accept(ExprVisitor *visitor) override;

    Expression* clone() const override {
        return VertexExpression::make(pool_);
    }

    std::string toString() const override {
        return "VERTEX";
    }

    bool operator==(const Expression &expr) const override {
        return kind() == expr.kind();
    }

private:
    explicit VertexExpression(ObjectPool *pool) : Expression(pool, Kind::kVertex) {}

    void writeTo(Encoder &encoder) const override {
        encoder << kind();
    }

    void resetFrom(Decoder &) override {}

private:
    Value                                   result_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_VERTEXEXPRESSION_H_
