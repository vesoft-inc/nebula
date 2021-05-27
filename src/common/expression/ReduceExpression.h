/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_REDUCEEXPRESSIONEXPRESSION_H_
#define COMMON_EXPRESSION_REDUCEEXPRESSIONEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

class ReduceExpression final : public Expression {
    friend class Expression;

public:
    explicit ReduceExpression(std::string* accumulator = nullptr,
                                 Expression* initial = nullptr,
                                 std::string* innerVar = nullptr,
                                 Expression* collection = nullptr,
                                 Expression* mapping = nullptr)
        : Expression(Kind::kReduce),
          accumulator_(accumulator),
          initial_(initial),
          innerVar_(innerVar),
          collection_(collection),
          mapping_(mapping) {}

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    std::string rawString() const override {
        return hasOriginString() ? *originString_ : "";
    }

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override;

    const std::string* accumulator() const {
        return accumulator_.get();
    }

    std::string* accumulator() {
        return accumulator_.get();
    }

    const Expression* initial() const {
        return initial_.get();
    }

    Expression* initial() {
        return initial_.get();
    }

    const std::string* innerVar() const {
        return innerVar_.get();
    }

    std::string* innerVar() {
        return innerVar_.get();
    }

    const Expression* collection() const {
        return collection_.get();
    }

    Expression* collection() {
        return collection_.get();
    }

    const Expression* mapping() const {
        return mapping_.get();
    }

    Expression* mapping() {
        return mapping_.get();
    }

    void setAccumulator(std::string* name) {
        accumulator_.reset(name);
    }

    void setInitial(Expression* expr) {
        initial_.reset(expr);
    }

    void setInnerVar(std::string* name) {
        innerVar_.reset(name);
    }

    void setCollection(Expression* expr) {
        collection_.reset(expr);
    }

    void setMapping(Expression* expr) {
        mapping_.reset(expr);
    }

    void setOriginString(std::string* s) {
        originString_.reset(s);
    }

    bool hasOriginString() const {
        return originString_ != nullptr;
    }

private:
    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

    std::unique_ptr<std::string> accumulator_;
    std::unique_ptr<Expression> initial_;
    std::unique_ptr<std::string> innerVar_;
    std::unique_ptr<Expression> collection_;
    std::unique_ptr<Expression> mapping_;
    std::unique_ptr<std::string> originString_;
    Value result_;
};

}   // namespace nebula
#endif   // EXPRESSION_REDUCEEXPRESSIONEXPRESSION_H_
