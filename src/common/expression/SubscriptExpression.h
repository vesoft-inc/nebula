/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_SUBSCRIPTEXPRESSION_H_
#define COMMON_EXPRESSION_SUBSCRIPTEXPRESSION_H_

#include <memory>
#include "common/expression/BinaryExpression.h"

namespace nebula {

class SubscriptExpression final : public BinaryExpression {
public:
    explicit SubscriptExpression(Expression *lhs = nullptr,
                                 Expression *rhs = nullptr)
        : BinaryExpression(Kind::kSubscript, lhs, rhs) {}

    const Value& eval(ExpressionContext &ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<SubscriptExpression>(left()->clone().release(),
                                                     right()->clone().release());
    }

private:
    Value                               result_;
};

class SubscriptRangeExpression final : public Expression {
public:
    // for decode ctor
    SubscriptRangeExpression()
        : Expression(Kind::kSubscriptRange),
          list_(nullptr),
          lo_(nullptr),
          hi_(nullptr) {}
    SubscriptRangeExpression(Expression *list, Expression *lo, Expression *hi)
        : Expression(Kind::kSubscriptRange),
          list_(DCHECK_NOTNULL(list)),
          lo_(lo),
          hi_(hi) {
              DCHECK(!(lo_ == nullptr && hi_ == nullptr));
          }

    const Value& eval(ExpressionContext &ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<SubscriptRangeExpression>(
            list_->clone().release(),
            lo_ == nullptr ? nullptr : lo_->clone().release(),
            hi_ == nullptr ? nullptr : hi_->clone().release());
    }

    bool operator==(const Expression& rhs) const override;

    const Expression* list() const {
        return list_.get();
    }

    Expression* list() {
        return list_.get();
    }

    void setList(Expression *list) {
        list_.reset(list);
    }

    const Expression* lo() const {
        return lo_.get();
    }

    Expression* lo() {
        return lo_.get();
    }

    void setLo(Expression *lo) {
        lo_.reset(lo);
    }

    const Expression* hi() const {
        return hi_.get();
    }

    Expression* hi() {
        return hi_.get();
    }

    void setHi(Expression *hi) {
        hi_.reset(hi);
    }

private:
    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

    // It's must list typed
    std::unique_ptr<Expression> list_;

    // range is [lo, hi), they could be any integer
    std::unique_ptr<Expression> lo_;
    std::unique_ptr<Expression> hi_;

    // runtime cache
    Value                       result_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_SUBSCRIPTEXPRESSION_H_
