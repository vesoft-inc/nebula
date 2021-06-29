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
    static SubscriptExpression* make(ObjectPool* pool,
                                     Expression* lhs = nullptr,
                                     Expression* rhs = nullptr) {
        DCHECK(!!pool);
        return pool->add(new SubscriptExpression(pool, lhs, rhs));
    }

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        return SubscriptExpression::make(pool_, left()->clone(), right()->clone());
    }

private:
    explicit SubscriptExpression(ObjectPool* pool,
                                 Expression* lhs = nullptr,
                                 Expression* rhs = nullptr)
        : BinaryExpression(pool, Kind::kSubscript, lhs, rhs) {}

private:
    Value result_;
};

class SubscriptRangeExpression final : public Expression {
public:
    static SubscriptRangeExpression* make(ObjectPool* pool,
                                          Expression* list = nullptr,
                                          Expression* lo = nullptr,
                                          Expression* hi = nullptr) {
        DCHECK(!!pool);
        return !list && !lo && !hi ? pool->add(new SubscriptRangeExpression(pool))
                                   : pool->add(new SubscriptRangeExpression(pool, list, lo, hi));
    }

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        return SubscriptRangeExpression::make(pool_,
                                              list_->clone(),
                                              lo_ == nullptr ? nullptr : lo_->clone(),
                                              hi_ == nullptr ? nullptr : hi_->clone());
    }

    bool operator==(const Expression& rhs) const override;

    const Expression* list() const {
        return list_;
    }

    Expression* list() {
        return list_;
    }

    void setList(Expression* list) {
        list_ = list;
    }

    const Expression* lo() const {
        return lo_;
    }

    Expression* lo() {
        return lo_;
    }

    void setLo(Expression* lo) {
        lo_ = lo;
    }

    const Expression* hi() const {
        return hi_;
    }

    Expression* hi() {
        return hi_;
    }

    void setHi(Expression* hi) {
        hi_ = hi;
    }

private:
    // for decode ctor
    explicit SubscriptRangeExpression(ObjectPool* pool)
        : Expression(pool, Kind::kSubscriptRange), list_(nullptr), lo_(nullptr), hi_(nullptr) {}

    explicit SubscriptRangeExpression(ObjectPool* pool,
                                      Expression* list,
                                      Expression* lo,
                                      Expression* hi)
        : Expression(pool, Kind::kSubscriptRange), list_(DCHECK_NOTNULL(list)), lo_(lo), hi_(hi) {
        DCHECK(!(lo_ == nullptr && hi_ == nullptr));
    }
    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

private:
    // It's must be list type
    Expression* list_;

    // range is [lo, hi), they could be any integer
    Expression* lo_;
    Expression* hi_;

    // runtime cache
    Value result_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_SUBSCRIPTEXPRESSION_H_
