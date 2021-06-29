/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_CASEEXPRESSION_H_
#define COMMON_EXPRESSION_CASEEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

class CaseList final {
public:
    static CaseList* make(ObjectPool* pool, size_t sz = 0) {
        DCHECK(!!pool);
        return pool->add(new CaseList(sz));
    }

    void add(Expression* when, Expression* then) {
        items_.emplace_back(when, then);
    }

    auto items() {
        return items_;
    }

    struct Item {
        Item(Expression* wh, Expression* th) : when(wh), then(th) {}
        Expression* when{nullptr};
        Expression* then{nullptr};
    };

private:
    CaseList() = default;
    explicit CaseList(size_t sz) {
        items_.reserve(sz);
    }

private:
    std::vector<Item> items_;
};

class CaseExpression final : public Expression {
    friend class Expression;

public:
    CaseExpression& operator=(const CaseExpression& rhs) = delete;
    CaseExpression& operator=(CaseExpression&&) = delete;

    static CaseExpression* make(ObjectPool* pool,
                                CaseList* cases = nullptr,
                                bool isGeneric = true) {
        DCHECK(!!pool);
        return !cases ? pool->add(new CaseExpression(pool))
                      : pool->add(new CaseExpression(pool, cases, isGeneric));
    }

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    void setGeneric(bool isGeneric = true) {
        isGeneric_ = isGeneric;
    }

    void setCondition(Expression* cond) {
        condition_ = cond;
    }

    void setDefault(Expression* defaultResult) {
        default_ = defaultResult;
    }

    void setCases(CaseList* cases) {
        cases_ = cases->items();
    }

    void setWhen(size_t index, Expression* when) {
        DCHECK_LT(index, cases_.size());
        cases_[index].when = when;
    }

    void setThen(size_t index, Expression* then) {
        DCHECK_LT(index, cases_.size());
        cases_[index].then = then;
    }

    bool hasCondition() const {
        return condition_ != nullptr;
    }

    bool hasDefault() const {
        return default_ != nullptr;
    }

    size_t numCases() const {
        return cases_.size();
    }

    bool isGeneric() const {
        return isGeneric_;
    }

    Expression* condition() const {
        return condition_;
    }

    Expression* defaultResult() const {
        return default_;
    }

    const std::vector<CaseList::Item>& cases() const {
        return cases_;
    }

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        auto caseList = CaseList::make(pool_, cases_.size());
        for (const auto& whenThen : cases_) {
            caseList->add(whenThen.when->clone(), whenThen.then->clone());
        }
        auto expr = CaseExpression::make(pool_, caseList, isGeneric_);
        auto cond = condition_ != nullptr ? condition_->clone() : nullptr;
        auto defaultResult = default_ != nullptr ? default_->clone() : nullptr;
        expr->setCondition(cond);
        expr->setDefault(defaultResult);
        return expr;
    }

private:
    explicit CaseExpression(ObjectPool* pool) : Expression(pool, Kind::kCase), isGeneric_(true) {}

    explicit CaseExpression(ObjectPool* pool, CaseList* cases, bool isGeneric)
        : Expression(pool, Kind::kCase), isGeneric_(isGeneric) {
        cases_ = cases->items();
    }
    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

private:
    bool isGeneric_;
    std::vector<CaseList::Item> cases_;
    Expression* condition_{nullptr};
    Expression* default_{nullptr};
    Value result_;
};

}   // namespace nebula
#endif   // COMMON_EXPRESSION_CASEEXPRESSION_H_
