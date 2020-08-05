/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_CONTAINEREXPRESSION_H_
#define COMMON_EXPRESSION_CONTAINEREXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

class ExpressionList final {
public:
    ExpressionList() = default;

    ExpressionList& add(Expression *expr) {
        items_.emplace_back(expr);
        return *this;
    }

    auto get() && {
        return std::move(items_);
    }

private:
    std::vector<std::unique_ptr<Expression>>    items_;
};


class MapItemList final {
public:
    MapItemList() = default;

    MapItemList& add(std::string *key, Expression *value) {
        items_.emplace_back(key, value);
        return *this;
    }

    auto get() && {
        return std::move(items_);
    }

private:
    using Pair = std::pair<std::unique_ptr<std::string>, std::unique_ptr<Expression>>;
    std::vector<Pair>                           items_;
};


class ListExpression final : public Expression {
public:
    ListExpression() : Expression(Kind::kList) {
    }

    explicit ListExpression(ExpressionList *items) : Expression(Kind::kList) {
        items_ = std::move(*items).get();
        delete items;
    }

    const Value& eval(ExpressionContext &ctx) override;

    size_t size() const {
        return items_.size();
    }

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

private:
    void writeTo(Encoder &encoder) const override;

    void resetFrom(Decoder &decoder) override;

private:
    std::vector<std::unique_ptr<Expression>>    items_;
    Value                                       result_;
};


class SetExpression final : public Expression {
public:
    SetExpression() : Expression(Kind::kSet) {
    }

    explicit SetExpression(ExpressionList *items) : Expression(Kind::kSet) {
        items_ = std::move(*items).get();
        delete items;
    }

    const Value& eval(ExpressionContext &ctx) override;

    size_t size() const {
        return items_.size();
    }

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

private:
    void writeTo(Encoder &encoder) const override;

    void resetFrom(Decoder &decoder) override;

private:
    std::vector<std::unique_ptr<Expression>>    items_;
    Value                                       result_;
};


class MapExpression final : public Expression {
public:
    MapExpression() : Expression(Kind::kMap) {
    }

    explicit MapExpression(MapItemList *items) : Expression(Kind::kMap) {
        items_ = std::move(*items).get();
        delete items;
    }

    const Value& eval(ExpressionContext &ctx) override;

    size_t size() const {
        return items_.size();
    }

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

private:
    void writeTo(Encoder &encoder) const override;

    void resetFrom(Decoder &decoder) override;

private:
    using Pair = std::pair<std::unique_ptr<std::string>, std::unique_ptr<Expression>>;
    std::vector<Pair>                       items_;
    Value                                   result_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_CONTAINEREXPRESSION_H_
