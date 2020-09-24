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
    explicit ExpressionList(size_t sz) {
        items_.reserve(sz);
    }

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
    explicit MapItemList(size_t sz) {
        items_.reserve(sz);
    }

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

    std::vector<const Expression*> items() const {
        std::vector<const Expression*> items;
        items.reserve(items_.size());
        for (auto &item : items_) {
            items.emplace_back(item.get());
        }
        return items;
    }

    void setItem(size_t index, Expression *item) {
        DCHECK_LT(index, items_.size());
        items_[index].reset(item);
    }

    std::vector<std::unique_ptr<Expression>> get() && {
        return std::move(items_);
    }

    void setItems(std::vector<std::unique_ptr<Expression>> items) {
        items_ = std::move(items);
    }

    size_t size() const {
        return items_.size();
    }

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor *visitor) override;

    std::unique_ptr<Expression> clone() const override {
        auto items = new ExpressionList(items_.size());
        for (auto &item : items_) {
            items->add(item->clone().release());
        }
        return std::make_unique<ListExpression>(items);
    }

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

    std::vector<const Expression*> items() const {
        std::vector<const Expression*> items;
        items.reserve(items_.size());
        for (auto &item : items_) {
            items.emplace_back(item.get());
        }
        return items;
    }

    void setItem(size_t index, Expression *item) {
        DCHECK_LT(index, items_.size());
        items_[index].reset(item);
    }

    std::vector<std::unique_ptr<Expression>> get() && {
        return std::move(items_);
    }

    void setItems(std::vector<std::unique_ptr<Expression>> items) {
        items_ = std::move(items);
    }

    size_t size() const {
        return items_.size();
    }

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        auto items = new ExpressionList(items_.size());
        for (auto &item : items_) {
            items->add(item->clone().release());
        }
        return std::make_unique<SetExpression>(items);
    }

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

    std::vector<std::pair<const std::string*, const Expression*>> items() const {
        std::vector<std::pair<const std::string*, const Expression*>> items;
        items.reserve(items_.size());
        for (auto &item : items_) {
            items.emplace_back(item.first.get(), item.second.get());
        }
        return items;
    }

    using Item = std::pair<std::unique_ptr<std::string>, std::unique_ptr<Expression>>;
    void setItems(std::vector<Item> items) {
        items_ = std::move(items);
    }

    void setItem(size_t index, Item item) {
        DCHECK_LT(index, items_.size());
        items_[index] = std::move(item);
    }

    std::vector<Item> get() && {
        return std::move(items_);
    }

    size_t size() const {
        return items_.size();
    }

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        auto items = new MapItemList(items_.size());
        for (auto &item : items_) {
            items->add(new std::string(*item.first), item.second->clone().release());
        }
        return std::make_unique<MapExpression>(items);
    }

private:
    void writeTo(Encoder &encoder) const override;

    void resetFrom(Decoder &decoder) override;

private:
    std::vector<Item>                       items_;
    Value                                   result_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_CONTAINEREXPRESSION_H_
