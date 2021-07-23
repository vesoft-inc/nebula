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
    static ExpressionList* make(ObjectPool *pool, size_t sz = 0) {
        DCHECK(!!pool);
        return pool->add(new ExpressionList(sz));
    }

    ExpressionList& add(Expression *expr) {
        items_.emplace_back(expr);
        return *this;
    }

    auto get() {
        return items_;
    }

private:
    ExpressionList() = default;
    explicit ExpressionList(size_t sz) {
        items_.reserve(sz);
    }

private:
    std::vector<Expression*>    items_;
};


class MapItemList final {
public:
    static MapItemList* make(ObjectPool *pool, size_t sz = 0) {
        DCHECK(!!pool);
        return pool->add(new MapItemList(sz));
    }

    MapItemList &add(const std::string &key, Expression *value) {
        items_.emplace_back(key, value);
        return *this;
    }

    auto get() {
        return items_;
    }

private:
    MapItemList() = default;
    explicit MapItemList(size_t sz) {
        items_.reserve(sz);
    }

private:
    using Pair = std::pair<std::string, Expression*>;
    std::vector<Pair>                         items_;
};


class ListExpression final : public Expression {
public:
    ListExpression& operator=(const ListExpression& rhs) = delete;
    ListExpression& operator=(ListExpression&&) = delete;


    static ListExpression *make(ObjectPool *pool, ExpressionList *items = nullptr) {
        DCHECK(!!pool);
        return items == nullptr ? pool->add(new ListExpression(pool))
                                : pool->add(new ListExpression(pool, items));
    }

    const Value& eval(ExpressionContext &ctx) override;

    const std::vector<Expression*> &items() const {
        return items_;
    }

    void setItem(size_t index, Expression *item) {
        DCHECK_LT(index, items_.size());
        items_[index] = item;
    }

    std::vector<Expression*> get() {
        return items_;
    }

    void setItems(std::vector<Expression*> items) {
        items_ = items;
    }

    size_t size() const {
        return items_.size();
    }

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor *visitor) override;

    Expression* clone() const override {
        auto items = ExpressionList::make(pool_, items_.size());
        for (auto &item : items_) {
            items->add(item->clone());
        }
        return ListExpression::make(pool_, items);
    }

    bool isContainerExpr() const override {
        return true;
    }

private:
    explicit ListExpression(ObjectPool *pool) : Expression(pool, Kind::kList) {}

    explicit ListExpression(ObjectPool *pool, ExpressionList *items)
        : Expression(pool, Kind::kList) {
        items_ = items->get();
    }

    void writeTo(Encoder &encoder) const override;

    void resetFrom(Decoder &decoder) override;

private:
    std::vector<Expression*>    items_;
    Value                       result_;
};


class SetExpression final : public Expression {
public:
    SetExpression& operator=(const SetExpression& rhs) = delete;
    SetExpression& operator=(SetExpression&&) = delete;

    static SetExpression *make(ObjectPool *pool, ExpressionList *items = nullptr) {
        DCHECK(!!pool);
        return items == nullptr ? pool->add(new SetExpression(pool))
                                : pool->add(new SetExpression(pool, items));
    }

    const Value& eval(ExpressionContext &ctx) override;

    const std::vector<Expression*> &items() const {
        return items_;
    }

    void setItem(size_t index, Expression* item) {
        DCHECK_LT(index, items_.size());
        items_[index] = item;
    }

    std::vector<Expression*> get() {
        return items_;
    }

    void setItems(std::vector<Expression*> items) {
        items_ = items;
    }

    size_t size() const {
        return items_.size();
    }

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        auto items = ExpressionList::make(pool_, items_.size());
        for (auto &item : items_) {
            items->add(item->clone());
        }
        return SetExpression::make(pool_, items);
    }

    bool isContainerExpr() const override {
        return true;
    }

private:
    explicit SetExpression(ObjectPool *pool) : Expression(pool, Kind::kSet) {}

    explicit SetExpression(ObjectPool *pool, ExpressionList *items) : Expression(pool, Kind::kSet) {
        items_ = items->get();
    }

    void writeTo(Encoder &encoder) const override;

    void resetFrom(Decoder &decoder) override;

private:
    std::vector<Expression*>    items_;
    Value                       result_;
};

class MapExpression final : public Expression {
public:
    MapExpression& operator=(const MapExpression& rhs) = delete;
    MapExpression& operator=(MapExpression&&) = delete;

    static MapExpression *make(ObjectPool *pool, MapItemList *items = nullptr) {
        DCHECK(!!pool);
        return items == nullptr ? pool->add(new MapExpression(pool))
                                : pool->add(new MapExpression(pool, items));
    }

    using Item = std::pair<std::string, Expression *>;

    const Value& eval(ExpressionContext &ctx) override;

    const std::vector<Item> &items() const {
        return items_;
    }

    void setItems(std::vector<Item> items) {
        items_ = items;
    }

    void setItem(size_t index, Item item) {
        DCHECK_LT(index, items_.size());
        items_[index] = item;
    }

    std::vector<Item> get() {
        return items_;
    }

    size_t size() const {
        return items_.size();
    }

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        auto items = MapItemList::make(pool_, items_.size());
        for (auto &item : items_) {
            items->add(item.first, item.second->clone());
        }
        return MapExpression::make(pool_, items);
    }

    bool isContainerExpr() const override {
        return true;
    }

private:
    explicit MapExpression(ObjectPool *pool) : Expression(pool, Kind::kMap) {}

    explicit MapExpression(ObjectPool *pool, MapItemList *items) : Expression(pool, Kind::kMap) {
        items_ = items->get();
    }

    void writeTo(Encoder &encoder) const override;

    void resetFrom(Decoder &decoder) override;

private:
    std::vector<Item>                       items_;
    Value                                   result_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_CONTAINEREXPRESSION_H_
