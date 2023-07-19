/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_CONTAINEREXPRESSION_H_
#define COMMON_EXPRESSION_CONTAINEREXPRESSION_H_

#include "common/base/ObjectPool.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/Expression.h"

namespace nebula {

class ExpressionList final {
 public:
  static ExpressionList *make(ObjectPool *pool, size_t sz = 0) {
    return pool->makeAndAdd<ExpressionList>(sz);
  }

  ExpressionList &add(Expression *expr) {
    items_.emplace_back(expr);
    return *this;
  }

  auto get() {
    return items_;
  }

 private:
  friend ObjectPool;
  ExpressionList() = default;
  explicit ExpressionList(size_t sz) {
    items_.reserve(sz);
  }

 private:
  std::vector<Expression *> items_;
};

class MapItemList final {
 public:
  static MapItemList *make(ObjectPool *pool, size_t sz = 0) {
    return pool->makeAndAdd<MapItemList>(sz);
  }

  MapItemList &add(const std::string &key, Expression *value) {
    items_.emplace_back(key, value);
    return *this;
  }

  auto get() {
    return items_;
  }

 private:
  friend ObjectPool;
  MapItemList() = default;
  explicit MapItemList(size_t sz) {
    items_.reserve(sz);
  }

 private:
  using Pair = std::pair<std::string, Expression *>;
  std::vector<Pair> items_;
};

class ContainerExpression : public Expression {
 public:
  virtual const std::vector<Expression *> getKeys() const = 0;
  virtual size_t size() const = 0;

 protected:
  ContainerExpression(ObjectPool *pool, Expression::Kind kind) : Expression(pool, kind) {}
};

class ListExpression final : public ContainerExpression {
 public:
  ListExpression &operator=(const ListExpression &rhs) = delete;
  ListExpression &operator=(ListExpression &&) = delete;

  static ListExpression *make(ObjectPool *pool, ExpressionList *items = nullptr) {
    return items == nullptr ? pool->makeAndAdd<ListExpression>(pool)
                            : pool->makeAndAdd<ListExpression>(pool, items);
  }

  const Value &eval(ExpressionContext &ctx) override;

  const std::vector<Expression *> &items() const {
    return items_;
  }

  void setItem(size_t index, Expression *item) {
    DCHECK_LT(index, items_.size());
    items_[index] = item;
  }

  const std::vector<Expression *> getKeys() const override {
    return items_;
  }

  void setItems(std::vector<Expression *> items) {
    items_ = items;
  }

  size_t size() const override {
    return items_.size();
  }

  bool operator==(const Expression &rhs) const override;

  std::string toString() const override;

  void accept(ExprVisitor *visitor) override;

  Expression *clone() const override {
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
  friend ObjectPool;
  explicit ListExpression(ObjectPool *pool) : ContainerExpression(pool, Kind::kList) {}

  ListExpression(ObjectPool *pool, ExpressionList *items) : ContainerExpression(pool, Kind::kList) {
    items_ = items->get();
  }

  void writeTo(Encoder &encoder) const override;

  void resetFrom(Decoder &decoder) override;

 private:
  std::vector<Expression *> items_;
  Value result_;
};

class SetExpression final : public ContainerExpression {
 public:
  SetExpression &operator=(const SetExpression &rhs) = delete;
  SetExpression &operator=(SetExpression &&) = delete;

  static SetExpression *make(ObjectPool *pool, ExpressionList *items = nullptr) {
    return items == nullptr ? pool->makeAndAdd<SetExpression>(pool)
                            : pool->makeAndAdd<SetExpression>(pool, items);
  }

  const Value &eval(ExpressionContext &ctx) override;

  const std::vector<Expression *> &items() const {
    return items_;
  }

  void setItem(size_t index, Expression *item) {
    DCHECK_LT(index, items_.size());
    items_[index] = item;
  }

  const std::vector<Expression *> getKeys() const override {
    return items_;
  }

  void setItems(std::vector<Expression *> items) {
    items_ = items;
  }

  size_t size() const override {
    return items_.size();
  }

  bool operator==(const Expression &rhs) const override;

  std::string toString() const override;

  void accept(ExprVisitor *visitor) override;

  Expression *clone() const override {
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
  friend ObjectPool;
  explicit SetExpression(ObjectPool *pool) : ContainerExpression(pool, Kind::kSet) {}

  SetExpression(ObjectPool *pool, ExpressionList *items) : ContainerExpression(pool, Kind::kSet) {
    items_ = items->get();
  }

  void writeTo(Encoder &encoder) const override;

  void resetFrom(Decoder &decoder) override;

 private:
  std::vector<Expression *> items_;
  Value result_;
};

class MapExpression final : public ContainerExpression {
 public:
  MapExpression &operator=(const MapExpression &rhs) = delete;
  MapExpression &operator=(MapExpression &&) = delete;

  static MapExpression *make(ObjectPool *pool, MapItemList *items = nullptr) {
    return items == nullptr ? pool->makeAndAdd<MapExpression>(pool)
                            : pool->makeAndAdd<MapExpression>(pool, items);
  }

  using Item = std::pair<std::string, Expression *>;

  const Value &eval(ExpressionContext &ctx) override;

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

  const std::vector<Expression *> getKeys() const override {
    std::vector<Expression *> keys;
    for (const auto &item : items_) {
      keys.emplace_back(ConstantExpression::make(pool_, item.first));
    }
    return keys;
  }

  size_t size() const override {
    return items_.size();
  }

  bool operator==(const Expression &rhs) const override;

  std::string toString() const override;

  void accept(ExprVisitor *visitor) override;

  Expression *clone() const override {
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
  friend ObjectPool;
  explicit MapExpression(ObjectPool *pool) : ContainerExpression(pool, Kind::kMap) {}

  MapExpression(ObjectPool *pool, MapItemList *items) : ContainerExpression(pool, Kind::kMap) {
    items_ = items->get();
  }

  void writeTo(Encoder &encoder) const override;

  void resetFrom(Decoder &decoder) override;

 private:
  std::vector<Item> items_;
  Value result_;
};

}  // namespace nebula

#endif  // COMMON_EXPRESSION_CONTAINEREXPRESSION_H_
