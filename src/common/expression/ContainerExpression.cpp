/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/ContainerExpression.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Map.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

std::string ListExpression::toString() const {
    // list *expression* is not allowed to be empty
    DCHECK(!items_.empty());
    std::string buf;
    buf.reserve(256);

    buf += '[';
    for (auto &expr : items_) {
        buf += expr->toString();
        buf += ",";
    }
    buf.back() = ']';

    return buf;
}


bool ListExpression::operator==(const Expression &rhs) const {
    if (kind() != rhs.kind()) {
        return false;
    }

    auto &list = static_cast<const ListExpression&>(rhs);
    if (size() != list.size()) {
        return false;
    }

    for (auto i = 0u; i < size(); i++) {
        if (*items_[i] != *list.items_[i]) {
            return false;
        }
    }

    return true;
}


const Value& ListExpression::eval(ExpressionContext &ctx) {
    // TODO(dutor) Reuse `result_' iff all elements are constant
    std::vector<Value> items;
    items.reserve(size());

    for (auto &expr : items_) {
        items.emplace_back(expr->eval(ctx));
    }
    result_.setList(List(std::move(items)));

    return result_;
}


void ListExpression::writeTo(Encoder &encoder) const {
    encoder << kind();
    encoder << size();
    for (auto &expr : items_) {
        encoder << *expr;
    }
}


void ListExpression::resetFrom(Decoder &decoder) {
    auto size = decoder.readSize();
    items_.reserve(size);
    for (auto i = 0u; i < size; i++) {
        items_.emplace_back(decoder.readExpression(pool_));
    }
}

void ListExpression::accept(ExprVisitor *visitor) {
    visitor->visit(this);
}

std::string SetExpression::toString() const {
    // set *expression* is not allowed to be empty
    DCHECK(!items_.empty());
    std::string buf;
    buf.reserve(256);

    buf += '{';
    for (auto &expr : items_) {
        buf += expr->toString();
        buf += ",";
    }
    buf.back() = '}';

    return buf;
}


bool SetExpression::operator==(const Expression &rhs) const {
    if (kind() != rhs.kind()) {
        return false;
    }

    auto &set = static_cast<const SetExpression&>(rhs);
    if (size() != set.size()) {
        return false;
    }

    for (auto i = 0u; i < size(); i++) {
        if (*items_[i] != *set.items_[i]) {
            return false;
        }
    }

    return true;
}


const Value& SetExpression::eval(ExpressionContext &ctx) {
    // TODO(dutor) Reuse `result_' iff all elements are constant
    std::unordered_set<Value> set;
    set.reserve(size());

    for (auto &expr : items_) {
        set.emplace(expr->eval(ctx));
    }
    result_.setSet(Set(std::move(set)));

    return result_;
}


void SetExpression::writeTo(Encoder &encoder) const {
    encoder << kind();
    encoder << size();
    for (auto &expr : items_) {
        encoder << *expr;
    }
}


void SetExpression::resetFrom(Decoder &decoder) {
    auto size = decoder.readSize();
    items_.reserve(size);
    for (auto i = 0u; i < size; i++) {
        items_.emplace_back(decoder.readExpression(pool_));
    }
}

void SetExpression::accept(ExprVisitor *visitor) {
    visitor->visit(this);
}

std::string MapExpression::toString() const {
    // map *expression* is not allowed to be empty
    DCHECK(!items_.empty());
    std::string buf;
    buf.reserve(256);

    buf += '{';
    for (auto &kv : items_) {
        buf += kv.first;
        buf += ":";
        buf += kv.second->toString();
        buf += ",";
    }
    buf.back() = '}';

    return buf;
}


bool MapExpression::operator==(const Expression &rhs) const {
    if (kind() != rhs.kind()) {
        return false;
    }

    auto &map = static_cast<const MapExpression&>(rhs);
    if (size() != map.size()) {
        return false;
    }

    for (auto i = 0u; i < size(); i++) {
        if (items_[i].first != map.items_[i].first) {
            return false;
        }
        if (*items_[i].second != *map.items_[i].second) {
            return false;
        }
    }

    return true;
}


const Value& MapExpression::eval(ExpressionContext &ctx) {
    // TODO(dutor) Reuse `result_' iff all elements are constant
    std::unordered_map<std::string, Value> map;
    map.reserve(size());

    for (auto &kv : items_) {
        map.emplace(kv.first, kv.second->eval(ctx));
    }
    result_.setMap(Map(std::move(map)));

    return result_;
}


void MapExpression::writeTo(Encoder &encoder) const {
    encoder << kind();
    encoder << size();
    for (auto &kv : items_) {
        encoder << kv.first;
        encoder << *kv.second;
    }
}


void MapExpression::resetFrom(Decoder &decoder) {
    auto size = decoder.readSize();
    items_.reserve(size);
    for (auto i = 0u; i < size; i++) {
        auto str = decoder.readStr();
        auto expr = decoder.readExpression(pool_);
        items_.emplace_back(str, expr);
    }
}

void MapExpression::accept(ExprVisitor *visitor) {
    visitor->visit(this);
}

}   // namespace nebula
