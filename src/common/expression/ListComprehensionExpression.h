/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_LISTCOMPREHENSIONEXPRESSION_H_
#define COMMON_EXPRESSION_LISTCOMPREHENSIONEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

class ListComprehensionExpression final : public Expression {
    friend class Expression;

public:
    ListComprehensionExpression& operator=(const ListComprehensionExpression& rhs) = delete;
    ListComprehensionExpression& operator=(ListComprehensionExpression&&) = delete;

    static ListComprehensionExpression* make(ObjectPool* pool,
                                             const std::string& innerVar = "",
                                             Expression* collection = nullptr,
                                             Expression* filter = nullptr,
                                             Expression* mapping = nullptr) {
        return pool->add(
            new ListComprehensionExpression(pool, innerVar, collection, filter, mapping));
    }

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    std::string rawString() const override {
        return hasOriginString() ? originString_ : toString();
    }

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override;

    const std::string& innerVar() const {
        return innerVar_;
    }

    const Expression* collection() const {
        return collection_;
    }

    Expression* collection() {
        return collection_;
    }

    const Expression* filter() const {
        return filter_;
    }

    Expression* filter() {
        return filter_;
    }

    const Expression* mapping() const {
        return mapping_;
    }

    Expression* mapping() {
        return mapping_;
    }

    void setInnerVar(const std::string& name) {
        innerVar_ = name;
    }

    void setCollection(Expression* expr) {
        collection_ = expr;
    }

    void setFilter(Expression* expr) {
        filter_ = expr;
    }

    void setMapping(Expression* expr) {
        mapping_ = expr;
    }

    bool hasFilter() const {
        return filter_ != nullptr;
    }

    bool hasMapping() const {
        return mapping_ != nullptr;
    }

    void setOriginString(const std::string& s) {
        originString_ = s;
    }

    bool hasOriginString() const {
        return !originString_.empty();
    }

private:
    explicit ListComprehensionExpression(ObjectPool* pool,
                                         const std::string& innerVar = "",
                                         Expression* collection = nullptr,
                                         Expression* filter = nullptr,
                                         Expression* mapping = nullptr)
        : Expression(pool, Kind::kListComprehension),
          innerVar_(innerVar),
          collection_(collection),
          filter_(filter),
          mapping_(mapping) {}

    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

private:
    std::string innerVar_;
    Expression* collection_{nullptr};
    Expression* filter_{nullptr};    // filter_ is optional
    Expression* mapping_{nullptr};   // mapping_ is optional
    std::string originString_;
    Value result_;
};

}   // namespace nebula
#endif   // EXPRESSION_LISTCOMPREHENSIONEXPRESSION_H_
