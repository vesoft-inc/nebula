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
    explicit ListComprehensionExpression(std::string* innerVar = nullptr,
                                         Expression* collection = nullptr,
                                         Expression* filter = nullptr,
                                         Expression* mapping = nullptr)
        : Expression(Kind::kListComprehension),
          innerVar_(innerVar),
          collection_(collection),
          filter_(filter),
          mapping_(mapping) {}

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        auto expr = std::make_unique<ListComprehensionExpression>(
            new std::string(*innerVar_),
            collection_->clone().release(),
            filter_ != nullptr ? filter_->clone().release() : nullptr,
            mapping_ != nullptr ? mapping_->clone().release() : nullptr);
        if (originString_ != nullptr) {
            expr->setOriginString(new std::string(*originString_));
        }
        return expr;
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

    const Expression* filter() const {
        return filter_.get();
    }

    Expression* filter() {
        return filter_.get();
    }

    const Expression* mapping() const {
        return mapping_.get();
    }

    Expression* mapping() {
        return mapping_.get();
    }

    void setInnerVar(std::string* name) {
        innerVar_.reset(name);
    }

    void setCollection(Expression* expr) {
        collection_.reset(expr);
    }

    void setFilter(Expression* expr) {
        filter_.reset(expr);
    }

    void setMapping(Expression* expr) {
        mapping_.reset(expr);
    }

    bool hasFilter() const {
        return filter_ != nullptr;
    }

    bool hasMapping() const {
        return mapping_ != nullptr;
    }

    void setOriginString(std::string* s) {
        originString_.reset(s);
    }

    std::string makeString() const;

    bool hasOriginString() const {
        return originString_ != nullptr;
    }

private:
    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

    std::unique_ptr<std::string> innerVar_;
    std::unique_ptr<Expression> collection_;
    std::unique_ptr<Expression> filter_;    // filter_ is optional
    std::unique_ptr<Expression> mapping_;   // mapping_ is optional
    std::unique_ptr<std::string> originString_;
    Value result_;
};

}   // namespace nebula
#endif   // EXPRESSION_LISTCOMPREHENSIONEXPRESSION_H_
