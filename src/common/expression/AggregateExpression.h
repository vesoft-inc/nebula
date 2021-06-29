/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_AGGREGATEEXPRESSION_H_
#define COMMON_EXPRESSION_AGGREGATEEXPRESSION_H_

#include <common/datatypes/Set.h>
#include "common/expression/Expression.h"
#include "common/function/AggFunctionManager.h"

namespace nebula {

class AggregateExpression final : public Expression {
    friend class Expression;

public:
    AggregateExpression& operator=(const AggregateExpression& rhs) = delete;
    AggregateExpression& operator=(AggregateExpression&&) = delete;

    static AggregateExpression* make(ObjectPool* pool,
                                     const std::string& name = "",
                                     Expression* arg = nullptr,
                                     bool distinct = false) {
        DCHECK(!!pool);
        return pool->add(new AggregateExpression(pool, name, arg, distinct));
    }

    const Value& eval(ExpressionContext& ctx) override;

    void apply(AggData* aggData, const Value& val);

    bool operator==(const Expression& rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        auto argCopy = arg()->clone();
        return AggregateExpression::make(pool_, name_, argCopy, distinct_);
    }

    const std::string& name() const {
        return name_;
    }

    const Expression* arg() const {
        return arg_;
    }

    Expression* arg() {
        return arg_;
    }

    void setArg(Expression* arg) {
        arg_ = arg;
    }

    bool distinct() {
        return distinct_;
    }

    void setDistinct(bool dist) {
        distinct_ = dist;
    }

    const AggData* aggData() const {
        return aggData_;
    }

    AggData* aggData() {
        return aggData_;
    }

    void setAggData(AggData* agg_data) {
        aggData_ = agg_data;
    }

private:
    explicit AggregateExpression(ObjectPool* pool,
                                 const std::string& name = "",
                                 Expression* arg = nullptr,
                                 bool distinct = false)
        : Expression(pool, Kind::kAggregate), name_(name), distinct_(distinct) {
        arg_ = arg;
        auto aggFuncResult = AggFunctionManager::get(name_);
        if (aggFuncResult.ok()) {
            aggFunc_ = std::move(aggFuncResult).value();
        }
    }

    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

private:
    std::string name_;
    Expression* arg_;
    bool distinct_{false};
    AggData* aggData_{nullptr};

    // runtime cache for aggregate function lambda
    AggFunctionManager::AggFunction aggFunc_;
};

}   // namespace nebula
#endif   // EXPRESSION_AGGREGATEEXPRESSION_H_
