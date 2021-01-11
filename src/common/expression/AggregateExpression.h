/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_AGGREGATEEXPRESSION_H_
#define COMMON_EXPRESSION_AGGREGATEEXPRESSION_H_

#include "common/expression/Expression.h"
#include <common/datatypes/Set.h>

namespace nebula {
class AggData final {
public:
    explicit AggData(Set* uniques = nullptr)
        : cnt_(0),
          sum_(0.0),
          avg_(0.0),
          deviation_(0.0),
          result_(Value::kNullValue) {
        if (uniques == nullptr) {
            uniques_ = std::make_unique<Set>();
        } else {
            uniques_.reset(uniques);
        }
    }

    const Value& cnt() const {
        return cnt_;
    }

    Value& cnt() {
        return cnt_;
    }

    void setCnt(Value&& cnt) {
        cnt_ = std::move(cnt);
    }

    const Value& sum() const {
        return sum_;
    }

    Value& sum() {
        return sum_;
    }

    void setSum(Value&& sum) {
        sum_ = std::move(sum);
    }

    const Value& avg() const {
        return avg_;
    }

    Value& avg() {
        return avg_;
    }

    void setAvg(Value&& avg) {
        avg_ = std::move(avg);
    }

    const Value& deviation() const {
        return deviation_;
    }

    Value& deviation() {
        return deviation_;
    }

    void setDeviation(Value&& deviation) {
        deviation_ = std::move(deviation);
    }

    const Value& result() const {
        return result_;
    }

    Value& result() {
        return result_;
    }

    void setResult(Value&& res) {
        result_ = std::move(res);
    }

    void setResult(const Value& res) {
        result_ = res;
    }

    const Set* uniques() const {
        return uniques_.get();
    }

    Set* uniques() {
        return uniques_.get();
    }

    void setUniques(Set* uniques) {
        uniques_.reset(uniques);
    }

private:
    Value cnt_;
    Value sum_;
    Value avg_;
    Value deviation_;
    Value result_;
    std::unique_ptr<Set>   uniques_;
};

class AggregateExpression final : public Expression {
    friend class Expression;

public:
    enum class Function : uint8_t {
        kNone,
        kCount,
        kSum,
        kAvg,
        kMax,
        kMin,
        kStdev,
        kBitAnd,
        kBitOr,
        kBitXor,
        kCollect,
        kCollectSet,
    };
    static std::unordered_map<AggregateExpression::Function,
        std::function<void(AggData*, const Value&)>> AGG_FUNC_MAP;
    static std::unordered_map<std::string, AggregateExpression::Function> NAME_ID_MAP;

    explicit AggregateExpression(std::string* name = nullptr,
                        Expression* arg = nullptr,
                        bool distinct = false)
        : Expression(Kind::kAggregate) {
        arg_.reset(arg);
        name_.reset(name);
        distinct_ = distinct;
    }

    const Value& eval(ExpressionContext& ctx) override;

    bool operator==(const Expression& rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        auto arg = arg_->clone();
        return std::make_unique<AggregateExpression>(new std::string(*name_),
                                                     std::move(arg).release(),
                                                     distinct_);
    }

    const std::string* name() const {
        return name_.get();
    }

    const Expression* arg() const {
        return arg_.get();
    }

    Expression* arg() {
        return arg_.get();
    }

    void setArg(Expression* arg) {
        arg_.reset(arg);
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
    void apply(AggData* aggData, const Value& val) {
        AGG_FUNC_MAP[NAME_ID_MAP[name_->c_str()]](aggData, val);
    }

    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

    std::unique_ptr<std::string>    name_;
    std::unique_ptr<Expression>     arg_;
    bool                            distinct_{false};
    AggData*                        aggData_{nullptr};
};

}  // namespace nebula
#endif  // EXPRESSION_AGGREGATEEXPRESSION_H_
