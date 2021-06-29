/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_REDUCEEXPRESSIONEXPRESSION_H_
#define COMMON_EXPRESSION_REDUCEEXPRESSIONEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

class ReduceExpression final : public Expression {
    friend class Expression;

public:
    static ReduceExpression* make(ObjectPool* pool,
                                  const std::string& accumulator = "",
                                  Expression* initial = nullptr,
                                  const std::string& innerVar = "",
                                  Expression* collection = nullptr,
                                  Expression* mapping = nullptr) {
        DCHECK(!!pool);
        return pool->add(
            new ReduceExpression(pool, accumulator, initial, innerVar, collection, mapping));
    }

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    std::string rawString() const override {
        return hasOriginString() ? originString_ : toString();
    }

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override;

    const std::string& accumulator() const {
        return accumulator_;
    }

    const Expression* initial() const {
        return initial_;
    }

    Expression* initial() {
        return initial_;
    }

    const std::string& innerVar() const {
        return innerVar_;
    }

    const Expression* collection() const {
        return collection_;
    }

    Expression* collection() {
        return collection_;
    }

    const Expression* mapping() const {
        return mapping_;
    }

    Expression* mapping() {
        return mapping_;
    }

    void setAccumulator(const std::string& name) {
        accumulator_ = name;
    }

    void setInitial(Expression* expr) {
        initial_ = expr;
    }

    void setInnerVar(const std::string& name) {
        innerVar_ = name;
    }

    void setCollection(Expression* expr) {
        collection_ = expr;
    }

    void setMapping(Expression* expr) {
        mapping_ = expr;
    }

    void setOriginString(const std::string& s) {
        originString_ = s;
    }

    bool hasOriginString() const {
        return !originString_.empty();
    }

private:
    explicit ReduceExpression(ObjectPool* pool,
                              const std::string& accumulator = "",
                              Expression* initial = nullptr,
                              const std::string& innerVar = "",
                              Expression* collection = nullptr,
                              Expression* mapping = nullptr)
        : Expression(pool, Kind::kReduce),
          accumulator_(accumulator),
          initial_(initial),
          innerVar_(innerVar),
          collection_(collection),
          mapping_(mapping) {}

    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

private:
    std::string accumulator_;
    Expression* initial_;
    std::string innerVar_;
    Expression* collection_;
    Expression* mapping_;
    std::string originString_;
    Value result_;
};

}   // namespace nebula
#endif   // EXPRESSION_REDUCEEXPRESSIONEXPRESSION_H_
