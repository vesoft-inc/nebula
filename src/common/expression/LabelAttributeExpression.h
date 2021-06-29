/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_LABELATTRIBUTEEXPRESSION_H_
#define COMMON_EXPRESSION_LABELATTRIBUTEEXPRESSION_H_

#include <memory>

#include "common/expression/ConstantExpression.h"
#include "common/expression/LabelExpression.h"

namespace nebula {

// label.label
class LabelAttributeExpression final : public Expression {
public:
    LabelAttributeExpression& operator=(const LabelAttributeExpression& rhs) = delete;
    LabelAttributeExpression& operator=(LabelAttributeExpression&&) = delete;

    static LabelAttributeExpression* make(ObjectPool* pool,
                                          LabelExpression* lhs = nullptr,
                                          ConstantExpression* rhs = nullptr) {
        DCHECK(!!pool);
        return pool->add(new LabelAttributeExpression(pool, lhs, rhs));
    }

    bool operator==(const Expression &rhs) const override {
        if (rhs.kind() != kind()) {
            return false;
        }
        auto &expr = static_cast<const LabelAttributeExpression&>(rhs);
        return *lhs_ == *expr.lhs_ && *rhs_ == *expr.rhs_;
    }

    const Value& eval(ExpressionContext&) override {
        LOG(FATAL) << "LabelAttributeExpression has to be rewritten";
        return Value::kNullBadData;
    }

    void accept(ExprVisitor *visitor) override;

    Expression* clone() const override {
        return LabelAttributeExpression::make(pool_,
                                              static_cast<LabelExpression*>(left()->clone()),
                                              static_cast<ConstantExpression*>(right()->clone()));
    }

    const LabelExpression* left() const {
        return lhs_;
    }

    LabelExpression* left() {
        return lhs_;
    }

    const ConstantExpression* right() const {
        return rhs_;
    }

    ConstantExpression* right() {
        return rhs_;
    }

    std::string toString() const override;

private:
    explicit LabelAttributeExpression(ObjectPool* pool,
                                      LabelExpression* lhs = nullptr,
                                      ConstantExpression* rhs = nullptr)
        : Expression(pool, Kind::kLabelAttribute) {
        DCHECK(rhs == nullptr || rhs->value().isStr());
        lhs_ = lhs;
        rhs_ = rhs;
    }

    void writeTo(Encoder&) const override {
        LOG(FATAL) << "LabelAttributeExpression not supporte to encode.";
    }

    void resetFrom(Decoder&) override {
        LOG(FATAL) << "LabelAttributeExpression not supporte to decode.";
    }

private:
    LabelExpression*    lhs_;
    ConstantExpression* rhs_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_LABELATTRIBUTEEXPRESSION_H_
