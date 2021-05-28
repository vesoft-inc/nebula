/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_PROPERTYEXPRESSION_H_
#define COMMON_EXPRESSION_PROPERTYEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

constexpr char const kInputRef[] = "$-";
constexpr char const kVarRef[] = "$";
constexpr char const kSrcRef[] = "$^";
constexpr char const kDstRef[] = "$$";

// Base abstract expression of getting properties.
// An expresion of getting props is consisted with 3 parts:
// 1. reference, e.g. $-, $, $^, $$
// 2. symbol, a symbol name, e.g. tag_name, edge_name, variable_name,
// 3. property, property name.
//
// The PropertyExpression will only be used in parser to indicate
// the form of symbol.prop, it will be transform to a proper expression
// in a parse rule.
class PropertyExpression: public Expression {
    friend class Expression;
public:
    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    const std::string &ref() const {
        return ref_;
    }

    const std::string &sym() const {
        return sym_;
    }

    const std::string &prop() const {
        return prop_;
    }

    std::string toString() const override;

protected:
    PropertyExpression(Kind kind,
                       const std::string& ref,
                       const std::string& sym,
                       const std::string& prop)
        : Expression(kind), ref_(ref), sym_(sym), prop_(prop) {}

    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

    std::string ref_;
    std::string sym_;
    std::string prop_;
};

// edge_name.any_prop_name
class EdgePropertyExpression final : public PropertyExpression {
public:
    explicit EdgePropertyExpression(const std::string& edge = "", const std::string& prop = "")
        : PropertyExpression(Kind::kEdgeProperty, "", edge, prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgePropertyExpression>(sym(), prop());
    }

private:
    Value result_;
};

// tag_name.any_prop_name
class TagPropertyExpression final : public PropertyExpression {
public:
    explicit TagPropertyExpression(const std::string& tag = "", const std::string& prop = "")
        : PropertyExpression(Kind::kTagProperty, "", tag, prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<TagPropertyExpression>(sym(), prop());
    }

private:
    Value result_;
};

// $-.any_prop_name
class InputPropertyExpression final : public PropertyExpression {
public:
    explicit InputPropertyExpression(const std::string& prop = "")
        : PropertyExpression(Kind::kInputProperty, kInputRef, "", prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<InputPropertyExpression>(prop());
    }
};

// $VarName.any_prop_name
class VariablePropertyExpression final : public PropertyExpression {
public:
    explicit VariablePropertyExpression(const std::string& var = "", const std::string& prop = "")
        : PropertyExpression(Kind::kVarProperty, kVarRef, var, prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<VariablePropertyExpression>(sym(), prop());
    }
};

// $^.TagName.any_prop_name
class SourcePropertyExpression final : public PropertyExpression {
public:
    explicit SourcePropertyExpression(const std::string& tag = "", const std::string& prop = "")
        : PropertyExpression(Kind::kSrcProperty, kSrcRef, tag, prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<SourcePropertyExpression>(sym(), prop());
    }

private:
    Value result_;
};

// $$.TagName.any_prop_name
class DestPropertyExpression final : public PropertyExpression {
public:
    explicit DestPropertyExpression(const std::string& tag = "", const std::string& prop = "")
        : PropertyExpression(Kind::kDstProperty, kDstRef, tag, prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<DestPropertyExpression>(sym(), prop());
    }
};

// EdgeName._src
class EdgeSrcIdExpression final : public PropertyExpression {
public:
    explicit EdgeSrcIdExpression(const std::string& edge = "")
        : PropertyExpression(Kind::kEdgeSrc, "", edge, kSrc) {}

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgeSrcIdExpression>(sym());
    }

private:
    Value result_;
};

// EdgeName._type
class EdgeTypeExpression final : public PropertyExpression {
public:
    explicit EdgeTypeExpression(const std::string& edge = "")
        : PropertyExpression(Kind::kEdgeType, "", edge, kType) {}

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgeTypeExpression>(sym());
    }

private:
    Value result_;
};

// EdgeName._rank
class EdgeRankExpression final : public PropertyExpression {
public:
    explicit EdgeRankExpression(const std::string& edge = "")
        : PropertyExpression(Kind::kEdgeRank, "", edge, kRank) {}

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgeRankExpression>(sym());
    }

private:
    Value result_;
};

// EdgeName._dst
class EdgeDstIdExpression final : public PropertyExpression {
public:
    explicit EdgeDstIdExpression(const std::string& edge = "")
        : PropertyExpression(Kind::kEdgeDst, "", edge, kDst) {}

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgeDstIdExpression>(sym());
    }

private:
    Value result_;
};

}   // namespace nebula
#endif
