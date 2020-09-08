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

    const std::string *ref() const {
        return ref_.get();
    }

    const std::string *sym() const {
        return sym_.get();
    }

    const std::string *prop() const {
        return prop_.get();
    }

    std::string toString() const override;

protected:
    PropertyExpression(Kind kind,
                       std::string* ref,
                       std::string* sym,
                       std::string* prop)
        : Expression(kind) {
        ref_.reset(ref);
        sym_.reset(sym);
        prop_.reset(prop);
    }

    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

    std::unique_ptr<std::string>    ref_;
    std::unique_ptr<std::string>    sym_;
    std::unique_ptr<std::string>    prop_;
};

// edge_name.any_prop_name
class EdgePropertyExpression final : public PropertyExpression {
public:
    EdgePropertyExpression(std::string* edge = nullptr,
                           std::string* prop = nullptr)
        : PropertyExpression(Kind::kEdgeProperty,
                             new std::string(""),
                             edge,
                             prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgePropertyExpression>(new std::string(*sym()),
                                                        new std::string(*prop()));
    }

private:
    Value                           result_;
};


// tag_name.any_prop_name
class TagPropertyExpression final : public PropertyExpression {
public:
    TagPropertyExpression(std::string* tag = nullptr,
                          std::string* prop = nullptr)
        : PropertyExpression(Kind::kTagProperty,
                             new std::string(""),
                             tag,
                             prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<TagPropertyExpression>(new std::string(*sym()),
                                                       new std::string(*prop()));
    }

private:
    Value result_;
};

// $-.any_prop_name
class InputPropertyExpression final : public PropertyExpression {
public:
    explicit InputPropertyExpression(std::string* prop = nullptr)
        : PropertyExpression(Kind::kInputProperty,
                             new std::string(kInputRef),
                             new std::string(""),
                             prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<InputPropertyExpression>(new std::string(*prop()));
    }
};

// $VarName.any_prop_name
class VariablePropertyExpression final : public PropertyExpression {
public:
    VariablePropertyExpression(std::string* var = nullptr,
                               std::string* prop = nullptr)
        : PropertyExpression(Kind::kVarProperty,
                             new std::string(kVarRef),
                             var,
                             prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<VariablePropertyExpression>(new std::string(*sym()),
                                                            new std::string(*prop()));
    }
};

// $^.TagName.any_prop_name
class SourcePropertyExpression final : public PropertyExpression {
public:
    SourcePropertyExpression(std::string* tag = nullptr,
                             std::string* prop = nullptr)
        : PropertyExpression(Kind::kSrcProperty,
                             new std::string(kSrcRef),
                             tag,
                             prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<SourcePropertyExpression>(new std::string(*sym()),
                                                          new std::string(*prop()));
    }

private:
    Value                           result_;
};

// $$.TagName.any_prop_name
class DestPropertyExpression final : public PropertyExpression {
public:
    DestPropertyExpression(std::string* tag = nullptr,
                           std::string* prop = nullptr)
        : PropertyExpression(Kind::kDstProperty,
                             new std::string(kDstRef),
                             tag,
                             prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<DestPropertyExpression>(new std::string(*sym()),
                                                        new std::string(*prop()));
    }
};

// EdgeName._src
class EdgeSrcIdExpression final : public PropertyExpression {
public:
    explicit EdgeSrcIdExpression(std::string* edge = nullptr)
        : PropertyExpression(Kind::kEdgeSrc,
                             new std::string(""),
                             edge,
                             new std::string(kSrc)) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgeSrcIdExpression>(new std::string(*sym()));
    }

private:
    Value                           result_;
};

// EdgeName._type
class EdgeTypeExpression final : public PropertyExpression {
public:
    explicit EdgeTypeExpression(std::string* edge = nullptr)
        : PropertyExpression(Kind::kEdgeType,
                             new std::string(""),
                             edge,
                             new std::string(kType)) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgeTypeExpression>(new std::string(*sym()));
    }

private:
    Value                           result_;
};

// EdgeName._rank
class EdgeRankExpression final : public PropertyExpression {
public:
    explicit EdgeRankExpression(std::string* edge = nullptr)
        : PropertyExpression(Kind::kEdgeRank,
                             new std::string(""),
                             edge,
                             new std::string(kRank)) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgeRankExpression>(new std::string(*sym()));
    }

private:
    Value                           result_;
};

// EdgeName._dst
class EdgeDstIdExpression final : public PropertyExpression {
public:
    explicit EdgeDstIdExpression(std::string* edge = nullptr)
        : PropertyExpression(Kind::kEdgeDst,
                             new std::string(""),
                             edge,
                             new std::string(kDst)) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<EdgeDstIdExpression>(new std::string(*sym()));
    }

private:
    Value                           result_;
};
}   // namespace nebula
#endif
