/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_SYMBOLPROPERTYEXPRESSION_H_
#define COMMON_EXPRESSION_SYMBOLPROPERTYEXPRESSION_H_

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
// The SymbolPropertyExpression will only be used in parser to indicate
// the form of symbol.prop, it will be transform to a proper expression
// in a parse rule.
class SymbolPropertyExpression: public Expression {
    friend class Expression;
public:
    SymbolPropertyExpression(Kind kind,
                             std::string* ref,
                             std::string* sym,
                             std::string* prop)
        : Expression(kind) {
        ref_.reset(ref);
        sym_.reset(sym);
        prop_.reset(prop);
    }

    bool operator==(const Expression& rhs) const override;

    const std::string *ref() const {
        return ref_.get();
    }

    const std::string *sym() const {
        return sym_.get();
    }

    const std::string *prop() const {
        return prop_.get();
    }

protected:
    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

    std::unique_ptr<std::string>    ref_;
    std::unique_ptr<std::string>    sym_;
    std::unique_ptr<std::string>    prop_;
};

// edge_name.any_prop_name
class EdgePropertyExpression final : public SymbolPropertyExpression {
public:
    EdgePropertyExpression(std::string* edge = nullptr,
                           std::string* prop = nullptr)
        : SymbolPropertyExpression(Kind::kEdgeProperty,
                                   new std::string(""),
                                   edge,
                                   prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override {
        // TODO
        return "";
    }
};

// $-.any_prop_name
class InputPropertyExpression final : public SymbolPropertyExpression {
public:
    explicit InputPropertyExpression(std::string* prop = nullptr)
        : SymbolPropertyExpression(Kind::kInputProperty,
                                   new std::string(kInputRef),
                                   new std::string(""),
                                   prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override {
        // TODO
        return "";
    }
};

// $VarName.any_prop_name
class VariablePropertyExpression final : public SymbolPropertyExpression {
public:
    VariablePropertyExpression(std::string* var = nullptr,
                               std::string* prop = nullptr)
        : SymbolPropertyExpression(Kind::kVarProperty,
                                   new std::string(kVarRef),
                                   var,
                                   prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override {
        // TODO
        return "";
    }
};

// $^.TagName.any_prop_name
class SourcePropertyExpression final : public SymbolPropertyExpression {
public:
    SourcePropertyExpression(std::string* tag = nullptr,
                             std::string* prop = nullptr)
        : SymbolPropertyExpression(Kind::kSrcProperty,
                                   new std::string(kSrcRef),
                                   tag,
                                   prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override {
        // TODO
        return "";
    }
};

// $$.TagName.any_prop_name
class DestPropertyExpression final : public SymbolPropertyExpression {
public:
    DestPropertyExpression(std::string* tag = nullptr,
                           std::string* prop = nullptr)
        : SymbolPropertyExpression(Kind::kDstProperty,
                                   new std::string(kDstRef),
                                   tag,
                                   prop) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override {
        // TODO
        return "";
    }
};

// EdgeName._src
class EdgeSrcIdExpression final : public SymbolPropertyExpression {
public:
    explicit EdgeSrcIdExpression(std::string* edge = nullptr)
        : SymbolPropertyExpression(Kind::kEdgeSrc,
                                   new std::string(""),
                                   edge,
                                   new std::string(_SRC)) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override {
        // TODO
        return "";
    }
};

// EdgeName._type
class EdgeTypeExpression final : public SymbolPropertyExpression {
public:
    explicit EdgeTypeExpression(std::string* edge = nullptr)
        : SymbolPropertyExpression(Kind::kEdgeType,
                                   new std::string(""),
                                   edge,
                                   new std::string(_TYPE)) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override {
        // TODO
        return "";
    }
};

// EdgeName._rank
class EdgeRankExpression final : public SymbolPropertyExpression {
public:
    explicit EdgeRankExpression(std::string* edge = nullptr)
        : SymbolPropertyExpression(Kind::kEdgeRank,
                                   new std::string(""),
                                   edge,
                                   new std::string(_RANK)) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override {
        // TODO
        return "";
    }
};

// EdgeName._dst
class EdgeDstIdExpression final : public SymbolPropertyExpression {
public:
    explicit EdgeDstIdExpression(std::string* edge = nullptr)
        : SymbolPropertyExpression(Kind::kEdgeDst,
                                   new std::string(""),
                                   edge,
                                   new std::string(_DST)) {}

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override {
        // TODO
        return "";
    }
};
}   // namespace nebula
#endif
