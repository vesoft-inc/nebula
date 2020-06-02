/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_ALIASPROPERTYEXPRESSION_H_
#define COMMON_EXPRESSION_ALIASPROPERTYEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

constexpr char const kInputRef[] = "$-";
constexpr char const kVarRef[] = "$";
constexpr char const kSrcRef[] = "$^";
constexpr char const kDstRef[] = "$$";

// Alias.any_prop_name, i.e. EdgeName.any_prop_name
class AliasPropertyExpression : public Expression {
    friend class Expression;

public:
    AliasPropertyExpression(Kind kind,
                            std::string* ref,
                            std::string* alias,
                            std::string* prop)
        : Expression(kind)
        , ref_(ref)
        , alias_(alias)
        , prop_(prop) {}

    bool operator==(const Expression& rhs) const override;

    const std::string* alias() const {
        return alias_.get();
    }

    const std::string* prop() const {
        return prop_.get();
    }

protected:
    std::unique_ptr<std::string> ref_;
    std::unique_ptr<std::string> alias_;
    std::unique_ptr<std::string> prop_;

    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;
};


// $-.any_prop_name
class InputPropertyExpression final : public AliasPropertyExpression {
public:
    explicit InputPropertyExpression(std::string* prop = nullptr)
        : AliasPropertyExpression(Kind::kInputProperty,
                                  new std::string(kInputRef),
                                  new std::string(""),
                                  prop) {}

    Value eval(const ExpressionContext& ctx) const override;

    std::string toString() const override {
        // TODO
        return "";
    }
};


// $VarName.any_prop_name
class VariablePropertyExpression final : public AliasPropertyExpression {
public:
    VariablePropertyExpression(std::string* var = nullptr,
                               std::string* prop = nullptr)
        : AliasPropertyExpression(Kind::kVarProperty,
                                  new std::string(kVarRef),
                                  var,
                                  prop) {}

    Value eval(const ExpressionContext& ctx) const override;

    std::string toString() const override {
        // TODO
        return "";
    }
};


// $^.TagName.any_prop_name
class SourcePropertyExpression final : public AliasPropertyExpression {
public:
    SourcePropertyExpression(std::string* tag = nullptr,
                             std::string* prop = nullptr)
        : AliasPropertyExpression(Kind::kSrcProperty,
                                  new std::string(kSrcRef),
                                  tag,
                                  prop) {}

    Value eval(const ExpressionContext& ctx) const override;

    std::string toString() const override {
        // TODO
        return "";
    }
};


// $$.TagName.any_prop_name
class DestPropertyExpression final : public AliasPropertyExpression {
public:
    DestPropertyExpression(std::string* tag = nullptr,
                           std::string* prop = nullptr)
        : AliasPropertyExpression(Kind::kDstProperty,
                                  new std::string(kDstRef),
                                  tag,
                                  prop) {}

    Value eval(const ExpressionContext& ctx) const override;

    std::string toString() const override {
        // TODO
        return "";
    }
};


// Alias._src, i.e. EdgeName._src
class EdgeSrcIdExpression final : public AliasPropertyExpression {
public:
    explicit EdgeSrcIdExpression(std::string* alias = nullptr)
        : AliasPropertyExpression(Kind::kEdgeSrc,
                                  new std::string(""),
                                  alias,
                                  new std::string(_SRC)) {}

    Value eval(const ExpressionContext& ctx) const override;

    std::string toString() const override {
        // TODO
        return "";
    }
};


// Alias._type, i.e. EdgeName._type
class EdgeTypeExpression final : public AliasPropertyExpression {
public:
    explicit EdgeTypeExpression(std::string* alias = nullptr)
        : AliasPropertyExpression(Kind::kEdgeType,
                                  new std::string(""),
                                  alias,
                                  new std::string(_TYPE)) {}

    Value eval(const ExpressionContext& ctx) const override;

    std::string toString() const override {
        // TODO
        return "";
    }
};


// Alias._rank, i.e. EdgeName._rank
class EdgeRankExpression final : public AliasPropertyExpression {
public:
    explicit EdgeRankExpression(std::string* alias = nullptr)
        : AliasPropertyExpression(Kind::kEdgeRank,
                                  new std::string(""),
                                  alias,
                                  new std::string(_RANK)) {}

    Value eval(const ExpressionContext& ctx) const override;

    std::string toString() const override {
        // TODO
        return "";
    }
};


// Alias._dst, i.e. EdgeName._dst
class EdgeDstIdExpression final : public AliasPropertyExpression {
public:
    explicit EdgeDstIdExpression(std::string* alias = nullptr)
        : AliasPropertyExpression(Kind::kEdgeDst,
                                  new std::string(""),
                                  alias,
                                  new std::string(_DST)) {}

    Value eval(const ExpressionContext& ctx) const override;

    std::string toString() const override {
        // TODO
        return "";
    }
};

}  // namespace nebula
#endif  // EXPRESSION_ALIASPROPERTYEXPRESSION_H_
