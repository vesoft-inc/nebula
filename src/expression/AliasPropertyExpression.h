/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXPRESSION_ALIASPROPERTYEXPRESSION_H_
#define EXPRESSION_ALIASPROPERTYEXPRESSION_H_

#include "expression/Expression.h"

namespace nebula {

constexpr char INPUT_REF[]  = "$-";
constexpr char VAR_REF[]    = "$";
constexpr char SRC_REF[]    = "$^";
constexpr char DST_REF[]    = "$$";

// Alias.any_prop_name, i.e. EdgeName.any_prop_name
class AliasPropertyExpression: public Expression {
public:
    AliasPropertyExpression(Type type = Type::EXP_ALIAS_PROPERTY,
                            std::string* ref = nullptr,
                            std::string* alias = nullptr,
                            std::string* prop = nullptr)
        : Expression(type) {
        ref_.reset(ref);
        alias_.reset(alias);
        prop_.reset(prop);
    }

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }

protected:
    std::unique_ptr<std::string>    ref_;
    std::unique_ptr<std::string>    alias_;
    std::unique_ptr<std::string>    prop_;
};

// $-.any_prop_name
class InputPropertyExpression final : public AliasPropertyExpression {
public:
    explicit InputPropertyExpression(std::string* prop)
        : AliasPropertyExpression(Type::EXP_INPUT_PROPERTY,
                                  new std::string(INPUT_REF),
                                  new std::string(""),
                                  prop) {}

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }
};

// $VarName.any_prop_name
class VariablePropertyExpression final : public AliasPropertyExpression {
public:
    VariablePropertyExpression(std::string* var,
                               std::string* prop)
        : AliasPropertyExpression(Type::EXP_VAR_PROPERTY,
                                  new std::string(VAR_REF),
                                  var,
                                  prop) {}

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }
};

// $^.TagName.any_prop_name
class SourcePropertyExpression final : public AliasPropertyExpression {
public:
    SourcePropertyExpression(std::string* tag,
                             std::string* prop)
        : AliasPropertyExpression(Type::EXP_SRC_PROPERTY,
                                  new std::string(SRC_REF),
                                  tag,
                                  prop) {}

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }
};

// $$.TagName.any_prop_name
class DestPropertyExpression final : public AliasPropertyExpression {
public:
    DestPropertyExpression(std::string* tag,
                           std::string* prop)
        : AliasPropertyExpression(Type::EXP_DST_PROPERTY,
                                  new std::string(DST_REF),
                                  tag,
                                  prop) {}

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }
};

// Alias._src, i.e. EdgeName._src
class EdgeSrcIdExpression final : public AliasPropertyExpression {
public:
    explicit EdgeSrcIdExpression(std::string* alias)
        : AliasPropertyExpression(Type::EXP_EDGE_SRC,
                                  new std::string(""),
                                  alias,
                                  new std::string(_SRC)) {}

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }
};

// Alias._type, i.e. EdgeName._type
class EdgeTypeExpression final : public AliasPropertyExpression {
public:
    explicit EdgeTypeExpression(std::string* alias)
        : AliasPropertyExpression(Type::EXP_EDGE_TYPE,
                                  new std::string(""),
                                  alias,
                                  new std::string(_TYPE)) {}

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }
};

// Alias._rank, i.e. EdgeName._rank
class EdgeRankExpression final : public AliasPropertyExpression {
public:
    explicit EdgeRankExpression(std::string* alias)
        : AliasPropertyExpression(Type::EXP_EDGE_RANK,
                                  new std::string(""),
                                  alias,
                                  new std::string(_RANK)) {}

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }
};

// Alias._dst, i.e. EdgeName._dst
class EdgeDstIdExpression final : public AliasPropertyExpression {
public:
    explicit EdgeDstIdExpression(std::string* alias)
        : AliasPropertyExpression(Type::EXP_EDGE_DST,
                                  new std::string(""),
                                  alias,
                                  new std::string(_DST)) {}

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }
};
}  // namespace nebula
#endif
