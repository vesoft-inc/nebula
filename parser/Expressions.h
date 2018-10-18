/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_EXPRESSIONS_H_
#define PARSER_EXPRESSIONS_H_

#include "base/Base.h"
#include "base/Status.h"
#include <boost/variant.hpp>

namespace vesoft {

enum ColumnType {
    INT8, INT16, INT32, INT64,
    UINT8, UINT16, UINT32, UINT64,
    STRING, DOUBLE, BIGINT, BOOL,
};

std::string columnTypeToString(ColumnType type);


enum AliasKind {
    Unknown,
    SourceNode,
    Edge,
};

std::string aliasKindToString(AliasKind kind);

struct AliasInfo {
    AliasKind                               kind_;
    std::string                             data_;
};


class ExpressionContext final {
public:
    void addSrcNodeProp(const std::string &prop) {
        srcNodePropNames_.emplace(prop);
    }

    void addDstNodeProp(const std::string &prop) {
        dstNodePropNames_.emplace(prop);
    }

    void addEdgeProp(const std::string &prop) {
        edgePropNames_.emplace(prop);
    }

    Status addAliasProp(const std::string &alias, const std::string &prop);

    void addAlias(const std::string &alias, AliasKind kind) {
        aliasInfo_[alias].kind_ = kind;
    }

    void addAlias(const std::string &alias, AliasKind kind, const std::string &data) {
        auto &entry = aliasInfo_[alias];
        entry.kind_ = kind;
        entry.data_ = data;
    }

    AliasKind aliasKind(const std::string &alias) {
        auto iter = aliasInfo_.find(alias);
        if (iter == aliasInfo_.end()) {
            return AliasKind::Unknown;
        }
        return iter->second.kind_;
    }

    std::vector<std::string> srcNodePropNames() const {
        return std::vector<std::string>(srcNodePropNames_.begin(), srcNodePropNames_.end());
    }

    std::vector<std::string> dstNodePropNames() const {
        return std::vector<std::string>(dstNodePropNames_.begin(), dstNodePropNames_.end());
    }

    std::vector<std::string> edgePropNames() const {
        return std::vector<std::string>(edgePropNames_.begin(), edgePropNames_.end());
    }

    void print() const;

private:
    std::unordered_map<std::string, AliasInfo>  aliasInfo_;
    std::unordered_set<std::string>             srcNodePropNames_;
    std::unordered_set<std::string>             dstNodePropNames_;
    std::unordered_set<std::string>             edgePropNames_;
};


using VariantType = boost::variant<int64_t, uint64_t, double, bool, std::string>;


class Expression {
public:
    virtual ~Expression() {}

    virtual std::string toString() const = 0;

    virtual Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) = 0;

    virtual VariantType eval() const = 0;

    static int64_t asInt(const VariantType &value) {
        return boost::get<int64_t>(value);
    }

    static uint64_t asUInt(const VariantType &value) {
        return boost::get<uint64_t>(value);
    }

    static double asDouble(const VariantType &value) {
        if (value.which() == 0) {
            return (double)boost::get<int64_t>(value);
        }
        if (value.which() == 1) {
            return (double)boost::get<uint64_t>(value);
        }
        return boost::get<double>(value);
    }

    static bool asBool(const VariantType &value) {
        switch (value.which()) {
            case 0:
                return asInt(value) != 0;
            case 1:
                return asUInt(value) != 0;
            case 2:
                return asDouble(value) != 0.0;
            case 3:
                return boost::get<bool>(value);
            case 4:
                return asString(value).empty();
            default:
                DCHECK(false);
        }
        return false;
    }

    static const std::string& asString(const VariantType &value) {
        return boost::get<std::string>(value);
    }

    static bool isInt(const VariantType &value) {
        return value.which() == 0;
    }

    static bool isUInt(const VariantType &value) {
        return value.which() == 1;
    }

    static bool isDouble(const VariantType &value) {
        return value.which() == 2;
    }

    static bool isBool(const VariantType &value) {
        return value.which() == 3;
    }

    static bool isString(const VariantType &value) {
        return value.which() == 4;
    }

    static bool isArithmetic(const VariantType &value) {
        return isInt(value) || isUInt(value) || isDouble(value);
    }

    static void print(const VariantType &value);

protected:
    ExpressionContext                          *context_{nullptr};
};


class PropertyExpression final : public Expression {
public:
    explicit PropertyExpression(std::string *minorProp) {
        minorPropName_.reset(minorProp);
    }

    PropertyExpression(std::string *majorProp, std::string *minorProp) {
        minorPropName_.reset(minorProp);
        majorPropName_.reset(majorProp);
    }

    PropertyExpression(std::string *majorProp, std::string *minorProp, std::string *tag) {
        minorPropName_.reset(minorProp);
        majorPropName_.reset(majorProp);
        tag_.reset(tag);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override {
        context_ = context;
        return Status::OK();
    }

private:
    std::unique_ptr<std::string>                majorPropName_;
    std::unique_ptr<std::string>                minorPropName_;
    std::unique_ptr<std::string>                tag_;
};


class InputPropertyExpression final : public Expression {
public:
    explicit InputPropertyExpression(std::string *prop) {
        prop_.reset(prop);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override {
        context_ = context;
        return Status::OK();
    }

private:
    std::unique_ptr<std::string>                prop_;
};


class InputIdPropertyExpression final : public Expression {
public:
    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override {
        context_ = context;
        return Status::OK();
    }

private:
};


class InputTypePropertyExpression final : public Expression {
public:
    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override {
        context_ = context;
        return Status::OK();
    }

private:
};


class InputTaggedPropertyExpression final : public Expression {
public:
    InputTaggedPropertyExpression(std::string *tag, std::string *prop) {
        tag_.reset(tag);
        prop_.reset(prop);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override {
        context_ = context;
        return Status::NotSupported("Tagged property not supported yet");
    }

private:
    std::unique_ptr<std::string>                tag_;
    std::unique_ptr<std::string>                prop_;
};


class DestPropertyExpression final : public Expression {
public:
    explicit DestPropertyExpression(std::string *prop) {
        prop_.reset(prop);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                prop_;
};


class DestIdPropertyExpression final : public Expression {
public:
    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
};


class DestTypePropertyExpression final : public Expression {
public:
    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
};


class DestTaggedPropertyExpression final : public Expression {
public:
    DestTaggedPropertyExpression(std::string *tag, std::string *prop) {
        tag_.reset(tag);
        prop_.reset(prop);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                tag_;
    std::unique_ptr<std::string>                prop_;
};


class VariablePropertyExpression final : public Expression {
public:
    VariablePropertyExpression(std::string *var, std::string *prop) {
        var_.reset(var);
        prop_.reset(prop);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                var_;
    std::unique_ptr<std::string>                prop_;
};


class VariableIdPropertyExpression final : public Expression {
public:
    explicit VariableIdPropertyExpression(std::string *var) {
        var_.reset(var);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                var_;
};


class VariableTypePropertyExpression final : public Expression {
public:
    explicit VariableTypePropertyExpression(std::string *var) {
        var_.reset(var);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                var_;
};


class VariableTaggedPropertyExpression final : public Expression {
public:
    VariableTaggedPropertyExpression(std::string *var, std::string *tag, std::string *prop) {
        var_.reset(var);
        tag_.reset(tag);
        prop_.reset(prop);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                var_;
    std::unique_ptr<std::string>                tag_;
    std::unique_ptr<std::string>                prop_;
};


class VariableTaggedIdPropertyExpression final : public Expression {
public:
    VariableTaggedIdPropertyExpression(std::string *var, std::string *tag) {
        var_.reset(var);
        tag_.reset(tag);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                var_;
    std::unique_ptr<std::string>                tag_;
};


class VariableTaggedTypePropertyExpression final : public Expression {
public:
    VariableTaggedTypePropertyExpression(std::string *var, std::string *tag) {
        var_.reset(var);
        tag_.reset(tag);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                var_;
    std::unique_ptr<std::string>                tag_;
};


class AliasPropertyExpression final : public Expression {
public:
    AliasPropertyExpression(std::string *alias, std::string *prop) {
        alias_.reset(alias);
        prop_.reset(prop);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                alias_;
    std::unique_ptr<std::string>                prop_;
};


class AliasIdPropertyExpression final : public Expression {
public:
    explicit AliasIdPropertyExpression(std::string *alias) {
        alias_.reset(alias);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                alias_;
};


class AliasTypePropertyExpression final : public Expression {
public:
    explicit AliasTypePropertyExpression(std::string *alias) {
        alias_.reset(alias);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                alias_;
};


class EdgeSrcIdExpression final : public Expression {
public:
    explicit EdgeSrcIdExpression(std::string *alias) {
        alias_.reset(alias);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                alias_;
};


class EdgeDstIdExpression final : public Expression {
public:
    explicit EdgeDstIdExpression(std::string *alias) {
        alias_.reset(alias);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                alias_;
};


class EdgeRankExpression final : public Expression {
public:
    explicit EdgeRankExpression(std::string *alias) {
        alias_.reset(alias);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                alias_;
};


class AliasTaggedPropertyExpression final : public Expression {
public:
    AliasTaggedPropertyExpression(std::string *alias, std::string *tag, std::string *prop) {
        alias_.reset(alias);
        tag_.reset(tag);
        prop_.reset(prop);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                alias_;
    std::unique_ptr<std::string>                tag_;
    std::unique_ptr<std::string>                prop_;
};


class AliasTaggedIdPropertyExpression final : public Expression {
public:
    AliasTaggedIdPropertyExpression(std::string *alias, std::string *tag) {
        alias_.reset(alias);
        tag_.reset(tag);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                alias_;
    std::unique_ptr<std::string>                tag_;
};


class AliasTaggedTypePropertyExpression final : public Expression {
public:
    AliasTaggedTypePropertyExpression(std::string *alias, std::string *tag) {
        alias_.reset(alias);
        tag_.reset(tag);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    std::unique_ptr<std::string>                alias_;
    std::unique_ptr<std::string>                tag_;
};


class PrimaryExpression final : public Expression {
public:
    using Operand = boost::variant<bool, int64_t, uint64_t, double, std::string>;

    PrimaryExpression(bool val) {
        operand_ = val;
    }

    PrimaryExpression(int64_t val) {
        operand_ = val;
    }

    PrimaryExpression(uint64_t val) {
        operand_ = val;
    }

    PrimaryExpression(double val) {
        operand_ = val;
    }

    PrimaryExpression(std::string val) {
        operand_ = std::move(val);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    Operand                                     operand_;
};


class UnaryExpression final : public Expression {
public:
    enum Operator {
        PLUS, MINUS, NOT
    };

    UnaryExpression(Operator op, Expression *operand) {
        op_ = op;
        operand_.reset(operand);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 operand_;
};


class TypeCastingExpression final : public Expression {
public:
    TypeCastingExpression(ColumnType type, Expression *operand) {
        type_ = type;
        operand_.reset(operand);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    ColumnType                                  type_;
    std::unique_ptr<Expression>                 operand_;
};


class ArithmeticExpression final : public Expression {
public:
    enum Operator {
        ADD, SUB, MUL, DIV, MOD
    };

    ArithmeticExpression(Expression *left, Operator op, Expression *right) {
        op_ = op;
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};


class RelationalExpression final : public Expression {
public:
    enum Operator {
        LT, LE, GT, GE, EQ, NE
    };

    RelationalExpression(Expression *left, Operator op, Expression *right) {
        op_ = op;
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};


class LogicalExpression final : public Expression {
public:
    enum Operator {
        AND, OR
    };

    LogicalExpression(Expression *left, Operator op, Expression *right) {
        op_ = op;
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override;

    VariantType eval() const override;

    Status VE_MUST_USE_RESULT prepare(ExpressionContext *context) override;

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};

}   // namespace vesoft

#endif  // PARSER_EXPRESSIONS_H_
