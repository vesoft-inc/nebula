/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_EXPRESSIONS_H_
#define PARSER_EXPRESSIONS_H_

#include "base/Base.h"
#include <boost/variant.hpp>

namespace vesoft {

enum ColumnType {
    INT8, INT16, INT32, INT64,
    UINT8, UINT16, UINT32, UINT64,
    STRING, DOUBLE, BIGINT, BOOL,
};

std::string columnTypeToString(ColumnType type);

class Expression {
public:
    virtual ~Expression() {}

    virtual std::string toString() const = 0;

    using ReturnType = boost::variant<int64_t, uint64_t, double, bool, std::string>;
    virtual ReturnType eval() const = 0;

    static int64_t asInt(const ReturnType &value) {
        return boost::get<int64_t>(value);
    }

    static uint64_t asUInt(const ReturnType &value) {
        return boost::get<uint64_t>(value);
    }

    static double asDouble(const ReturnType &value) {
        if (value.which() == 0) {
            return (double)boost::get<int64_t>(value);
        }
        if (value.which() == 1) {
            return (double)boost::get<uint64_t>(value);
        }
        return boost::get<double>(value);
    }

    static bool asBool(const ReturnType &value) {
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
                assert(false);
        }
    }

    static const std::string& asString(const ReturnType &value) {
        return boost::get<std::string>(value);
    }

    static bool isInt(const ReturnType &value) {
        return value.which() == 0;
    }

    static bool isUInt(const ReturnType &value) {
        return value.which() == 1;
    }

    static bool isDouble(const ReturnType &value) {
        return value.which() == 2;
    }

    static bool isBool(const ReturnType &value) {
        return value.which() == 3;
    }

    static bool isString(const ReturnType &value) {
        return value.which() == 4;
    }

    static bool isArithmetic(const ReturnType &value) {
        return isInt(value) || isUInt(value) || isDouble(value);
    }

    static void print(const ReturnType &value);
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

    ReturnType eval() const override;

private:
    std::unique_ptr<std::string>                majorPropName_;
    std::unique_ptr<std::string>                minorPropName_;
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
        operand_ = val;
    }

    std::string toString() const override;

    ReturnType eval() const override;

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

    ReturnType eval() const override;

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

    ReturnType eval() const override;

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

    ReturnType eval() const override;

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

    ReturnType eval() const override;

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

    ReturnType eval() const override;

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};

}   // namespace vesoft

#endif  // PARSER_EXPRESSIONS_H_
