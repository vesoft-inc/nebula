/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_EXPRESSION_H_
#define COMMON_EXPRESSION_EXPRESSION_H_

#include "common/base/Base.h"
#include "common/datatypes/Value.h"
#include "common/context/ExpressionContext.h"

namespace nebula {

class ExprVisitor;

class Expression {
public:
    enum class Kind : uint8_t {
        kConstant,

        kAdd,
        kMinus,
        kMultiply,
        kDivision,
        kMod,

        kUnaryPlus,
        kUnaryNegate,
        kUnaryNot,
        kUnaryIncr,
        kUnaryDecr,

        kRelEQ,
        kRelNE,
        kRelLT,
        kRelLE,
        kRelGT,
        kRelGE,
        kRelREG,
        kRelIn,
        kRelNotIn,
        kContains,
        kNotContains,
        kStartsWith,
        kNotStartsWith,
        kEndsWith,
        kNotEndsWith,
        kSubscript,
        kAttribute,
        kLabelAttribute,
        kColumn,

        kLogicalAnd,
        kLogicalOr,
        kLogicalXor,

        kTypeCasting,

        kFunctionCall,

        kTagProperty,
        kEdgeProperty,
        kInputProperty,
        kVarProperty,
        kDstProperty,
        kSrcProperty,
        kEdgeSrc,
        kEdgeType,
        kEdgeRank,
        kEdgeDst,
        kVertex,
        kEdge,

        kUUID,

        kVar,
        kVersionedVar,

        kList,
        kSet,
        kMap,

        kLabel,

        kCase,

        kPredicate,
        kListComprehension,
        kReduce,

        kPathBuild,
        // text or key word search expression
        kTSPrefix,
        kTSWildcard,
        kTSRegexp,
        kTSFuzzy,

        kAggregate,
        kIsNull,
        kIsNotNull,
        kIsEmpty,
        kIsNotEmpty,
    };


    explicit Expression(Kind kind) : kind_(kind) {}

    virtual ~Expression() = default;

    Kind kind() const {
        return kind_;
    }

    static Value eval(Expression* expr, ExpressionContext& ctx) {
        return expr->eval(ctx);
    }

    virtual const Value& eval(ExpressionContext& ctx) = 0;

    virtual bool operator==(const Expression& rhs) const = 0;
    bool operator!=(const Expression& rhs) const {
        return !operator==(rhs);
    }

    virtual std::string toString() const = 0;

    virtual void accept(ExprVisitor* visitor) = 0;

    // Deep copy
    virtual std::unique_ptr<Expression> clone() const = 0;

    std::string encode() const;

    static std::string encode(const Expression& exp);

    static std::unique_ptr<Expression> decode(folly::StringPiece encoded);

protected:
    class Encoder final {
    public:
        explicit Encoder(size_t bufSizeHint = 2048);
        std::string moveStr();

        Encoder& operator<<(Kind kind) noexcept;
        Encoder& operator<<(const std::string* str) noexcept;
        Encoder& operator<<(const Value& val) noexcept;
        Encoder& operator<<(size_t size) noexcept;
        Encoder& operator<<(Value::Type vType) noexcept;
        Encoder& operator<<(const Expression& exp) noexcept;

    private:
        std::string buf_;
    };

    class Decoder final {
    public:
        explicit Decoder(folly::StringPiece encoded);

        bool finished() const;

        Kind readKind() noexcept;
        std::unique_ptr<std::string> readStr() noexcept;
        Value readValue() noexcept;
        size_t readSize() noexcept;
        Value::Type readValueType() noexcept;
        std::unique_ptr<Expression> readExpression() noexcept;

        // Convert the unprocessed part into the hex string
        std::string getHexStr() const;

    private:
        folly::StringPiece encoded_;
        const char* ptr_;
    };

protected:
    static std::unique_ptr<Expression> decode(Decoder& decoder);

    // Serialize the content of the expression to the given encoder
    virtual void writeTo(Encoder& encoder) const = 0;

    // Reset the content of the expression from the given decoder
    virtual void resetFrom(Decoder& decoder) = 0;

    Kind kind_;
};

std::ostream& operator<<(std::ostream& os, Expression::Kind kind);

}   // namespace nebula

#endif   // COMMON_EXPRESSION_EXPRESSION_H_
