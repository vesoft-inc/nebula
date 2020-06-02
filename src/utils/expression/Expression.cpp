/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/Expression.h"
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include "common/datatypes/ValueOps.h"
#include "common/expression/AliasPropertyExpression.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"

namespace nebula {

using serializer = apache::thrift::CompactSerializer;

/****************************************
 *
 *  class Expression::Encoder
 *
 ***************************************/
Expression::Encoder::Encoder(size_t bufSizeHint) {
    buf_.reserve(bufSizeHint);
}


std::string Expression::Encoder::moveStr() {
    return std::move(buf_);
}


Expression::Encoder& Expression::Encoder::operator<<(Kind kind) noexcept {
    buf_.append(reinterpret_cast<const char*>(&kind), sizeof(uint8_t));
    return *this;
}


Expression::Encoder& Expression::Encoder::operator<<(const std::string* str) noexcept {
    size_t sz = str ? str->size() : 0;
    buf_.append(reinterpret_cast<char*>(&sz), sizeof(size_t));
    if (sz > 0) {
        buf_.append(str->data(), sz);
    }
    return *this;
}


Expression::Encoder& Expression::Encoder::operator<<(const Value& val) noexcept {
    serializer::serialize(val, &buf_);
    return *this;
}


Expression::Encoder& Expression::Encoder::operator<<(size_t size) noexcept {
    buf_.append(reinterpret_cast<char*>(&size), sizeof(size_t));
    return *this;
}


Expression::Encoder& Expression::Encoder::operator<<(Value::Type vType) noexcept {
    buf_.append(reinterpret_cast<char*>(&vType), sizeof(uint8_t));
    return *this;
}


Expression::Encoder& Expression::Encoder::operator<<(const Expression& exp) noexcept {
    exp.writeTo(*this);
    return *this;
}


/****************************************
 *
 *  class Expression::Decoder
 *
 ***************************************/
Expression::Decoder::Decoder(folly::StringPiece encoded)
    : encoded_(encoded)
    , ptr_(encoded_.begin()) {}


bool Expression::Decoder::finished() const {
    return ptr_ >= encoded_.end();
}


std::string Expression::Decoder::getHexStr() const {
    return toHexStr(encoded_);
}


Expression::Kind Expression::Decoder::readKind() noexcept {
    CHECK_LE(ptr_ + sizeof(uint8_t), encoded_.end());

    Expression::Kind kind;
    memcpy(reinterpret_cast<void*>(&kind), ptr_, sizeof(uint8_t));
    ptr_ += sizeof(uint8_t);

    return kind;
}


std::unique_ptr<std::string> Expression::Decoder::readStr() noexcept {
    CHECK_LE(ptr_ + sizeof(size_t), encoded_.end());

    size_t sz = 0;
    memcpy(reinterpret_cast<void*>(&sz), ptr_, sizeof(size_t));
    ptr_ += sizeof(size_t);

    std::unique_ptr<std::string> str;
    if (sz > 0) {
        CHECK_LE(ptr_ + sz, encoded_.end());
        str.reset(new std::string(ptr_, sz));
    } else {
        str.reset(new std::string());
    }
    ptr_ += sz;

    return str;
}


Value Expression::Decoder::readValue() noexcept {
    Value val;
    size_t len = serializer::deserialize(folly::StringPiece(ptr_, encoded_.end()), val);
    ptr_ += len;
    return val;
}


size_t Expression::Decoder::readSize() noexcept {
    size_t sz = 0;
    memcpy(reinterpret_cast<void*>(&sz), ptr_, sizeof(size_t));
    ptr_ += sizeof(size_t);
    return sz;
}


Value::Type Expression::Decoder::readValueType() noexcept {
    Value::Type type;
    memcpy(reinterpret_cast<void*>(&type), ptr_, sizeof(uint8_t));
    ptr_ += sizeof(uint8_t);
    return type;
}


std::unique_ptr<Expression> Expression::Decoder::readExpression() noexcept {
    return Expression::decode(*this);
}


/****************************************
 *
 *  class Expression
 *
 ***************************************/
// static
std::string Expression::encode(const Expression& exp) {
    Encoder encoder;
    exp.writeTo(encoder);
    return encoder.moveStr();
}


// static
std::unique_ptr<Expression> Expression::decode(folly::StringPiece encoded) {
    Decoder decoder(encoded);
    if (decoder.finished()) {
        return nullptr;
    }
    return decode(decoder);
}


// static
std::unique_ptr<Expression> Expression::decode(Expression::Decoder& decoder) {
    Kind kind = decoder.readKind();
    switch (kind) {
        case Expression::Kind::kConstant: {
            auto exp = std::make_unique<ConstantExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kAdd: {
            auto exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kAdd);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kMinus: {
            auto exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kMinus);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kMultiply: {
            auto exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kMultiply);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kDivision: {
            auto exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kDivision);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kMod: {
            auto exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kMod);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kUnaryPlus: {
            auto exp = std::make_unique<UnaryExpression>(Expression::Kind::kUnaryPlus);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kUnaryNegate: {
            auto exp = std::make_unique<UnaryExpression>(Expression::Kind::kUnaryNegate);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kUnaryNot: {
            auto exp = std::make_unique<UnaryExpression>(Expression::Kind::kUnaryNot);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelEQ: {
            auto exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelEQ);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelNE: {
            auto exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelNE);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelLT: {
            auto exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelLT);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelLE: {
            auto exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelLE);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelGT: {
            auto exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelGT);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelGE: {
            auto exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelGE);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kLogicalAnd: {
            auto exp = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kLogicalOr: {
            auto exp = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalOr);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kLogicalXor: {
            auto exp = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalXor);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kTypeCasting: {
            auto exp = std::make_unique<TypeCastingExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kFunctionCall: {
            auto exp = std::make_unique<FunctionCallExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kAliasProperty: {
            return nullptr;
        }
        case Expression::Kind::kInputProperty: {
            auto exp = std::make_unique<InputPropertyExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kVarProperty: {
            auto exp = std::make_unique<VariablePropertyExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kDstProperty: {
            auto exp = std::make_unique<DestPropertyExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kSrcProperty: {
            auto exp = std::make_unique<SourcePropertyExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdgeSrc: {
            auto exp = std::make_unique<EdgeSrcIdExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdgeType: {
            auto exp = std::make_unique<EdgeTypeExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdgeRank: {
            auto exp = std::make_unique<EdgeRankExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdgeDst: {
            auto exp = std::make_unique<EdgeDstIdExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kUUID: {
            auto exp = std::make_unique<UUIDExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        default: {
            break;
        }
    }

    LOG(FATAL) << "Unknown expression: " << decoder.getHexStr();
}


std::ostream& operator<<(std::ostream& os, Expression::Kind kind) {
    switch (kind) {
        case Expression::Kind::kConstant:
            os << "Constant";
            break;
        case Expression::Kind::kAdd:
            os << "Add";
            break;
        case Expression::Kind::kMinus:
            os << "Minus";
            break;
        case Expression::Kind::kMultiply:
            os << "Multiply";
            break;
        case Expression::Kind::kDivision:
            os << "Division";
            break;
        case Expression::Kind::kMod:
            os << "Mod";
            break;
        case Expression::Kind::kUnaryPlus:
            os << "UnaryPlus";
            break;
        case Expression::Kind::kUnaryNegate:
            os << "UnaryNegate";
            break;
        case Expression::Kind::kUnaryNot:
            os << "UnaryNot";
            break;
        case Expression::Kind::kRelEQ:
            os << "Equal";
            break;
        case Expression::Kind::kRelNE:
            os << "NotEuqal";
            break;
        case Expression::Kind::kRelLT:
            os << "LessThan";
            break;
        case Expression::Kind::kRelLE:
            os << "LessEqual";
            break;
        case Expression::Kind::kRelGT:
            os << "GreaterThan";
            break;
        case Expression::Kind::kRelGE:
            os << "GreaterEqual";
            break;
        case Expression::Kind::kLogicalAnd:
            os << "LogicalAnd";
            break;
        case Expression::Kind::kLogicalOr:
            os << "LogicalOr";
            break;
        case Expression::Kind::kLogicalXor:
            os << "LogicalXor";
            break;
        case Expression::Kind::kTypeCasting:
            os << "TypeCasting";
            break;
        case Expression::Kind::kFunctionCall:
            os << "FunctionCall";
            break;
        case Expression::Kind::kAliasProperty:
            os << "AliasProp";
            break;
        case Expression::Kind::kInputProperty:
            os << "InputProp";
            break;
        case Expression::Kind::kVarProperty:
            os << "VarProp";
            break;
        case Expression::Kind::kDstProperty:
            os << "DstProp";
            break;
        case Expression::Kind::kSrcProperty:
            os << "SrcProp";
            break;
        case Expression::Kind::kEdgeSrc:
            os << "EdgeSrc";
            break;
        case Expression::Kind::kEdgeType:
            os << "EdgeType";
            break;
        case Expression::Kind::kEdgeRank:
            os << "EdgeRank";
            break;
        case Expression::Kind::kEdgeDst:
            os << "EdgeDst";
            break;
        case Expression::Kind::kUUID:
            os << "UUID";
            break;
    }
    return os;
}

}  // namespace nebula
