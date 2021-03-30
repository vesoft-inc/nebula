/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/Expression.h"

#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "common/datatypes/ValueOps.inl"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/AttributeExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/EdgeExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/AggregateExpression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PathBuildExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/VertexExpression.h"
#include "common/expression/CaseExpression.h"
#include "common/expression/ColumnExpression.h"
#include "common/expression/ListComprehensionExpression.h"
#include "common/expression/PredicateExpression.h"
#include "common/expression/ReduceExpression.h"

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
    buf_.append(reinterpret_cast<char*>(&vType), sizeof(Value::Type));
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
    memcpy(reinterpret_cast<void*>(&type), ptr_, sizeof(Value::Type));
    ptr_ += sizeof(Value::Type);
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
    return exp.encode();
}

std::string Expression::encode() const {
    Encoder encoder;
    writeTo(encoder);
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
    std::unique_ptr<Expression> exp = nullptr;
    Kind kind = decoder.readKind();
    switch (kind) {
        case Expression::Kind::kConstant: {
            exp = std::make_unique<ConstantExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kAdd: {
            exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kAdd);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kMinus: {
            exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kMinus);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kMultiply: {
            exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kMultiply);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kDivision: {
            exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kDivision);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kMod: {
            exp = std::make_unique<ArithmeticExpression>(Expression::Kind::kMod);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kUnaryPlus: {
            exp = std::make_unique<UnaryExpression>(Expression::Kind::kUnaryPlus);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kUnaryNegate: {
            exp = std::make_unique<UnaryExpression>(Expression::Kind::kUnaryNegate);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kUnaryNot: {
            exp = std::make_unique<UnaryExpression>(Expression::Kind::kUnaryNot);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kUnaryIncr: {
            exp = std::make_unique<UnaryExpression>(Expression::Kind::kUnaryIncr);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kUnaryDecr: {
            exp = std::make_unique<UnaryExpression>(Expression::Kind::kUnaryDecr);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kIsNull: {
            exp = std::make_unique<UnaryExpression>(Expression::Kind::kIsNull);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kIsNotNull: {
            exp = std::make_unique<UnaryExpression>(Expression::Kind::kIsNotNull);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kIsEmpty: {
            exp = std::make_unique<UnaryExpression>(Expression::Kind::kIsEmpty);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kIsNotEmpty: {
            exp = std::make_unique<UnaryExpression>(Expression::Kind::kIsNotEmpty);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelEQ: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelEQ);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelNE: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelNE);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelLT: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelLT);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelLE: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelLE);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelGT: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelGT);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelGE: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelGE);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelREG: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelREG);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelIn: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelIn);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kRelNotIn: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kRelNotIn);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kContains: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kContains);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kNotContains: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kNotContains);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kStartsWith: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kStartsWith);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kNotStartsWith: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kNotStartsWith);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEndsWith: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kEndsWith);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kNotEndsWith: {
            exp = std::make_unique<RelationalExpression>(Expression::Kind::kNotEndsWith);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kSubscript: {
            exp = std::make_unique<SubscriptExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kColumn: {
            exp = std::make_unique<ColumnExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kAttribute: {
            exp = std::make_unique<AttributeExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kLabelAttribute: {
            exp = std::make_unique<LabelAttributeExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kLogicalAnd: {
            exp = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kLogicalOr: {
            exp = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalOr);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kLogicalXor: {
            exp = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalXor);
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kTypeCasting: {
            exp = std::make_unique<TypeCastingExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kFunctionCall: {
            exp = std::make_unique<FunctionCallExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kAggregate: {
            exp = std::make_unique<AggregateExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kInputProperty: {
            LOG(FATAL) << "Should not decode input property expression";
            return exp;
        }
        case Expression::Kind::kVarProperty: {
            LOG(FATAL) << "Should not decode variable property expression";
            return exp;
        }
        case Expression::Kind::kDstProperty: {
            exp = std::make_unique<DestPropertyExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kSrcProperty: {
            exp = std::make_unique<SourcePropertyExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdgeProperty: {
            exp = std::make_unique<EdgePropertyExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kTagProperty: {
            exp = std::make_unique<TagPropertyExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdgeSrc: {
            exp = std::make_unique<EdgeSrcIdExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdgeType: {
            exp = std::make_unique<EdgeTypeExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdgeRank: {
            exp = std::make_unique<EdgeRankExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdgeDst: {
            exp = std::make_unique<EdgeDstIdExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kVertex: {
            exp = std::make_unique<VertexExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kEdge: {
            exp = std::make_unique<EdgeExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kVar: {
            exp = std::make_unique<VariableExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kVersionedVar: {
            LOG(FATAL) << "Should not decode version variable expression";
            return exp;
        }
        case Expression::Kind::kUUID: {
            exp = std::make_unique<UUIDExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kList: {
            exp = std::make_unique<ListExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kSet: {
            exp = std::make_unique<SetExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kMap: {
            exp = std::make_unique<MapExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kLabel: {
            exp = std::make_unique<LabelExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kCase: {
            exp = std::make_unique<CaseExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kPathBuild: {
            exp = std::make_unique<PathBuildExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kListComprehension: {
            exp = std::make_unique<ListComprehensionExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kPredicate: {
            exp = std::make_unique<PredicateExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kReduce: {
            exp = std::make_unique<ReduceExpression>();
            exp->resetFrom(decoder);
            return exp;
        }
        case Expression::Kind::kTSPrefix:
        case Expression::Kind::kTSWildcard:
        case Expression::Kind::kTSRegexp:
        case Expression::Kind::kTSFuzzy: {
            LOG(FATAL) << "Should not decode text search expression";
            return exp;
        }
        // no default so the compiler will warning when lack
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
        case Expression::Kind::kUnaryIncr:
            os << "AutoIncrement";
            break;
        case Expression::Kind::kUnaryDecr:
            os << "AutoDecrement";
            break;
        case Expression::Kind::kIsNull:
            os << "IsNull";
            break;
        case Expression::Kind::kIsNotNull:
            os << "IsNotNull";
            break;
        case Expression::Kind::kIsEmpty:
            os << "IsEmpty";
            break;
        case Expression::Kind::kIsNotEmpty:
            os << "IsNotEmpty";
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
        case Expression::Kind::kRelREG:
            os << "RegexMatch";
            break;
        case Expression::Kind::kRelIn:
            os << "In";
            break;
        case Expression::Kind::kRelNotIn:
            os << "NotIn";
            break;
        case Expression::Kind::kContains:
            os << "Contains";
            break;
        case Expression::Kind::kNotContains:
            os << "NotContains";
            break;
        case Expression::Kind::kStartsWith:
            os << "StartsWith";
            break;
        case Expression::Kind::kNotStartsWith:
            os << "NotStartsWith";
            break;
        case Expression::Kind::kEndsWith:
            os << "EndsWith";
            break;
        case Expression::Kind::kNotEndsWith:
            os << "NotEndsWith";
            break;
        case Expression::Kind::kSubscript:
            os << "Subscript";
            break;
        case Expression::Kind::kColumn:
            os << "Column";
            break;
        case Expression::Kind::kAttribute:
            os << "Attribute";
            break;
        case Expression::Kind::kLabelAttribute:
            os << "LabelAttribute";
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
        case Expression::Kind::kAggregate:
            os << "Aggregate";
            break;
        case Expression::Kind::kEdgeProperty:
            os << "EdgeProp";
            break;
        case Expression::Kind::kTagProperty:
            os << "TagProp";
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
        case Expression::Kind::kVertex:
            os << "Vertex";
            break;
        case Expression::Kind::kEdge:
            os << "Edge";
            break;
        case Expression::Kind::kUUID:
            os << "UUID";
            break;
        case Expression::Kind::kVar:
            os << "Variable";
            break;
        case Expression::Kind::kVersionedVar:
            os << "VersionedVariable";
            break;
        case Expression::Kind::kList:
            os << "List";
            break;
        case Expression::Kind::kSet:
            os << "Set";
            break;
        case Expression::Kind::kMap:
            os << "Map";
            break;
        case Expression::Kind::kLabel:
            os << "Label";
            break;
        case Expression::Kind::kCase:
            os << "Case";
            break;
        case Expression::Kind::kPathBuild:
            os << "PathBuild";
            break;
        case Expression::Kind::kTSPrefix:
            os << "Prefix";
            break;
        case Expression::Kind::kTSWildcard:
            os << "Wildcard";
            break;
        case Expression::Kind::kTSRegexp:
            os << "Regexp";
            break;
        case Expression::Kind::kTSFuzzy:
            os << "Fuzzy";
            break;
        case Expression::Kind::kListComprehension:
            os << "ListComprehension";
            break;
        case Expression::Kind::kPredicate:
            os << "Predicate";
            break;
        case Expression::Kind::kReduce:
            os << "Reduce";
            break;
    }
    return os;
}

}  // namespace nebula
