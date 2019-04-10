/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "base/Cord.h"
#include "parser/Expressions.h"


#define THROW_IF_NO_SPACE(POS, END, REQUIRE)                                        \
    do {                                                                            \
        if ((POS) + (REQUIRE) > (END)) {                                            \
            throw Status::Error("Not enough space left, left: %lu bytes, "          \
                                "require: %lu bytes, at: %s:%d", (END) - (POS),     \
                                (REQUIRE), __FILE__, __LINE__);                     \
        }                                                                           \
    } while (false)

namespace nebula {


Status ExpressionContext::addAliasProp(const std::string &alias, const std::string &prop) {
    auto kind = aliasKind(alias);
    if (kind == AliasKind::Unknown) {
        return Status::Error("Alias `%s' not defined", alias.c_str());
    }
    if (kind == AliasKind::Edge) {
        addEdgeProp(prop);
        return Status::OK();
    }
    return Status::Error("Illegal alias `%s'", alias.c_str());
}


std::string aliasKindToString(AliasKind kind) {
    switch (kind) {
        case AliasKind::Unknown:
            return "Unknown";
        case AliasKind::SourceNode:
            return "SourceNode";
        case AliasKind::Edge:
            return "Edge";
        default:
            FLOG_FATAL("Illegal AliasKind");
    }
    return "Unknown";
}


void Expression::print(const VariantType &value) {
    switch (value.which()) {
        case 0:
            fprintf(stderr, "%ld\n", asInt(value));
            break;
        case 1:
            fprintf(stderr, "%lf\n", asDouble(value));
            break;
        case 2:
            fprintf(stderr, "%d\n", asBool(value));
            break;
        case 3:
            fprintf(stderr, "%s\n", asString(value).c_str());
            break;
    }
}


std::unique_ptr<Expression> Expression::makeExpr(uint8_t kind) {
    switch (intToKind(kind)) {
        case kPrimary:
            return std::make_unique<PrimaryExpression>();
        case kUnary:
            return std::make_unique<UnaryExpression>();
        case kTypeCasting:
            return std::make_unique<TypeCastingExpression>();
        case kArithmetic:
            return std::make_unique<ArithmeticExpression>();
        case kRelational:
            return std::make_unique<RelationalExpression>();
        case kLogical:
            return std::make_unique<LogicalExpression>();
        case kSourceProp:
            return std::make_unique<SourcePropertyExpression>();
        case kEdgeRank:
            return std::make_unique<EdgeRankExpression>();
        case kEdgeDstId:
            return std::make_unique<EdgeDstIdExpression>();
        case kEdgeSrcId:
            return std::make_unique<EdgeSrcIdExpression>();
        case kEdgeType:
            return std::make_unique<EdgeTypeExpression>();
        case kEdgeProp:
            return std::make_unique<EdgePropertyExpression>();
        case kVariableProp:
            return std::make_unique<VariablePropertyExpression>();
        case kDestProp:
            return std::make_unique<DestPropertyExpression>();
        case kInputProp:
            return std::make_unique<InputPropertyExpression>();
        default:
            throw Status::Error("Illegal expression kind: %u", kind);
    }
}


// static
std::string Expression::encode(Expression *expr) noexcept {
    Cord cord(1024);
    expr->encode(cord);
    return cord.str();
}


// static
StatusOr<std::unique_ptr<Expression>>
Expression::decode(folly::StringPiece buffer) noexcept {
    auto *pos = buffer.data();
    auto *end = pos + buffer.size();
    try {
        THROW_IF_NO_SPACE(pos, end, 1UL);
        auto expr = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
        pos = expr->decode(pos, end);
        if (pos != end) {
            return Status::Error("Buffer not consumed up, end: %p, used upto: %p", end, pos);
        }
        return std::move(expr);
    } catch (const Status &status) {
        return status;
    }
}


std::string InputPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$-.";
    buf += *prop_;
    return buf;
}


VariantType InputPropertyExpression::eval() const {
    return context_->getters().getInputProp(*prop_);
}


void InputPropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* InputPropertyExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    prop_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string DestPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$$[";
    buf += *tag_;
    buf += "].";
    buf += *prop_;
    return buf;
}


VariantType DestPropertyExpression::eval() const {
    return context_->getters().getDstTagProp(*tag_, *prop_);
}


Status DestPropertyExpression::prepare() {
    context_->addDstTagProp(*tag_, *prop_);
    return Status::OK();
}


void DestPropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(tag_->size());
    cord << *tag_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* DestPropertyExpression::decode(const char *pos, const char *end) {
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        tag_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        prop_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }

    return pos;
}


std::string VariablePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$";
    buf += *var_;
    buf += ".";
    buf += *prop_;
    return buf;
}


VariantType VariablePropertyExpression::eval() const {
    // TODO(dutor)
    return toString();
}


Status VariablePropertyExpression::prepare() {
    // TODO(dutor)
    return Status::OK();
}


void VariablePropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(var_->size());
    cord << *var_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* VariablePropertyExpression::decode(const char *pos, const char *end) {
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        var_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        prop_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }

    return pos;
}


std::string EdgePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += ".";
    buf += *prop_;
    return buf;
}


VariantType EdgePropertyExpression::eval() const {
    return context_->getters().getEdgeProp(*prop_);
}


Status EdgePropertyExpression::prepare() {
    return context_->addAliasProp(*alias_, *prop_);
}


void EdgePropertyExpression::encode(Cord &cord) const {
    // TODO(dutor) We better replace `alias_' with integral edge type
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* EdgePropertyExpression::decode(const char *pos, const char *end) {
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        alias_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        prop_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }

    return pos;
}


std::string EdgeTypeExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._type";
    return buf;
}


VariantType EdgeTypeExpression::eval() const {
    return *alias_;
}


Status EdgeTypeExpression::prepare() {
    return context_->addAliasProp(*alias_, "_type");
}


void EdgeTypeExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
}


const char* EdgeTypeExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    alias_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string EdgeSrcIdExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._src";
    return buf;
}


VariantType EdgeSrcIdExpression::eval() const {
    return context_->getters().getEdgeProp("_src");
}


Status EdgeSrcIdExpression::prepare() {
    return context_->addAliasProp(*alias_, "_src");
}


void EdgeSrcIdExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
}


const char* EdgeSrcIdExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    alias_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string EdgeDstIdExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._dst";
    return buf;
}


VariantType EdgeDstIdExpression::eval() const {
    return context_->getters().getEdgeProp("_dst");
}


Status EdgeDstIdExpression::prepare() {
    return context_->addAliasProp(*alias_, "_dst");
}


void EdgeDstIdExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
}


const char* EdgeDstIdExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    alias_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string EdgeRankExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._rank";
    return buf;
}


VariantType EdgeRankExpression::eval() const {
    return context_->getters().getEdgeProp("_rank");
}


Status EdgeRankExpression::prepare() {
    return context_->addAliasProp(*alias_, "_rank");
}


void EdgeRankExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
}


const char* EdgeRankExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    alias_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string SourcePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$^[";
    buf += *tag_;
    buf += "].";
    buf += *prop_;
    return buf;
}


VariantType SourcePropertyExpression::eval() const {
    return context_->getters().getSrcTagProp(*tag_, *prop_);
}


Status SourcePropertyExpression::prepare() {
    context_->addSrcTagProp(*tag_, *prop_);
    return Status::OK();
}


void SourcePropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(tag_->size());
    cord << *tag_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* SourcePropertyExpression::decode(const char *pos, const char *end) {
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        tag_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        prop_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }

    return pos;
}


std::string PrimaryExpression::toString() const {
    char buf[1024];
    switch (operand_.which()) {
        case kBool:
            snprintf(buf, sizeof(buf), "%s", boost::get<bool>(operand_) ? "true" : "false");
            break;
        case kInt:
            snprintf(buf, sizeof(buf), "%ld", boost::get<int64_t>(operand_));
            break;
        case kDouble:
            return std::to_string(boost::get<double>(operand_));
        case kString:
            return "\"" + boost::get<std::string>(operand_) + "\"";
    }
    return buf;
}


VariantType PrimaryExpression::eval() const {
    switch (operand_.which()) {
        case kBool:
            return boost::get<bool>(operand_);
            break;
        case kInt:
            return boost::get<int64_t>(operand_);
            break;
        case kDouble:
            return boost::get<double>(operand_);
            break;
        case kString:
            return boost::get<std::string>(operand_);
    }
    return std::string("Unknown");
}


Status PrimaryExpression::prepare() {
    return Status::OK();
}


void PrimaryExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    uint8_t which = operand_.which();
    cord << which;
    switch (which) {
        case kBool:
            cord << static_cast<uint8_t>(boost::get<bool>(operand_));
            break;
        case kInt:
            cord << boost::get<int64_t>(operand_);
            break;
        case kDouble:
            cord << boost::get<double>(operand_);
            break;
        case kString: {
            auto &str = boost::get<std::string>(operand_);
            cord << static_cast<uint16_t>(str.size());
            cord << str;
            break;
        }
        default:
            DCHECK(false);
    }
}


const char* PrimaryExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 1);
    auto which = *reinterpret_cast<const uint8_t*>(pos++);
    switch (static_cast<Which>(which)) {
        case kBool:
            THROW_IF_NO_SPACE(pos, end, 1UL);
            operand_ = *reinterpret_cast<const bool*>(pos++);
            break;
        case kInt:
            THROW_IF_NO_SPACE(pos, end, 8UL);
            operand_ = *reinterpret_cast<const int64_t*>(pos);
            pos += 8;
            break;
        case kDouble:
            THROW_IF_NO_SPACE(pos, end, 8UL);
            operand_ = *reinterpret_cast<const double*>(pos);
            pos += 8;
            break;
        case kString: {
            THROW_IF_NO_SPACE(pos, end, 2UL);
            auto size = *reinterpret_cast<const uint16_t*>(pos);
            pos += 2;
            THROW_IF_NO_SPACE(pos, end, size);
            operand_ = std::string(pos, size);
            pos += size;
            break;
        }
        default:
            throw Status::Error("Unknown variant type");
    }
    return pos;
}


std::string UnaryExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    switch (op_) {
        case PLUS:
            buf += '+';
            break;
        case NEGATE:
            buf += '-';
            break;
        case NOT:
            buf += '!';
            break;
    }
    buf += '(';
    buf.append(operand_->toString());
    buf += ')';
    return buf;
}


VariantType UnaryExpression::eval() const {
    auto value = operand_->eval();
    if (op_ == PLUS) {
        return value;
    } else if (op_ == NEGATE) {
        if (isInt(value)) {
            return -asInt(value);
        } else if (isDouble(value)) {
            return -asDouble(value);
        }
    } else {
        return !asBool(value);
    }
    return value;
}


Status UnaryExpression::prepare() {
    return operand_->prepare();
}


void UnaryExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    operand_->encode(cord);
}


const char* UnaryExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == PLUS || op_ == NEGATE || op_ == NOT);

    operand_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return operand_->decode(pos, end);
}


std::string columnTypeToString(ColumnType type) {
    switch (type) {
        case INT:
            return "int";
        case STRING:
            return "string";
        case DOUBLE:
            return "double";
        case BIGINT:
            return "bigint";
        case BOOL:
            return "bool";
        case TIMESTAMP:
            return  "timestamp";
        default:
            return "unknown";
    }
}


std::string TypeCastingExpression::toString() const {
    return "";
}


VariantType TypeCastingExpression::eval() const {
    return toString();
}


Status TypeCastingExpression::prepare() {
    return operand_->prepare();
}


void TypeCastingExpression::encode(Cord &cord) const {
    UNUSED(cord);
}


std::string ArithmeticExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += '(';
    buf.append(left_->toString());
    switch (op_) {
        case ADD:
            buf += '+';
            break;
        case SUB:
            buf += '-';
            break;
        case MUL:
            buf += '*';
            break;
        case DIV:
            buf += '/';
            break;
        case MOD:
            buf += '%';
            break;
    }
    buf.append(right_->toString());
    buf += ')';
    return buf;
}


VariantType ArithmeticExpression::eval() const {
    auto left = left_->eval();
    auto right = right_->eval();
    switch (op_) {
    case ADD:
        assert((isArithmetic(left) && isArithmetic(right))
                || (isString(left) && isString(right)));
        if (isArithmetic(left) && isArithmetic(right)) {
            if (isDouble(left) || isDouble(right)) {
                return asDouble(left) + asDouble(right);
            }
            return asInt(left) + asInt(right);
        }
        return asString(left) + asString(right);
    case SUB:
        assert(isArithmetic(left) && isArithmetic(right));
        if (isDouble(left) || isDouble(right)) {
            return asDouble(left) - asDouble(right);
        }
        return asInt(left) - asInt(right);
    case MUL:
        assert(isArithmetic(left) && isArithmetic(right));
        if (isDouble(left) || isDouble(right)) {
            return asDouble(left) * asDouble(right);
        }
        return asInt(left) * asInt(right);
    case DIV:
        assert(isArithmetic(left) && isArithmetic(right));
        if (isDouble(left) || isDouble(right)) {
            return asDouble(left) / asDouble(right);
        }
        return asInt(left) / asInt(right);
    case MOD:
        assert(isInt(left) && isInt(right));
        return asInt(left) % asInt(right);
    default:
        DCHECK(false);
    }
    return false;
}


Status ArithmeticExpression::prepare() {
    auto status = left_->prepare();
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare();
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}


void ArithmeticExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    left_->encode(cord);
    right_->encode(cord);
}


const char* ArithmeticExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == ADD || op_ == SUB || op_ == MUL || op_ == DIV || op_ == MOD);

    left_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    pos = left_->decode(pos, end);

    THROW_IF_NO_SPACE(pos, end, 1UL);
    right_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return right_->decode(pos, end);
}


std::string RelationalExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += '(';
    buf.append(left_->toString());
    switch (op_) {
        case LT:
            buf += '<';
            break;
        case LE:
            buf += "<=";
            break;
        case GT:
            buf += '>';
            break;
        case GE:
            buf += ">=";
            break;
        case EQ:
            buf += "==";
            break;
        case NE:
            buf += "!=";
            break;
    }
    buf.append(right_->toString());
    buf += ')';
    return buf;
}


VariantType RelationalExpression::eval() const {
    auto left = left_->eval();
    auto right = right_->eval();
    switch (op_) {
        case LT:
            return left < right;
        case LE:
            return left <= right;
        case GT:
            return left > right;
        case GE:
            return left >= right;
        case EQ:
            if (isDouble(left) || isDouble(right)) {
                return almostEqual(asDouble(left), asDouble(right));
            }
            return left == right;
        case NE:
            if (isDouble(left) || isDouble(right)) {
                return !almostEqual(asDouble(left), asDouble(right));
            }
            return left != right;
    }
    return false;
}


Status RelationalExpression::prepare() {
    auto status = left_->prepare();
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare();
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}


void RelationalExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    left_->encode(cord);
    right_->encode(cord);
}


const char* RelationalExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == LT || op_ == LE || op_ == GT || op_ == GE || op_ == EQ || op_ == NE);

    left_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    pos = left_->decode(pos, end);

    THROW_IF_NO_SPACE(pos, end, 1UL);
    right_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return right_->decode(pos, end);
}


std::string LogicalExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += '(';
    buf.append(left_->toString());
    switch (op_) {
        case AND:
            buf += "&&";
            break;
        case OR:
            buf += "||";
            break;
    }
    buf.append(right_->toString());
    buf += ')';
    return buf;
}


VariantType LogicalExpression::eval() const {
    auto left = left_->eval();
    auto right = right_->eval();
    if (op_ == AND) {
        return asBool(left) && asBool(right);
    } else {
        return asBool(left) || asBool(right);
    }
}


Status LogicalExpression::prepare() {
    auto status = left_->prepare();
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare();
    return Status::OK();
}


void LogicalExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    left_->encode(cord);
    right_->encode(cord);
}


const char* LogicalExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == AND || op_ == OR);

    left_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    pos = left_->decode(pos, end);

    THROW_IF_NO_SPACE(pos, end, 1UL);
    right_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return right_->decode(pos, end);
}


#undef THROW_IF_NO_SPACE

}   // namespace nebula
